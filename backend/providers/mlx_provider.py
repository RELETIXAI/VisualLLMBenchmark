"""MLX-VLM provider — runs vision-language models directly on Apple Silicon via mlx-vlm.

Models are loaded from HuggingFace (mlx-community namespace) and cached in memory
for the lifetime of the process — so the first row of a run pays the load cost
(typically 5-30 s depending on model size), all subsequent rows are fast.

Requires:  pip install mlx-vlm
Supported: Apple Silicon Macs (M1 / M2 / M3 / M4)
"""
from __future__ import annotations

import json
import os
import tempfile
import threading

from .base import BaseProvider, ProviderResult, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT

# ── model cache (model_id → (model, processor, config)) ────────────────────────
_CACHE: dict[str, tuple] = {}
_CACHE_LOCK = threading.Lock()

# Text-only model_type values that mlx_vlm cannot run with images
_TEXT_ONLY_TYPES = {
    "gemma3_text", "gemma2", "llama", "mistral", "qwen2", "phi3",
    "phi2", "gpt2", "gpt_neox", "falcon", "mpt", "bloom",
}


def _check_vision_capable(model_path: str) -> None:
    """Raise ValueError immediately if the model is text-only.

    Three layers of detection, in order of certainty:

      1. Config says the model is text-only (model_type ∈ _TEXT_ONLY_TYPES
         AND no vision keys at all).
      2. Config CLAIMS vision (has vision keys, *ConditionalGeneration
         architecture, etc.) but the safetensors index contains ZERO
         vision_tower / siglip / embed_vision weights — i.e. the
         conversion stripped the vision tower while keeping the
         multimodal config metadata. This is the classic "I converted
         the text part of a VL model" gotcha. Caught Qwen3.5-MoE and
         gemma-4-26B-A4B-it being run as VLMs when only the language
         model was actually converted.
      3. Otherwise treat as vision-capable and let mlx_vlm decide.
    """
    cfg_path = os.path.join(model_path, "config.json")
    if not os.path.exists(cfg_path):
        return  # can't tell — let mlx_vlm decide
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
    except Exception:
        return
    model_type = cfg.get("model_type", "")
    vision_keys = {
        "vision_config", "vision_model_type", "vision_soft_tokens_per_image",
        "image_token_index", "image_token_id",
        "pixel_shuffle_factor", "visual",
        "boi_token_id", "eoi_token_id", "video_token_id",
        "vision_start_token_id", "vision_end_token_id",
    }
    config_says_vision = (
        any(k in cfg for k in vision_keys)
        or any(a for a in (cfg.get("architectures") or [])
               if any(tag in a for tag in
                      ("VisionLanguage", "VLM", "Vision",
                       "ConditionalGeneration", "MultiModal")))
    )
    if not config_says_vision and model_type in _TEXT_ONLY_TYPES:
        raise ValueError(
            f"Model '{os.path.basename(model_path)}' is text-only (type: {model_type}) "
            f"and cannot process images. Pick a vision-capable model such as "
            f"mlx-community/Qwen2.5-VL-7B-Instruct-4bit from LM Studio or HuggingFace."
        )

    # Layer 2: fail fast on text-only conversions of multimodal models.
    # Read the safetensors index and count vision-tower weights.
    idx_path = os.path.join(model_path, "model.safetensors.index.json")
    if config_says_vision and os.path.exists(idx_path):
        try:
            with open(idx_path) as f:
                weight_map = json.load(f).get("weight_map") or {}
        except Exception:
            return
        if not weight_map:
            return
        n_vision = sum(1 for k in weight_map
                       if any(t in k for t in ("vision_tower", "siglip",
                                                 "embed_vision", "vision_model")))
        # Multimodal models have hundreds of vision-tower weights
        # (gemma-4 ~356, qwen3-VL ~393, llava ~250+). Fewer than 20
        # means the vision tower was not converted.
        if n_vision < 20:
            raise ValueError(
                f"Model '{os.path.basename(model_path)}' was converted text-only: "
                f"its config.json declares vision capability (model_type={model_type}) "
                f"but the safetensors contain only {n_vision} vision-tower weights "
                f"(expected ~250-400). Re-convert from a true VL source — for Qwen, "
                f"use 'Qwen/Qwen3-VL-*' or 'Qwen/Qwen2.5-VL-*' rather than the "
                f"text-only Qwen3-MoE; for Gemma, use the multimodal release rather "
                f"than the language-model-only checkpoint."
            )


def _load_model(model_id: str) -> tuple:
    with _CACHE_LOCK:
        if model_id not in _CACHE:
            from mlx_vlm import load
            from mlx_vlm.utils import load_config
            model, processor = load(model_id)
            config = load_config(model_id)
            _CACHE[model_id] = (model, processor, config)
        return _CACHE[model_id]


class MLXProvider(BaseProvider):
    """Runs mlx-community VLMs directly on Apple Silicon without any external server."""
    name = "mlx"

    def run(self, system_prompt: str, image_path: str | None, image_url: str | None,
            model_id: str, user_prompt: str | None = None,
            timeout: float = 600.0) -> ProviderResult:

        user_prompt = user_prompt or DEFAULT_USER_PROMPT

        def _do() -> ProviderResult:
            try:
                from mlx_vlm import generate
                from mlx_vlm.prompt_utils import apply_chat_template
            except ImportError:
                raise RuntimeError(
                    "mlx-vlm is not installed. Run: pip install mlx-vlm"
                )

            # Fail fast if model is text-only — avoids burning through 50 rows
            if os.path.isdir(model_id):
                _check_vision_capable(model_id)

            # Resolve image to a local path (mlx_vlm needs a file path or PIL image)
            tmp_path: str | None = None
            img_arg: str | None = None

            if image_url:
                import httpx
                r = httpx.get(image_url, timeout=60.0)
                r.raise_for_status()
                suffix = ".jpg"
                for ext in (".png", ".webp", ".gif", ".bmp"):
                    if ext in image_url.lower():
                        suffix = ext
                        break
                fd, tmp_path = tempfile.mkstemp(suffix=suffix)
                with os.fdopen(fd, "wb") as f:
                    f.write(r.content)
                img_arg = tmp_path
            elif image_path:
                img_arg = image_path

            try:
                model, processor, config = _load_model(model_id)

                # Build prompt with chat template
                prompt = apply_chat_template(
                    processor, config, user_prompt,
                    num_images=1 if img_arg else 0,
                )

                # Count approximate input tokens
                try:
                    tok = processor.tokenizer if hasattr(processor, "tokenizer") else processor
                    in_tokens = len(tok.encode(system_prompt + user_prompt))
                except Exception:
                    in_tokens = (len(system_prompt) + len(user_prompt)) // 4

                output = generate(
                    model, processor,
                    image=img_arg,
                    prompt=prompt,
                    max_tokens=8192,    # generous cap so dishes with long
                                        # ingredient lists don't truncate
                    verbose=False,
                )

                text = output if isinstance(output, str) else getattr(output, "text", str(output))

                try:
                    tok = processor.tokenizer if hasattr(processor, "tokenizer") else processor
                    out_tokens = len(tok.encode(text))
                except Exception:
                    out_tokens = len(text) // 4

                parsed = normalize_prediction(parse_json_loose(text))
                return ProviderResult(
                    text=text, parsed=parsed,
                    input_tokens=in_tokens, output_tokens=out_tokens,
                    raw_meta={"model": model_id},
                )
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        return self._timed(_do)
