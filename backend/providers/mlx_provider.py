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
    """Raise ValueError immediately if the model is text-only."""
    cfg_path = os.path.join(model_path, "config.json")
    if not os.path.exists(cfg_path):
        return  # can't tell — let mlx_vlm decide
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
    except Exception:
        return
    model_type = cfg.get("model_type", "")
    is_vision = any(k in cfg for k in (
        "vision_config", "image_token_index", "vision_model_type",
        "pixel_shuffle_factor", "visual",
    )) or any(a for a in (cfg.get("architectures") or [])
              if "VisionLanguage" in a or "VLM" in a or "Vision" in a)
    if not is_vision and model_type in _TEXT_ONLY_TYPES:
        raise ValueError(
            f"Model '{os.path.basename(model_path)}' is text-only (type: {model_type}) "
            f"and cannot process images. Download a vision-capable model such as "
            f"mlx-community/Qwen2.5-VL-7B-Instruct-4bit from LM Studio or HuggingFace."
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
                    max_tokens=2048,
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
