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


class _LoRAWrapper(object):
    """Minimal runtime LoRA wrapper.

    Calls the original layer (quantized or float) unchanged, then adds the
    LoRA contribution to the output:

        y = base_layer(x)  +  (x @ A @ B) * scale

    Why a plain object instead of nn.Module:
      mlx_vlm's update_modules / set_module_by_name accept any callable as a
      replacement. Using a plain __call__ object avoids mlx nn.Module
      parameter-tracking overhead and keeps A/B as pure mx.arrays so they
      stay on the compute graph without extra boilerplate.
    """
    def __init__(self, base, A, B, scale):
        self.base   = base    # original nn.Linear or nn.QuantizedLinear
        self.A      = A       # (in_features, rank) — from adapter file
        self.B      = B       # (rank, out_features) — from adapter file
        self.scale  = scale

    def __call__(self, x):
        import mlx.core as mx
        base_out = self.base(x)
        # x: (..., in_features)  A: (in_features, rank)  B: (rank, out_features)
        lora_out = (x @ self.A @ self.B) * self.scale
        return base_out + lora_out.astype(base_out.dtype)


def _apply_lora(model, adapter_path: str) -> object:
    """Apply a LoRA adapter by wrapping only the trained layers at inference.

    Strategy — output-side wrapper (never touches base weights):
    ────────────────────────────────────────────────────────────
    For every (A, B) pair saved in the adapter file we:
      1. Navigate the model tree to find the exact layer by its full path.
      2. Replace it with a _LoRAWrapper that calls base_layer(x) and adds
         (x @ A @ B) * scale to the output.

    This sidesteps all weight-shape and quantisation-layout issues:
      • The base layer (QuantizedLinear or Linear) handles its own forward
        pass as usual — we never read or reformat its weights.
      • A and B come directly from the adapter file — no shape inference.
      • Only layers present in the adapter file are wrapped; every other
        linear layer runs untouched.
    """
    import json
    from pathlib import Path

    import mlx.core as mx
    from mlx_vlm.trainer.utils import set_module_by_name

    adapter_dir = Path(adapter_path)
    with open(adapter_dir / "adapter_config.json") as f:
        cfg = json.load(f)
    rank  = int(cfg.get("rank",  16))
    alpha = float(cfg.get("alpha", float(rank)))
    scale = alpha / rank

    lora_weights = mx.load(str(adapter_dir / "adapters.safetensors"))

    # Group into {base_path: {"A": tensor, "B": tensor}}
    pairs: dict[str, dict] = {}
    for key, weight in lora_weights.items():
        if key.endswith(".A"):
            pairs.setdefault(key[:-2], {})["A"] = weight
        elif key.endswith(".B"):
            pairs.setdefault(key[:-2], {})["B"] = weight

    LM_PREFIX = "language_model."
    lm = model.language_model
    n_applied = 0

    for full_key, mats in pairs.items():
        A = mats.get("A")   # (in_features, rank)
        B = mats.get("B")   # (rank, out_features)
        if A is None or B is None:
            continue
        if not full_key.startswith(LM_PREFIX):
            continue
        rel_name = full_key[len(LM_PREFIX):]   # e.g. "model.layers.9.self_attn.v_proj"

        # Navigate to the target layer
        layer = lm
        for part in rel_name.split("."):
            try:
                layer = layer[int(part)] if part.isdigit() else getattr(layer, part)
            except (AttributeError, IndexError, TypeError):
                layer = None
                break
        if layer is None or not callable(layer):
            continue

        wrapper = _LoRAWrapper(layer, A, B, scale)
        set_module_by_name(lm, rel_name, wrapper)
        n_applied += 1

    mx.eval(model.parameters())
    print(f"[mlx]  LoRA: wrapped {n_applied}/{len(pairs)} layers", flush=True)
    return model


def _load_model(model_id: str, adapter_path: str | None = None) -> tuple:
    """Load (and cache) a model + processor + config, optionally with a
    LoRA adapter applied on top.

    Cache key: (model_id, adapter_path or ""). A given (base, adapter)
    pair is loaded once and reused. Loading the same base with a
    different adapter creates a separate cache entry — this costs
    extra memory but is safer than mutating an already-loaded model
    in place (load_adapters is not reversible without a re-load).
    """
    key = (model_id, adapter_path or "")
    with _CACHE_LOCK:
        if key not in _CACHE:
            from mlx_vlm import load
            from mlx_vlm.utils import load_config
            model, processor = load(model_id)
            config = load_config(model_id)
            if adapter_path:
                model = _apply_lora(model, adapter_path)
                print(f"[mlx] loaded adapter: {adapter_path}", flush=True)
            _CACHE[key] = (model, processor, config)
        return _CACHE[key]


class MLXProvider(BaseProvider):
    """Runs mlx-community VLMs directly on Apple Silicon without any external server."""
    name = "mlx"

    def run(self, system_prompt: str, image_path: str | None, image_url: str | None,
            model_id: str, user_prompt: str | None = None,
            timeout: float = 600.0,
            gen_params: dict | None = None) -> ProviderResult:

        user_prompt = user_prompt or DEFAULT_USER_PROMPT
        gp = dict(gen_params or {})

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
                # adapter_path travels through gen_params (see RunIn in
                # backend/main.py). Empty string is treated as no
                # adapter so users can pass it from a form without
                # special-casing the empty case.
                adapter_path = (gp.get("adapter_path") or "").strip() or None
                model, processor, config = _load_model(model_id, adapter_path)

                # ── chat-template kwargs (controls what the *prompt*
                # contains — e.g. Qwen3-VL inserts a thinking block here
                # when enable_thinking=True). Default OFF so the model
                # produces a direct JSON answer instead of a thinking
                # monologue + answer.
                template_kwargs: dict = {}
                if "enable_thinking" in gp:
                    template_kwargs["enable_thinking"] = bool(gp["enable_thinking"])
                else:
                    template_kwargs["enable_thinking"] = False

                # Build prompt with chat template.
                #
                # Pre-fix bug: we used to pass the user_prompt as a bare
                # string, which produces just  '<|image|> {user_prompt}'
                # — the SYSTEM prompt was silently dropped. This was the
                # cause of the E4B fine-tune's "repetition collapse" at
                # inference: the trainer fed messages = [system, user,
                # assistant] through apply_chat_template (see
                # mlx_vlm/trainer/datasets.py), so the LoRA learned to
                # produce JSON conditional on the schema appearing in
                # the system slot. Without it at inference time, the
                # model has no anchor and degenerates.
                #
                # Fix: when system_prompt is non-empty, build a real
                # [system, user] message list and let apply_chat_template
                # render the full turn structure (matches training).
                # When empty, fall back to the old single-string path so
                # any caller that relied on it still works.
                if system_prompt:
                    msgs = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ]
                    prompt = apply_chat_template(
                        processor, config, msgs,
                        num_images=1 if img_arg else 0,
                        **template_kwargs,
                    )
                else:
                    prompt = apply_chat_template(
                        processor, config, user_prompt,
                        num_images=1 if img_arg else 0,
                        **template_kwargs,
                    )

                # Count approximate input tokens
                try:
                    tok = processor.tokenizer if hasattr(processor, "tokenizer") else processor
                    in_tokens = len(tok.encode(system_prompt + user_prompt))
                except Exception:
                    in_tokens = (len(system_prompt) + len(user_prompt)) // 4

                # ── generation kwargs (controls sampling). Forward
                # everything mlx_vlm.generate_step understands. Unknown
                # keys are silently dropped by the underlying call.
                gen_kwargs: dict = {
                    "max_tokens": int(gp.get("max_tokens") or 8192),
                    "temperature": float(gp.get("temperature", 0.0)),
                    "verbose": False,
                }
                # Optional sampler params — only include if set, so we
                # honour mlx_vlm's defaults otherwise.
                for src, dst, cast in [
                    ("top_p", "top_p", float),
                    ("top_k", "top_k", int),
                    ("min_p", "min_p", float),
                    ("repetition_penalty", "repetition_penalty", float),
                    ("repetition_context_size", "repetition_context_size", int),
                ]:
                    if src in gp and gp[src] not in (None, ""):
                        try:
                            gen_kwargs[dst] = cast(gp[src])
                        except (TypeError, ValueError):
                            pass
                # Thinking budget (token cap on the thinking block).
                # Only applies when enable_thinking is True.
                if gp.get("enable_thinking") and "thinking_budget" in gp:
                    try:
                        gen_kwargs["thinking_budget"] = int(gp["thinking_budget"])
                        gen_kwargs["enable_thinking"] = True
                    except (TypeError, ValueError):
                        pass

                output = generate(
                    model, processor,
                    image=img_arg,
                    prompt=prompt,
                    **gen_kwargs,
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
