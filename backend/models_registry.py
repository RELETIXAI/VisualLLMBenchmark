"""Vision-capable model registry per cloud provider.

Sourced from official docs (April 2026):
- Anthropic:  https://platform.claude.com/docs/en/about-claude/models/overview
              https://platform.claude.com/docs/en/about-claude/pricing
- Gemini:     https://ai.google.dev/gemini-api/docs/pricing
- OpenAI:     https://openai.com/api/pricing/  (cross-checked w/ pricepertoken)

Each entry: {id, label, input, output, group, notes?}
- id     -> exact API model id
- input  -> $ per 1M input tokens (text)
- output -> $ per 1M output tokens
- group  -> "current" | "legacy"

ALL listed models accept image input.  Curated to vision-capable only —
text-only models (e.g. realtime, embeddings, image-gen) are excluded.
"""
from __future__ import annotations

PROVIDER_MODELS: dict[str, list[dict]] = {
    "openai": [
        # Current generation
        {"id": "gpt-5",          "label": "GPT-5",          "input": 1.25,  "output": 10.00, "group": "current"},
        {"id": "gpt-5-mini",     "label": "GPT-5 mini",     "input": 0.25,  "output":  2.00, "group": "current"},
        {"id": "gpt-5-nano",     "label": "GPT-5 nano",     "input": 0.05,  "output":  0.40, "group": "current"},
        {"id": "gpt-4.1",        "label": "GPT-4.1",        "input": 2.00,  "output":  8.00, "group": "current"},
        {"id": "gpt-4.1-mini",   "label": "GPT-4.1 mini",   "input": 0.40,  "output":  1.60, "group": "current"},
        {"id": "gpt-4.1-nano",   "label": "GPT-4.1 nano",   "input": 0.10,  "output":  0.40, "group": "current"},
        {"id": "o4-mini",        "label": "o4-mini",        "input": 1.10,  "output":  4.40, "group": "current",
         "notes": "Reasoning model with vision"},
        # Still-supported, broadly used
        {"id": "gpt-4o",         "label": "GPT-4o",         "input": 2.50,  "output": 10.00, "group": "legacy"},
        {"id": "gpt-4o-mini",    "label": "GPT-4o mini",    "input": 0.15,  "output":  0.60, "group": "legacy"},
        {"id": "o3",             "label": "o3",             "input": 2.00,  "output":  8.00, "group": "legacy",
         "notes": "Reasoning model with vision"},
        {"id": "o1",             "label": "o1",             "input": 15.00, "output": 60.00, "group": "legacy",
         "notes": "Reasoning model with vision"},
    ],
    "anthropic": [
        # Current frontier
        {"id": "claude-opus-4-7",   "label": "Claude Opus 4.7",    "input":  5.00, "output": 25.00, "group": "current",
         "notes": "Most capable; new tokenizer (~35% more tokens vs older)"},
        {"id": "claude-sonnet-4-6", "label": "Claude Sonnet 4.6",  "input":  3.00, "output": 15.00, "group": "current"},
        {"id": "claude-haiku-4-5",  "label": "Claude Haiku 4.5",   "input":  1.00, "output":  5.00, "group": "current"},
        # Recent legacy still in service
        {"id": "claude-opus-4-6",       "label": "Claude Opus 4.6",    "input":  5.00, "output": 25.00, "group": "legacy"},
        {"id": "claude-sonnet-4-5",     "label": "Claude Sonnet 4.5",  "input":  3.00, "output": 15.00, "group": "legacy"},
        {"id": "claude-opus-4-5",       "label": "Claude Opus 4.5",    "input":  5.00, "output": 25.00, "group": "legacy"},
        {"id": "claude-opus-4-1",       "label": "Claude Opus 4.1",    "input": 15.00, "output": 75.00, "group": "legacy"},
        {"id": "claude-3-5-haiku-latest","label": "Claude Haiku 3.5",  "input":  0.80, "output":  4.00, "group": "legacy"},
    ],
    "gemini": [
        # Current generation
        {"id": "gemini-3.1-pro-preview",        "label": "Gemini 3.1 Pro (preview)",     "input": 2.00, "output": 12.00, "group": "current",
         "notes": "≤200k tokens; >200k context priced higher"},
        {"id": "gemini-3-flash-preview",        "label": "Gemini 3 Flash (preview)",     "input": 0.50, "output":  3.00, "group": "current"},
        {"id": "gemini-3.1-flash-lite-preview", "label": "Gemini 3.1 Flash-Lite (preview)","input": 0.25, "output":  1.50, "group": "current"},
        # Stable production tier
        {"id": "gemini-2.5-pro",        "label": "Gemini 2.5 Pro",        "input": 1.25, "output": 10.00, "group": "current"},
        {"id": "gemini-2.5-flash",      "label": "Gemini 2.5 Flash",      "input": 0.30, "output":  2.50, "group": "current"},
        {"id": "gemini-2.5-flash-lite", "label": "Gemini 2.5 Flash-Lite", "input": 0.10, "output":  0.40, "group": "current"},
        # Legacy
        {"id": "gemini-2.0-flash",      "label": "Gemini 2.0 Flash",      "input": 0.10, "output":  0.40, "group": "legacy"},
    ],

    # ── MLX (Apple Silicon, free — runs locally via mlx-vlm) ──────────────────
    # Model IDs are HuggingFace repo paths (mlx-community namespace).
    # All models below are 4-bit quantised VLMs known to support image input.
    # Cost is $0 (local compute). Download size noted in label.
    "mlx": [
        # Qwen2.5-VL — best overall vision quality for food/nutrition tasks
        {"id": "mlx-community/Qwen2.5-VL-7B-Instruct-4bit",  "label": "Qwen2.5-VL 7B (4-bit, ~4 GB)",  "input": 0, "output": 0, "group": "current",
         "notes": "Best balance of speed and accuracy; recommended starting point"},
        {"id": "mlx-community/Qwen2.5-VL-3B-Instruct-4bit",  "label": "Qwen2.5-VL 3B (4-bit, ~2 GB)",  "input": 0, "output": 0, "group": "current",
         "notes": "Fast, good accuracy on clear food photos"},
        {"id": "mlx-community/Qwen2.5-VL-32B-Instruct-4bit", "label": "Qwen2.5-VL 32B (4-bit, ~18 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Highest local accuracy; needs 24+ GB RAM"},
        # Llama-3.2-Vision — Meta's vision family
        {"id": "mlx-community/Llama-3.2-11B-Vision-Instruct-4bit", "label": "Llama 3.2 Vision 11B (4-bit, ~6 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Strong vision reasoning; good JSON compliance"},
        {"id": "mlx-community/Llama-3.2-3B-Vision-Instruct-4bit",  "label": "Llama 3.2 Vision 3B (4-bit, ~2 GB)",  "input": 0, "output": 0, "group": "current",
         "notes": "Lightweight, fast inference"},
        # Gemma-3 — Google, vision-capable IT variants
        {"id": "mlx-community/gemma-3-12b-it-4bit", "label": "Gemma 3 12B IT (4-bit, ~7 GB)",  "input": 0, "output": 0, "group": "current",
         "notes": "Google model; solid nutrition understanding"},
        {"id": "mlx-community/gemma-3-4b-it-4bit",  "label": "Gemma 3 4B IT (4-bit, ~2.5 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Compact and quick"},
        # Phi-4 multimodal — Microsoft
        {"id": "mlx-community/phi-4-multimodal-instruct-4bit", "label": "Phi-4 Multimodal (4-bit, ~8 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Microsoft; strong at structured output (JSON)"},
        # SmolVLM — tiny/speed benchmark
        {"id": "mlx-community/SmolVLM2-2.2B-Instruct-4bit",  "label": "SmolVLM2 2.2B (4-bit, ~1.5 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Fastest; useful as a baseline or for large-scale sweeps"},
        # Mistral / Pixtral
        {"id": "mlx-community/Mistral-Small-3.1-24B-Instruct-2503-4bit", "label": "Mistral Small 3.1 24B (4-bit, ~14 GB)", "input": 0, "output": 0, "group": "current",
         "notes": "Vision + text; needs 16+ GB RAM"},
    ],
}


def models_for(provider: str) -> list[dict]:
    return PROVIDER_MODELS.get(provider.lower(), [])


def lookup(provider: str, model_id: str) -> dict | None:
    for m in PROVIDER_MODELS.get(provider.lower(), []):
        if m["id"] == model_id:
            return m
    return None


def flat_pricing() -> dict[str, dict]:
    """All vision-models flattened into {model_id: {input, output}}."""
    out: dict[str, dict] = {}
    for prov, models in PROVIDER_MODELS.items():
        for m in models:
            out[m["id"]] = {"input": m["input"], "output": m["output"]}
    return out
