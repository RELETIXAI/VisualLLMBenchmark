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
