"""Token pricing per model in USD per million tokens (input, output).

Source of truth is `models_registry.PROVIDER_MODELS`.  This module
re-exports a flat {id: {input,output}} dict for the runner.
"""
from __future__ import annotations

from .models_registry import flat_pricing

PRICING = flat_pricing()


def price_for(model_id: str, provider: str, override: dict | None = None) -> dict:
    if override and ("input" in override or "output" in override):
        return {"input": float(override.get("input", 0)), "output": float(override.get("output", 0))}
    if provider in ("ollama", "lmstudio"):
        return {"input": 0.0, "output": 0.0}
    if model_id in PRICING:
        return PRICING[model_id]
    for k, v in PRICING.items():
        if model_id.startswith(k) or k.startswith(model_id):
            return v
    return {"input": 0.0, "output": 0.0}


def cost_usd(input_tokens: int, output_tokens: int, model_id: str, provider: str, override: dict | None = None) -> float:
    p = price_for(model_id, provider, override)
    return (input_tokens / 1_000_000) * p["input"] + (output_tokens / 1_000_000) * p["output"]
