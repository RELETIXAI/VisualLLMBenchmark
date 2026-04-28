"""Provider registry."""
from __future__ import annotations
from .base import BaseProvider, ProviderResult, DEFAULT_USER_PROMPT
from .openai_provider import OpenAIProvider
from .anthropic_provider import AnthropicProvider
from .gemini_provider import GeminiProvider
from .ollama_provider import OllamaProvider
from .lmstudio_provider import LMStudioProvider


def _safe_cloud_base(base_url: str | None) -> str | None:
    """Drop a base_url that points at localhost — almost always a UI bug
    (e.g. leftover Ollama URL after switching to a cloud provider).
    Cloud SDKs use their official endpoints when base_url is None.
    """
    if not base_url:
        return None
    if any(s in base_url for s in ("localhost", "127.0.0.1", "0.0.0.0", "::1")):
        return None
    return base_url


def get_provider(name: str, api_key: str | None = None, base_url: str | None = None) -> BaseProvider:
    name = name.lower().strip()
    if name == "openai":
        return OpenAIProvider(api_key=api_key, base_url=_safe_cloud_base(base_url))
    if name == "anthropic":
        return AnthropicProvider(api_key=api_key, base_url=_safe_cloud_base(base_url))
    if name in ("gemini", "google"):
        return GeminiProvider(api_key=api_key, base_url=_safe_cloud_base(base_url))
    if name == "ollama":
        return OllamaProvider(base_url=base_url or "http://localhost:11434")
    if name in ("lmstudio", "lm_studio", "lm-studio"):
        return LMStudioProvider(base_url=base_url or "http://localhost:1234/v1")
    raise ValueError(f"Unknown provider: {name}")


PROVIDERS = ["openai", "anthropic", "gemini", "ollama", "lmstudio"]
