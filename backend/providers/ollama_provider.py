from __future__ import annotations

import base64
import httpx

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class OllamaProvider(BaseProvider):
    name = "ollama"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=300.0,
            gen_params: dict | None = None) -> ProviderResult:
        user_prompt = user_prompt or DEFAULT_USER_PROMPT
        base = (self.base_url or "http://localhost:11434").rstrip("/")
        gp = dict(gen_params or {})

        def _do():
            images = []
            if image_url:
                r = httpx.get(image_url, timeout=timeout)
                images.append(base64.b64encode(r.content).decode())
            else:
                b64, _mime, _raw = load_image_b64(image_path)
                if b64:
                    images.append(b64)
            # Ollama receives sampling params under "options". Ollama maps
            # num_predict→max_tokens and uses snake_case for everything.
            options: dict = {}
            if gp.get("max_tokens"): options["num_predict"] = int(gp["max_tokens"])
            if gp.get("temperature") is not None: options["temperature"] = float(gp["temperature"])
            if gp.get("top_p") is not None: options["top_p"] = float(gp["top_p"])
            if gp.get("top_k") not in (None, ""): options["top_k"] = int(gp["top_k"])
            if gp.get("min_p") not in (None, ""): options["min_p"] = float(gp["min_p"])
            if gp.get("repetition_penalty") not in (None, ""):
                options["repeat_penalty"] = float(gp["repetition_penalty"])
            payload = {
                "model": model_id,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt, "images": images},
                ],
                "stream": False,
                "format": "json",
            }
            if options:
                payload["options"] = options
            # Ollama supports per-request "think" toggle (0.x+) for
            # thinking-capable models like Qwen3-VL. Default off.
            if "enable_thinking" in gp:
                payload["think"] = bool(gp["enable_thinking"])
            with httpx.Client(timeout=timeout) as cli:
                resp = cli.post(f"{base}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            text = (data.get("message") or {}).get("content", "") or data.get("response", "")
            in_t = data.get("prompt_eval_count", 0)
            out_t = data.get("eval_count", 0)
            parsed = normalize_prediction(parse_json_loose(text))
            return ProviderResult(text=text, parsed=parsed,
                                  input_tokens=in_t, output_tokens=out_t,
                                  raw_meta={"model": model_id})
        return self._timed(_do)
