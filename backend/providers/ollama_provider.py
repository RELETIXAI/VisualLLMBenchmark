from __future__ import annotations

import base64
import httpx

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class OllamaProvider(BaseProvider):
    name = "ollama"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=300.0) -> ProviderResult:
        user_prompt = user_prompt or DEFAULT_USER_PROMPT
        base = (self.base_url or "http://localhost:11434").rstrip("/")

        def _do():
            images = []
            if image_url:
                r = httpx.get(image_url, timeout=timeout)
                images.append(base64.b64encode(r.content).decode())
            else:
                b64, _mime, _raw = load_image_b64(image_path)
                if b64:
                    images.append(b64)
            payload = {
                "model": model_id,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt, "images": images},
                ],
                "stream": False,
                "format": "json",
            }
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
