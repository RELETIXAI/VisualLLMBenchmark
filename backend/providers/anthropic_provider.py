from __future__ import annotations

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class AnthropicProvider(BaseProvider):
    name = "anthropic"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=120.0) -> ProviderResult:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key, base_url=self.base_url, timeout=timeout)
        user_prompt = user_prompt or DEFAULT_USER_PROMPT

        def _do():
            content = []
            if image_url:
                content.append({"type": "image",
                                "source": {"type": "url", "url": image_url}})
            else:
                b64, mime, _ = load_image_b64(image_path)
                if b64:
                    content.append({"type": "image",
                                    "source": {"type": "base64", "media_type": mime, "data": b64}})
            content.append({"type": "text", "text": user_prompt})
            resp = client.messages.create(
                model=model_id,
                # 4096 keeps room for a full schema response with ~10 ingredients
                # (each ingredient ~150 output tokens incl. all nutrition fields).
                # Was 1024, which truncated complex meals mid-JSON.
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": content}],
            )
            text = "".join(b.text for b in resp.content if hasattr(b, "text"))
            in_t = getattr(resp.usage, "input_tokens", 0)
            out_t = getattr(resp.usage, "output_tokens", 0)
            parsed = normalize_prediction(parse_json_loose(text))
            return ProviderResult(text=text, parsed=parsed,
                                  input_tokens=in_t, output_tokens=out_t,
                                  raw_meta={"model": model_id})
        return self._timed(_do)
