from __future__ import annotations

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class LMStudioProvider(BaseProvider):
    """LM Studio exposes an OpenAI-compatible API at /v1."""
    name = "lmstudio"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=300.0) -> ProviderResult:
        from openai import OpenAI
        base = (self.base_url or "http://localhost:1234/v1").rstrip("/")
        client = OpenAI(api_key=self.api_key or "lm-studio", base_url=base, timeout=timeout)
        user_prompt = user_prompt or DEFAULT_USER_PROMPT

        def _do():
            content = [{"type": "text", "text": user_prompt}]
            if image_url:
                content.append({"type": "image_url", "image_url": {"url": image_url}})
            else:
                b64, mime, _ = load_image_b64(image_path)
                if b64:
                    content.append({"type": "image_url",
                                    "image_url": {"url": f"data:{mime};base64,{b64}"}})
            resp = client.chat.completions.create(
                model=model_id,
                messages=[{"role": "system", "content": system_prompt},
                          {"role": "user", "content": content}],
            )
            text = resp.choices[0].message.content or ""
            usage = getattr(resp, "usage", None)
            in_t = getattr(usage, "prompt_tokens", 0) if usage else 0
            out_t = getattr(usage, "completion_tokens", 0) if usage else 0
            parsed = normalize_prediction(parse_json_loose(text))
            return ProviderResult(text=text, parsed=parsed, input_tokens=in_t,
                                  output_tokens=out_t,
                                  raw_meta={"model": model_id})
        return self._timed(_do)
