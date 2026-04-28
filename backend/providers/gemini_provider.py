from __future__ import annotations

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class GeminiProvider(BaseProvider):
    name = "gemini"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=120.0) -> ProviderResult:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=self.api_key)
        user_prompt = user_prompt or DEFAULT_USER_PROMPT

        def _do():
            parts = []
            if image_url:
                # Gemini SDK requires bytes; fetch URL inline.
                import httpx
                r = httpx.get(image_url, timeout=timeout)
                parts.append(types.Part.from_bytes(data=r.content,
                                                   mime_type=r.headers.get("content-type", "image/jpeg")))
            else:
                b64, mime, raw = load_image_b64(image_path)
                if raw:
                    parts.append(types.Part.from_bytes(data=raw, mime_type=mime))
            parts.append(user_prompt)
            resp = client.models.generate_content(
                model=model_id,
                contents=parts,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    response_mime_type="application/json",
                ),
            )
            text = resp.text or ""
            usage = getattr(resp, "usage_metadata", None)
            in_t = getattr(usage, "prompt_token_count", 0) if usage else 0
            out_t = getattr(usage, "candidates_token_count", 0) if usage else 0
            parsed = normalize_prediction(parse_json_loose(text))
            return ProviderResult(text=text, parsed=parsed,
                                  input_tokens=in_t, output_tokens=out_t,
                                  raw_meta={"model": model_id})
        return self._timed(_do)
