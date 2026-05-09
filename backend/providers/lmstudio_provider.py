from __future__ import annotations

from .base import BaseProvider, ProviderResult, load_image_b64, parse_json_loose, normalize_prediction, DEFAULT_USER_PROMPT


class LMStudioProvider(BaseProvider):
    """LM Studio exposes an OpenAI-compatible API at /v1."""
    name = "lmstudio"

    def run(self, system_prompt, image_path, image_url, model_id,
            user_prompt=None, timeout=300.0,
            gen_params: dict | None = None) -> ProviderResult:
        from openai import OpenAI
        base = (self.base_url or "http://localhost:1234/v1").rstrip("/")
        client = OpenAI(api_key=self.api_key or "lm-studio", base_url=base, timeout=timeout)
        user_prompt = user_prompt or DEFAULT_USER_PROMPT
        gp = dict(gen_params or {})

        def _do():
            content = [{"type": "text", "text": user_prompt}]
            if image_url:
                content.append({"type": "image_url", "image_url": {"url": image_url}})
            else:
                b64, mime, _ = load_image_b64(image_path)
                if b64:
                    content.append({"type": "image_url",
                                    "image_url": {"url": f"data:{mime};base64,{b64}"}})

            # OpenAI-compat sampling kwargs. LM Studio / vLLM / llama-server
            # accept temperature, top_p, max_tokens via the standard fields.
            # top_k and min_p are passed via extra_body since they're not
            # in the OpenAI spec; LM Studio honours both, llama-server
            # ignores unknown keys.
            extra: dict = {}
            req: dict = {
                "model": model_id,
                "messages": [{"role": "system", "content": system_prompt},
                             {"role": "user", "content": content}],
            }
            if "max_tokens" in gp and gp["max_tokens"]:
                req["max_tokens"] = int(gp["max_tokens"])
            if "temperature" in gp and gp["temperature"] is not None:
                req["temperature"] = float(gp["temperature"])
            if "top_p" in gp and gp["top_p"] is not None:
                req["top_p"] = float(gp["top_p"])
            if "top_k" in gp and gp["top_k"] not in (None, ""):
                extra["top_k"] = int(gp["top_k"])
            if "min_p" in gp and gp["min_p"] not in (None, ""):
                extra["min_p"] = float(gp["min_p"])
            if "repetition_penalty" in gp and gp["repetition_penalty"] not in (None, ""):
                extra["repetition_penalty"] = float(gp["repetition_penalty"])
            # Qwen-family thinking switch is exposed by LM Studio as
            # chat_template_kwargs.enable_thinking.
            if "enable_thinking" in gp:
                extra.setdefault("chat_template_kwargs", {})["enable_thinking"] = bool(gp["enable_thinking"])
            if extra:
                req["extra_body"] = extra
            resp = client.chat.completions.create(**req)
            text = resp.choices[0].message.content or ""
            usage = getattr(resp, "usage", None)
            in_t = getattr(usage, "prompt_tokens", 0) if usage else 0
            out_t = getattr(usage, "completion_tokens", 0) if usage else 0
            parsed = normalize_prediction(parse_json_loose(text))
            return ProviderResult(text=text, parsed=parsed, input_tokens=in_t,
                                  output_tokens=out_t,
                                  raw_meta={"model": model_id})
        return self._timed(_do)
