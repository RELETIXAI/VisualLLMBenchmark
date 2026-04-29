"""Provider adapter interface.

Each adapter takes (system_prompt, user_text, image_bytes_or_url, model_id)
and returns ProviderResult with parsed JSON output, token counts, latency.
"""
from __future__ import annotations

import base64
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ProviderResult:
    text: str = ""
    parsed: dict = field(default_factory=dict)
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    error: str | None = None
    raw_meta: dict = field(default_factory=dict)


def load_image_b64(path_or_url: str | None, image_url: str | None = None) -> tuple[str | None, str | None, bytes | None]:
    """Returns (b64, mime, raw_bytes). Either path or url must be provided."""
    if image_url:
        return None, None, None  # caller will pass URL directly when supported
    if not path_or_url:
        return None, None, None
    p = Path(path_or_url)
    if not p.exists():
        return None, None, None
    raw = p.read_bytes()
    ext = p.suffix.lower().lstrip(".")
    mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
            "gif": "image/gif", "webp": "image/webp"}.get(ext, "image/png")
    return base64.b64encode(raw).decode(), mime, raw


def parse_json_loose(text: str) -> dict:
    """Extract JSON object from possibly-fenced or chatty model output.

    Robust to truncation: if the response was cut off mid-string (Anthropic
    hitting max_tokens, Ollama timeout, etc.), walks back to the last safe
    boundary, closes any open brackets, and parses the partial result so we
    don't drop a row that's 90% valid.
    """
    if not text:
        return {}
    # 1. Strip code fences
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    if fence:
        try:
            return json.loads(fence.group(1))
        except Exception:
            pass
    # 2. Try whole text
    try:
        return json.loads(text)
    except Exception:
        pass
    # 3. Try the first balanced {...}
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i + 1])
                except Exception:
                    start = -1
                    break
    # 4. SALVAGE truncated JSON
    return _salvage_truncated_json(text)


def _salvage_truncated_json(text: str) -> dict:
    """Recover as much structure as possible from truncated JSON.

    Walk forward tracking quote/bracket state. Remember the last position where
    the JSON was 'safe to cut' — i.e. just after a complete key:value pair at
    the top object. Truncate there, close open brackets, parse.
    """
    start = text.find("{")
    if start < 0:
        return {}
    s = text[start:]

    in_str = False
    escape = False
    obj_stack: list[int] = []   # indexes of '{' positions
    arr_stack: list[int] = []   # indexes of '[' positions
    last_safe_cut = -1          # index just AFTER a top-level complete pair

    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if in_str:
            if ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            obj_stack.append(i)
        elif ch == "}":
            if obj_stack:
                obj_stack.pop()
            if not obj_stack and not arr_stack:
                # Whole document closed
                try:
                    return json.loads(s[: i + 1])
                except Exception:
                    pass
        elif ch == "[":
            arr_stack.append(i)
        elif ch == "]":
            if arr_stack:
                arr_stack.pop()
        elif ch == "," and len(obj_stack) == 1 and not arr_stack:
            # comma at the root object level — safe truncation point
            last_safe_cut = i

    if last_safe_cut <= 0:
        return {}

    # Truncate at the last safe comma and close open structures
    candidate = s[:last_safe_cut]
    # Replay state for the truncated candidate to figure out close requirements
    in_str = escape = False
    o_open = a_open = 0
    for ch in candidate:
        if escape: escape = False; continue
        if in_str:
            if ch == "\\": escape = True
            elif ch == '"': in_str = False
            continue
        if ch == '"': in_str = True; continue
        if ch == "{": o_open += 1
        elif ch == "}": o_open -= 1
        elif ch == "[": a_open += 1
        elif ch == "]": a_open -= 1
    if in_str:
        return {}  # truncation landed mid-string in our candidate, give up
    closes = "]" * a_open + "}" * o_open
    try:
        return json.loads(candidate + closes)
    except Exception:
        return {}


_NUT_ALIAS = {
    "calories":  ["calories", "kcal", "energy", "cal"],
    "protein_g": ["protein_g", "protein", "proteins"],
    "carbs_g":   ["carbs_g", "carbs", "carbohydrates", "carb"],
    "fat_g":     ["fat_g", "fat", "fats", "lipids"],
    "fiber_g":   ["fiber_g", "fiber", "fibre"],
    "sugar_g":   ["sugar_g", "sugar", "sugars"],
    "sodium_mg": ["sodium_mg", "sodium", "salt"],
}


def _grab_number(v):
    if isinstance(v, (int, float)):
        return float(v)
    if v is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", str(v))
    return float(m.group(0)) if m else None


def _normalize_nutrition(nut: dict) -> dict:
    out: dict = {}
    if not isinstance(nut, dict):
        return out
    lower = {str(k).lower().strip(): v for k, v in nut.items()}
    for canon, aliases in _NUT_ALIAS.items():
        for a in aliases:
            if a in lower:
                n = _grab_number(lower[a])
                if n is not None:
                    out[canon] = n
                break
    return out


def _normalize_ingredient(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    name = item.get("name") or item.get("ingredient") or item.get("item")
    if not name:
        return None
    qty = _grab_number(item.get("quantity") or item.get("qty") or item.get("amount") or item.get("weight"))
    unit = str(item.get("unit") or item.get("units") or "g").strip()
    out = {"name": str(name).strip(), "quantity": qty, "unit": unit}
    # Per-ingredient nutrition (optional)
    nested = _normalize_nutrition(item)
    out.update(nested)
    return out


def normalize_prediction(parsed: dict) -> dict:
    """Map model output into the canonical schema used by the scorer."""
    out = {"food": None, "description": None, "nutrition": {},
           "ingredients": [], "health_score": None}
    if not isinstance(parsed, dict):
        return out
    for k in ("food", "name", "dish", "item", "meal", "meal_name"):
        if k in parsed and parsed[k]:
            out["food"] = str(parsed[k])
            break
    for k in ("description", "explanation", "details", "summary"):
        if k in parsed and parsed[k]:
            out["description"] = str(parsed[k])
            break
    out["nutrition"] = _normalize_nutrition(parsed.get("nutrition") or parsed.get("nutrients") or {})
    ings = parsed.get("ingredients") or parsed.get("components") or []
    if isinstance(ings, list):
        for it in ings:
            n = _normalize_ingredient(it)
            if n:
                out["ingredients"].append(n)
    for k in ("health_score", "healthScore", "health", "grade", "health_grade"):
        if k in parsed and parsed[k] is not None and str(parsed[k]).strip():
            out["health_score"] = str(parsed[k]).strip()
            break
    return out


DEFAULT_USER_PROMPT = """Analyze the food in this image. Return ONLY valid JSON, no prose, matching exactly this schema:

{
  "food": "<short canonical dish name>",
  "description": "<one-sentence description>",
  "nutrition": {
    "calories":  <kcal per serving>,
    "protein_g": <g>,
    "carbs_g":   <g>,
    "fat_g":     <g>,
    "fiber_g":   <g>,
    "sugar_g":   <g>,
    "sodium_mg": <mg>
  },
  "ingredients": [
    {"name": "<ingredient>", "quantity": <number>, "unit": "g"},
    ...
  ],
  "health_score": "<one of A, B, C, D, E>"
}

Rules:
- List every visible ingredient with an estimated quantity in grams.
- Health score reflects nutritional quality: A = very healthy, E = very unhealthy.
- Use null for any value you cannot estimate. Output JSON only."""



class BaseProvider:
    name: str = "base"

    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key
        self.base_url = base_url

    def run(self, system_prompt: str, image_path: str | None, image_url: str | None,
            model_id: str, user_prompt: str | None = None, timeout: float = 120.0) -> ProviderResult:
        raise NotImplementedError

    def _timed(self, fn):
        start = time.time()
        try:
            res = fn()
            res.latency_ms = (time.time() - start) * 1000
            return res
        except Exception as e:
            return ProviderResult(error=f"{type(e).__name__}: {e}",
                                  latency_ms=(time.time() - start) * 1000)
