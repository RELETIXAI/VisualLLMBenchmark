"""LLM-as-judge ingredient matcher.

Replaces the rule-based bipartite matcher (token-F1 + small embedding
floor + manual synonym map) with a single LLM call per row. The model
does the assignment + similarity scoring in one shot, with full
language understanding — synonyms, regional names, translations,
cooking-state variants, identity-bearing differences, all at once.

Cost-control:
  • One call per (truth_list, pred_list) pair, NOT per ingredient pair.
  • Aggressively cached in the embeddings DB by SHA-1 of the canonical
    inputs. Re-scoring a previously-judged dataset is free (DB read
    only, no LLM call).
  • Compact JSON-only protocol; typical token cost is 200-1000 input,
    200-500 output. At gpt-5-mini rates this is < $0.001 per row.

Default judge model: gpt-5-mini (set LLM_JUDGE_MODEL to override).
Provider auto-detected from model id prefix:
  • gpt-* / o1-*       → OpenAI
  • claude-*           → Anthropic
  • gemini-*           → Google
  • everything else    → Ollama (local) at OLLAMA_BASE_URL or
                         http://localhost:11434
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from typing import Optional

from . import db
from .providers.base import parse_json_loose

# ── public API ────────────────────────────────────────────────────────────

DEFAULT_MODEL = os.environ.get("LLM_JUDGE_MODEL", "gpt-5-mini")
_LOCK = threading.Lock()
_TABLE_READY = False


def is_configured() -> bool:
    """True iff a judge model is set AND we have credentials for it."""
    model = (os.environ.get("LLM_JUDGE_MODEL") or DEFAULT_MODEL).lower()
    if model.startswith(("gpt", "o1")):
        return bool(os.environ.get("OPENAI_API_KEY"))
    if model.startswith("claude"):
        return bool(os.environ.get("ANTHROPIC_API_KEY"))
    if model.startswith("gemini"):
        return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    # Local providers (ollama / mlx) don't need a key
    return True


def match(pred_list: list[dict], truth_list: list[dict],
          threshold: float = 0.40,
          model: Optional[str] = None,
          api_key: Optional[str] = None) -> dict:
    """LLM-judge bipartite match. Same return shape as
    backend.scoring.ingredient_match so the two are drop-in
    interchangeable."""
    pred_list = pred_list or []
    truth_list = truth_list or []
    if not pred_list or not truth_list:
        return _empty_result(pred_list, truth_list)

    model = model or DEFAULT_MODEL
    cache_key = _cache_key(pred_list, truth_list, model)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        raw = _call_judge(pred_list, truth_list, model, api_key)
        parsed = _parse_judge_output(raw, pred_list, truth_list)
    except Exception as e:
        # Degrade gracefully: token matcher is still the safety net.
        from .scoring import ingredient_match as token_match
        return token_match(pred_list, truth_list, threshold=threshold)

    result = _build_result(parsed, pred_list, truth_list, threshold)
    _cache_put(cache_key, result, model)
    return result


# ── prompt + parsing ──────────────────────────────────────────────────────

_SYS_PROMPT = """You are a culinary expert evaluating how accurately an LLM identified the ingredients of a photographed dish.

Your task: pair each TRUTH ingredient with the MODEL's most appropriate ingredient, OR mark it unmatched if the model didn't include it.

Treat these as the SAME ingredient (high score):
  • Translations and regional names: tahini ≡ sesame paste, mutabbal ≡ baba ganoush, aubergine ≡ eggplant, cilantro ≡ coriander leaves, garbanzo ≡ chickpea, courgette ≡ zucchini, rocket ≡ arugula
  • Plurals and forms: strawberry ≡ strawberries, tomato ≡ tomatoes
  • Sub-types where the model used the general name: "rice" against "Rice, Basmati" is a partial-but-strong match
  • Cooking-state differences: "egg, scrambled" vs "egg, boiled" — same ingredient, different prep

Treat these as DIFFERENT ingredients (zero or low score):
  • Different fat / processing variants: whole milk ≠ skim milk, full-fat yogurt ≠ low-fat yogurt
  • Different grain refinement: brown rice ≠ white rice, whole wheat bread ≠ white bread
  • Different fillings sharing a form: apple pie ≠ cherry pie, strawberry juice ≠ watermelon juice, olive oil ≠ vegetable oil
  • Different proteins: chicken ≠ beef, salmon ≠ tuna

Score each match 0.0–1.0:
  • 1.00: identical, translation, or trivial plural/form difference
  • 0.70–0.95: same ingredient with minor sub-type or cooking-state difference
  • 0.40–0.65: same general food kind but real detail mismatch (chicken whole vs chicken breast)
  • 0.00–0.30: different ingredients

The match should be a 1-to-1 assignment (no truth pairs with two preds, no pred pairs with two truths). Pick the strongest available pairing for each.

Output ONLY this JSON, no commentary:
{
  "matches": [
    {"truth_idx": <int>, "pred_idx": <int>, "score": <float>, "reason": "<one short sentence>"}
  ],
  "unmatched_truth": [<int>, ...],
  "unmatched_pred": [<int>, ...]
}

The `truth_idx` / `pred_idx` are 1-based positions in the lists I send you."""


def _build_user_prompt(pred_list: list[dict], truth_list: list[dict]) -> str:
    def fmt(items: list[dict]) -> str:
        out = []
        for i, x in enumerate(items, start=1):
            name = (x.get("name") or "").strip() or "?"
            qty = x.get("quantity")
            unit = x.get("unit") or "g"
            qty_part = f" ({qty} {unit})" if qty is not None else ""
            out.append(f"  {i}. {name}{qty_part}")
        return "\n".join(out)

    return (
        f"TRUTH INGREDIENTS ({len(truth_list)}):\n{fmt(truth_list)}\n\n"
        f"MODEL INGREDIENTS ({len(pred_list)}):\n{fmt(pred_list)}\n\n"
        f"Return the JSON now."
    )


def _parse_judge_output(text: str, pred_list, truth_list) -> dict:
    """Parse and normalise the judge's JSON output. 1-based indices in
    the wire format, 0-based internally."""
    obj = parse_json_loose(text) or {}
    n_p, n_t = len(pred_list), len(truth_list)
    matches = []
    used_p, used_t = set(), set()
    for m in (obj.get("matches") or []):
        try:
            pi = int(m.get("pred_idx", 0)) - 1
            ti = int(m.get("truth_idx", 0)) - 1
            score = float(m.get("score", 0))
        except (TypeError, ValueError):
            continue
        if not (0 <= pi < n_p and 0 <= ti < n_t):
            continue
        if pi in used_p or ti in used_t:
            continue
        used_p.add(pi); used_t.add(ti)
        matches.append({"pi": pi, "ti": ti, "score": max(0.0, min(1.0, score)),
                        "reason": str(m.get("reason") or "")})
    return {"matches": matches, "n_p": n_p, "n_t": n_t,
            "used_p": used_p, "used_t": used_t}


def _build_result(parsed: dict, pred_list, truth_list, threshold: float) -> dict:
    """Convert parsed judge output to the standard ingredient_match shape."""
    matches = []
    matched_truth = set()
    used_pred = set()
    for m in parsed["matches"]:
        if m["score"] < threshold:
            continue
        pi, ti = m["pi"], m["ti"]
        used_pred.add(pi); matched_truth.add(ti)
        p = pred_list[pi]; t = truth_list[ti]
        # Quantity score uses the existing helper
        from .scoring import _quantity_score
        qs = _quantity_score(p.get("quantity"), t.get("quantity"))
        matches.append({
            "truth_idx": ti, "pred_idx": pi,
            "truth_name": t.get("name"), "pred_name": p.get("name"),
            "truth_qty": t.get("quantity"), "pred_qty": p.get("quantity"),
            "unit": t.get("unit") or p.get("unit") or "g",
            "name_sim": round(m["score"], 3),
            "weight_score": (None if qs is None else round(qs, 3)),
            "reason": m["reason"],   # extra: LLM's explanation
        })
    matches.sort(key=lambda m: m["truth_idx"])

    n_p = parsed["n_p"]; n_t = parsed["n_t"]
    matched = len(matches)
    precision = matched / n_p if n_p else 0.0
    recall = matched / n_t if n_t else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    valid_w = [m["weight_score"] for m in matches if m["weight_score"] is not None]
    weight_acc = sum(valid_w) / len(valid_w) if valid_w else 0.0

    unmatched_truth = [
        {"truth_idx": i, "name": truth_list[i].get("name"),
         "qty": truth_list[i].get("quantity"), "unit": truth_list[i].get("unit") or "g"}
        for i in range(n_t) if i not in matched_truth
    ]
    unmatched_pred = [
        {"pred_idx": i, "name": pred_list[i].get("name"),
         "qty": pred_list[i].get("quantity"), "unit": pred_list[i].get("unit") or "g"}
        for i in range(n_p) if i not in used_pred
    ]
    return {
        "matches": matches,
        "unmatched_truth": unmatched_truth,
        "unmatched_pred": unmatched_pred,
        "n_pred": n_p, "n_truth": n_t, "matched": matched,
        "precision": round(precision, 3), "recall": round(recall, 3),
        "f1": round(f1, 3), "weight_acc": round(weight_acc, 3),
        "judged_by": "llm",   # provenance flag for the UI
    }


def _empty_result(pred_list, truth_list) -> dict:
    return {
        "matches": [],
        "unmatched_truth": [{"truth_idx": i, "name": t.get("name"),
                             "qty": t.get("quantity"), "unit": t.get("unit") or "g"}
                            for i, t in enumerate(truth_list)],
        "unmatched_pred": [{"pred_idx": i, "name": p.get("name"),
                            "qty": p.get("quantity"), "unit": p.get("unit") or "g"}
                           for i, p in enumerate(pred_list)],
        "n_pred": len(pred_list), "n_truth": len(truth_list), "matched": 0,
        "precision": 0.0, "recall": 0.0, "f1": 0.0, "weight_acc": 0.0,
        "judged_by": "llm",
    }


# ── provider dispatch ─────────────────────────────────────────────────────

def _call_judge(pred_list, truth_list, model: str, api_key: Optional[str]) -> str:
    """One LLM call. Returns raw text response."""
    user = _build_user_prompt(pred_list, truth_list)
    m = model.lower()
    if m.startswith(("gpt", "o1", "o3", "o4")):
        return _call_openai(model, _SYS_PROMPT, user, api_key)
    if m.startswith("claude"):
        return _call_anthropic(model, _SYS_PROMPT, user, api_key)
    if m.startswith("gemini"):
        return _call_gemini(model, _SYS_PROMPT, user, api_key)
    # Default: ollama
    return _call_ollama(model, _SYS_PROMPT, user)


def _call_openai(model, system, user, api_key):
    from openai import OpenAI
    cli = OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))
    r = cli.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0,
    )
    return r.choices[0].message.content or ""


def _call_anthropic(model, system, user, api_key):
    import anthropic
    cli = anthropic.Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))
    r = cli.messages.create(
        model=model, max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": user}],
        temperature=0,
    )
    return "".join(b.text for b in r.content if hasattr(b, "text"))


def _call_gemini(model, system, user, api_key):
    import google.generativeai as genai
    genai.configure(api_key=api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    m = genai.GenerativeModel(model_name=model, system_instruction=system,
                              generation_config={"response_mime_type": "application/json",
                                                  "temperature": 0})
    r = m.generate_content(user)
    return r.text or ""


def _call_ollama(model, system, user):
    import httpx
    base = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
    r = httpx.post(f"{base}/api/chat", timeout=180,
                   json={
                       "model": model,
                       "messages": [{"role": "system", "content": system},
                                    {"role": "user", "content": user}],
                       "format": "json",
                       "options": {"temperature": 0},
                       "stream": False,
                   })
    r.raise_for_status()
    return r.json().get("message", {}).get("content", "")


# ── cache (SQLite, separate table) ────────────────────────────────────────

def _ensure_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    with _LOCK:
        if _TABLE_READY:
            return
        with db._conn() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS judge_cache (
                    key TEXT PRIMARY KEY,
                    model TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
        _TABLE_READY = True


def _canonical_for_hash(items: list[dict]) -> list[dict]:
    """Strip volatile fields so the cache key only depends on what
    actually affects the matching decision."""
    out = []
    for x in items:
        name = re.sub(r"\s+", " ", str(x.get("name") or "").strip().lower())
        qty = x.get("quantity")
        unit = (x.get("unit") or "g").lower().strip()
        out.append({"n": name, "q": qty, "u": unit})
    return out


def _cache_key(pred_list, truth_list, model: str) -> str:
    payload = json.dumps(
        {"m": model.lower(),
         "p": _canonical_for_hash(pred_list),
         "t": _canonical_for_hash(truth_list)},
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode()).hexdigest()


def _cache_get(key: str) -> Optional[dict]:
    _ensure_table()
    with db._conn() as c:
        r = c.execute("SELECT result_json FROM judge_cache WHERE key=?", (key,)).fetchone()
        if not r:
            return None
        try:
            return json.loads(r["result_json"])
        except Exception:
            return None


def _cache_put(key: str, result: dict, model: str) -> None:
    _ensure_table()
    with db._conn() as c:
        c.execute("INSERT OR REPLACE INTO judge_cache (key, model, result_json, created_at) "
                  "VALUES (?,?,?,?)",
                  (key, model.lower(), json.dumps(result), time.time()))


def cache_size() -> int:
    _ensure_table()
    with db._conn() as c:
        return int(c.execute("SELECT COUNT(*) FROM judge_cache").fetchone()[0])
