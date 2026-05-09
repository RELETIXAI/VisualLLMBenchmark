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
    model = os.environ.get("LLM_JUDGE_MODEL") or DEFAULT_MODEL
    # MLX local checkpoints are always usable (no API key, no network)
    if os.path.isdir(model):
        return True
    m = model.lower()
    if m.startswith(("gpt", "o1", "o3", "o4")):
        return bool(os.environ.get("OPENAI_API_KEY"))
    if m.startswith("claude"):
        return bool(os.environ.get("ANTHROPIC_API_KEY"))
    if m.startswith("gemini"):
        return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    # Local providers (ollama / mlx via HF identifier) don't need a key
    return True


def match(pred_list: list[dict], truth_list: list[dict],
          threshold: float = 0.40,
          model: Optional[str] = None,
          api_key: Optional[str] = None,
          base_url: Optional[str] = None,
          pred_food: Optional[str] = None,
          truth_food: Optional[str] = None) -> dict:
    """LLM-judge bipartite match for ingredients **and** food-name scoring
    in a single call. Returned dict matches `backend.scoring.ingredient_match`
    plus two extra fields:
        - food_score (float | None): 0–1, how well pred_food describes
          the same dish as truth_food. None when either side is missing.
        - food_reason (str): short category label from the prompt rubric.

    base_url is honoured for OpenAI-compatible local endpoints
    (LM Studio at http://localhost:1234/v1) and Ollama
    (http://localhost:11434).
    """
    pred_list = pred_list or []
    truth_list = truth_list or []
    pred_food = (pred_food or "").strip() or None
    truth_food = (truth_food or "").strip() or None

    # Skip the LLM call entirely when there's nothing to judge: both
    # ingredient lists empty AND no food-name pair to compare.
    if not pred_list and not truth_list and not (pred_food and truth_food):
        return _empty_result(pred_list, truth_list)

    model = model or DEFAULT_MODEL
    cache_key = _cache_key(pred_list, truth_list, model,
                           pred_food=pred_food, truth_food=truth_food)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        raw = _call_judge(pred_list, truth_list, model, api_key, base_url,
                          pred_food=pred_food, truth_food=truth_food)
        parsed = _parse_judge_output(raw, pred_list, truth_list)
    except Exception as e:
        # Friendly degrade: log the cause then fall back to the rule-based
        # matcher so the run / rescore doesn't break entirely.
        msg = str(e)
        if "context length" in msg.lower() or "token" in msg.lower() and "greater than" in msg.lower():
            print(f"[llm_judge] context overflow on {model}: {msg[:200]}")
            print("[llm_judge] HINT: in LM Studio, reload the model with a larger "
                  "n_ctx (8192+); or pick a model with a larger native context.")
        elif "connect" in msg.lower() or "refused" in msg.lower():
            print(f"[llm_judge] cannot reach {model}: {msg[:200]}")
            print("[llm_judge] HINT: is LM Studio's server started, or Ollama running?")
        else:
            print(f"[llm_judge] {type(e).__name__}: {msg[:200]}")
        from .scoring import ingredient_match as token_match
        return token_match(pred_list, truth_list, threshold=threshold)

    result = _build_result(parsed, pred_list, truth_list, threshold)
    _cache_put(cache_key, result, model)
    return result


# ── prompt + parsing ──────────────────────────────────────────────────────

_SYS_PROMPT = """Score a food-photo benchmark row. Two tasks in ONE response:

  TASK A — Score the DISH NAME match (one number, 0–1).
  TASK B — Match the INGREDIENT LISTS, 1-to-1 bipartite assignment.

==============================================================
NO-DOUBLE-PENALTY RULE  (read this first, applies to BOTH tasks)
==============================================================
The benchmark scores nutrition (calories, fat, carbs, protein, fiber,
sugar, sodium) as a SEPARATE sub-score. Any difference that ONLY shows
up as a calorie / macro difference is already captured there.

Therefore, when you score names (dish or ingredient), you MUST IGNORE
modifiers whose entire effect is on macros. The IDENTITY of the food is
what matters here, not its nutritional grade.

MACRO-ONLY MODIFIERS — strip / ignore when comparing names:
  • fat / dairy grade ........ skim, skimmed, whole, full-fat,
                                low-fat, reduced-fat, fat-free,
                                semi-skimmed, 2%, 1%
  • grain refinement ......... brown, white, refined, wholegrain,
                                wholewheat, whole-wheat, multigrain
  • sweetness / salt ......... sweetened, unsweetened, salted,
                                unsalted, no-sugar, no-salt,
                                lightly-salted
  • prep / cooking state ..... raw, cooked, fresh, dried, frozen,
                                grilled, boiled, fried, baked,
                                roasted, steamed, scrambled,
                                poached, toasted, smoked
  • cut / portion ............ breast, thigh, fillet, drumstick, leg,
                                wing, whole (when applied to a meat
                                or vegetable), chunks, ground, sliced,
                                diced, chopped, mashed, shredded
  • ripeness / colour ........ ripe, unripe, green, red, dark,
                                light (when applied to a single food)
  • origin / processing ...... organic, free-range, grass-fed,
                                homemade, instant, natural

If two names differ ONLY by these modifiers (or any combination of
them), score them as IDENTICAL (1.00 for ingredients, 1.00 for dish
name). Their macro consequence is captured by the nutrition sub-score
and would otherwise be punished twice.

WHAT IS *NOT* a macro-only modifier — these are real identity changes:
  • different species / SKU ... olive oil vs vegetable oil,
                                strawberry vs watermelon, soy milk
                                vs cow milk, almond flour vs wheat
                                flour, brown sugar vs white sugar
                                are macro-only ONLY when the underlying
                                food is the same; "soy" and "wheat"
                                ARE different SKUs, not modifiers.
  • different protein / cut
    that's actually a
    different animal ......... beef vs chicken, salmon vs tuna,
                                pork vs lamb
  • different fruit / veg /
    grain entirely ........... apple vs cherry (in pies),
                                rice vs quinoa, basmati rice vs
                                jasmine rice (subtype, score 0.85)

Rule of thumb: would a reasonable shopper buy the SAME PRODUCT FROM THE
SAME SHELF, with only a different label sub-line? If yes → identical.
If they'd grab a different product from a different shelf → different.

==============================================================
TASK A — DISH NAME (food_score, 0–1)
==============================================================
Score how well MODEL's dish name describes the SAME dish as TRUTH's name.
Use real understanding of cuisine, not string overlap. Apply the
no-double-penalty rule above.

DISH-NAME TIERS:
- 1.00  same dish. Differences are paraphrase / translation / word-order
        / morphology, OR consist only of macro-only modifiers from the
        list above.
        e.g. "oatmeal with milk" = "oats with skimmed milk" = "oats with
              milk" = "oats with whole milk"  (all 1.00 — fat grade is
              captured in macros)
        e.g. "chicken biryani" = "biryani with chicken" (word-order)
        e.g. "aubergine moussaka" = "eggplant moussaka" (translation)
        e.g. "grilled chicken sandwich" = "chicken sandwich" (prep word)
- 0.85  same dish, real specifier difference that ISN'T macro-only.
        e.g. "rice" vs "basmati rice" (subtype — basmati is a specific
              cultivar, but the dish-name still refers to the same plate)
        e.g. "fattoush" vs "fattoush salad" (qualifier within the same
              cuisine)
- 0.65  same dish family but a real content variant.
        e.g. "chicken biryani" vs "vegetable biryani" (different protein
              source — biryani template, different actual filling)
        e.g. "beef shawarma" vs "chicken shawarma"
- 0.40  same form / category but different actual food.
        e.g. "strawberry juice" vs "watermelon juice"
        e.g. "apple pie" vs "cherry pie"
        e.g. "olive oil" vs "vegetable oil"
- 0.00  different dish entirely.
        e.g. "Greek salad" vs "Egyptian breakfast plate"
        e.g. "pizza" vs "sushi"

food_reason: ONE of these labels, optionally with a brief clarifier:
  identical, paraphrase, translation, macros-only-modifier,
  specifier-only, variant-mismatch, same-form-different-content,
  different-dish.

==============================================================
TASK B — INGREDIENT MATCHING (bipartite, 1-to-1)
==============================================================
COVERAGE RULE — read this first.
Task B is INDEPENDENT of Task A. The dish names may disagree, be worded
differently, or even describe different dishes — that affects food_score
ONLY, never the ingredient matching. Evaluate every pred×truth pair on
its own identity merit.

If a pred ingredient and a truth ingredient share the same identity
(identical or trivially equivalent names — synonyms, plurals, or names
that differ only by macro-only modifiers), you MUST emit a match.
Stopping early because "the dish name doesn't mention this item" is
incorrect reasoning — that observation belongs to Task A only.

Coverage is mandatory, not optional. Before leaving any truth ingredient
unmatched, confirm that no pred ingredient is its identity equal.

Apply the no-double-penalty rule first, then assign:

- 1.00  same ingredient, including macro-only modifier differences:
          milk = skim milk = whole milk = low-fat milk
          rice = brown rice = white rice = wholegrain rice
          chicken = chicken breast = chicken thigh = grilled chicken
          yogurt = full-fat yogurt = unsweetened yogurt
          bread = wholewheat bread = white bread
- 0.95  translation / regional name / plural-singular only:
          tahini = sesame paste, mutabbal = baba ganoush,
          aubergine = eggplant, cilantro = coriander,
          chickpea = garbanzo, strawberry = strawberries
- 0.85  same ingredient family, real subtype that's NOT macro-only:
          rice vs basmati rice (specific cultivar),
          cheese vs cheddar (specific variety),
          apple vs granny smith
        (these are sub-types within the same product line, but the
         specific subtype carries information beyond macros — variety,
         origin, or texture — so partial credit only)
- 0.65  composite-vs-decomposition partial: TRUTH names a single composite
        dish (sponge cake, bread bun, lasagna, cheesecake) and MODEL lists
        ONE of its main raw ingredients (flour, sugar, eggs, butter).
        Pair the composite with the most representative raw ingredient,
        leave the other raw ingredients unmatched. Same in reverse.
- 0.00  different ingredient identity:
          chicken vs beef, salmon vs tuna, olive oil vs vegetable oil,
          strawberry vs watermelon, soy milk vs cow milk, almond flour
          vs wheat flour, apple vs cherry.
          Same FORM / DIFFERENT CONTENT counts as different.

REASON FIELD per match: short category label, NOT a restatement of the names.
Use one of: identical, macros-only-modifier, translation, plural,
subtype, composite-decomposition, different-item.
Optionally append " — <one-clause clarification>".

Examples:
  truth="skim milk", pred="milk"
    → score 1.00, reason "macros-only-modifier — fat grade captured in macros"
  truth="brown rice", pred="white rice"
    → score 1.00, reason "macros-only-modifier — refinement captured in macros"
  truth="basmati rice", pred="rice"
    → score 0.85, reason "subtype — basmati is a specific cultivar"
  truth="olive oil", pred="vegetable oil"
    → score 0.00, reason "different-item — different oil source"
  truth="apple", pred="cherry"
    → score 0.00, reason "different-item — different fruit"

==============================================================
OUTPUT
==============================================================
Output ONLY this JSON, no prose, no markdown fences:
{"food_score":0.85,"food_reason":"<label>","matches":[{"truth_idx":1,"pred_idx":1,"score":0.95,"reason":"<label>"}],"unmatched_truth":[],"unmatched_pred":[]}

Indices are 1-based positions in the lists below.
If TRUTH or MODEL has no dish name, set food_score to null and
food_reason to "no-name". If both ingredient lists are empty, return
matches:[] and skip the bipartite step."""


def _build_user_prompt(pred_list: list[dict], truth_list: list[dict],
                       pred_food: Optional[str] = None,
                       truth_food: Optional[str] = None) -> str:
    def fmt(items: list[dict]) -> str:
        if not items:
            return "  (none)"
        out = []
        for i, x in enumerate(items, start=1):
            name = (x.get("name") or "").strip() or "?"
            qty = x.get("quantity")
            unit = x.get("unit") or "g"
            qty_part = f" ({qty} {unit})" if qty is not None else ""
            out.append(f"  {i}. {name}{qty_part}")
        return "\n".join(out)

    food_block = (
        f"TRUTH DISH NAME: {truth_food or '(missing)'}\n"
        f"MODEL DISH NAME: {pred_food or '(missing)'}\n\n"
    )

    return (
        food_block +
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

    # Food-name score: optional, may be null when one side has no name.
    food_score = obj.get("food_score")
    if food_score is not None:
        try:
            food_score = max(0.0, min(1.0, float(food_score)))
        except (TypeError, ValueError):
            food_score = None
    food_reason = str(obj.get("food_reason") or "")

    return {"matches": matches, "n_p": n_p, "n_t": n_t,
            "used_p": used_p, "used_t": used_t,
            "food_score": food_score, "food_reason": food_reason}


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
        "food_score": (None if parsed.get("food_score") is None
                       else round(parsed["food_score"], 3)),
        "food_reason": parsed.get("food_reason") or "",
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
        "food_score": None,
        "food_reason": "",
    }


# ── provider dispatch ─────────────────────────────────────────────────────

def _call_judge(pred_list, truth_list, model: str, api_key: Optional[str],
                base_url: Optional[str] = None,
                pred_food: Optional[str] = None,
                truth_food: Optional[str] = None) -> str:
    """One LLM call. Returns raw text response."""
    user = _build_user_prompt(pred_list, truth_list,
                              pred_food=pred_food, truth_food=truth_food)
    m = model.lower()

    # If the caller passed an explicit base_url, that's the strongest
    # signal: use OpenAI-compat client pointed at it. LM Studio,
    # vLLM-server, llama-server, anything else OpenAI-compat → all
    # land here. Ollama is handled below if base_url contains 11434.
    if base_url:
        bu = base_url.rstrip("/")
        if "11434" in bu:
            return _call_ollama(model, _SYS_PROMPT, user, base_url=bu)
        # Default for any /v1 OpenAI-compat endpoint (LM Studio,
        # llama-server, vLLM, etc.). LM Studio doesn't require a key
        # — we pass a dummy if none was provided.
        return _call_openai(model, _SYS_PROMPT, user,
                            api_key=api_key or "lm-studio",
                            base_url=bu)

    # MLX local checkpoint — model id is a filesystem path. Reuses the
    # model cache from backend.providers.mlx_provider, so if a benchmark
    # run already loaded gemma-4-26B-A4B-it-4bit, the judge skips the
    # 15 GB load and goes straight to inference.
    if os.path.isdir(model):
        return _call_mlx(model, _SYS_PROMPT, user)

    if m.startswith(("gpt", "o1", "o3", "o4")):
        return _call_openai(model, _SYS_PROMPT, user, api_key)
    if m.startswith("claude"):
        return _call_anthropic(model, _SYS_PROMPT, user, api_key)
    if m.startswith("gemini"):
        return _call_gemini(model, _SYS_PROMPT, user, api_key)

    # Treat any "namespace/repo[…]" identifier without a colon as an
    # MLX HuggingFace checkpoint (e.g. "mlx-community/gemma-4-…-4bit").
    # Ollama uses "name:tag" format, so the colon distinguishes them.
    if "/" in model and ":" not in model:
        return _call_mlx(model, _SYS_PROMPT, user)

    # Default: Ollama (for "gemma3:4b", "qwen2.5vl:7b", etc.)
    return _call_ollama(model, _SYS_PROMPT, user)


def _call_mlx(model_id: str, system: str, user: str) -> str:
    """Run text-only inference on an MLX VLM checkpoint. Shares the
    model cache with backend.providers.mlx_provider so the judge and
    the benchmark runner don't double-load 15 GB of weights when they
    happen to use the same model."""
    from .providers.mlx_provider import _load_model
    from mlx_vlm import generate
    from mlx_vlm.prompt_utils import apply_chat_template

    model, processor, config = _load_model(model_id)
    # Some VLMs require system + user joined for tokeniser; mlx_vlm's
    # apply_chat_template wants a string, not a messages list. Concat
    # explicitly so the system block is preserved.
    full = f"{system}\n\n{user}"
    prompt = apply_chat_template(processor, config, full, num_images=0)
    output = generate(model, processor, prompt=prompt, image=None,
                      max_tokens=8192, verbose=False)
    return output if isinstance(output, str) else getattr(output, "text", str(output))


def _call_openai(model, system, user, api_key, base_url: Optional[str] = None):
    from openai import OpenAI
    kwargs = {"api_key": api_key or os.environ.get("OPENAI_API_KEY") or "sk-no-key"}
    if base_url:
        kwargs["base_url"] = base_url
    cli = OpenAI(**kwargs)
    # response_format=json_object isn't universally supported on OpenAI-
    # compatible local endpoints (LM Studio honours it; some llama-server
    # builds reject it). Try with first, retry without on TypeError /
    # BadRequestError-from-the-server-side parameter rejection.
    base_messages = [{"role": "system", "content": system},
                     {"role": "user", "content": user}]
    try:
        r = cli.chat.completions.create(
            model=model, messages=base_messages,
            response_format={"type": "json_object"},
            temperature=0,
        )
    except Exception as e:
        # Common signatures: "response_format' is not supported", "json mode"
        if "response_format" in str(e) or "json" in str(e).lower():
            r = cli.chat.completions.create(
                model=model, messages=base_messages, temperature=0,
            )
        else:
            raise
    return r.choices[0].message.content or ""


def _call_anthropic(model, system, user, api_key):
    import anthropic
    cli = anthropic.Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))
    r = cli.messages.create(
        model=model, max_tokens=8192,
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


def _call_ollama(model, system, user, base_url: Optional[str] = None):
    import httpx
    base = (base_url or os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")).rstrip("/")
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


def _cache_key(pred_list, truth_list, model: str,
               pred_food: Optional[str] = None,
               truth_food: Optional[str] = None) -> str:
    def _canon_name(s: Optional[str]) -> str:
        if not s:
            return ""
        return re.sub(r"\s+", " ", s.strip().lower())

    payload = json.dumps(
        {"m": model.lower(),
         "p": _canonical_for_hash(pred_list),
         "t": _canonical_for_hash(truth_list),
         "pf": _canon_name(pred_food),
         "tf": _canon_name(truth_food)},
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
