"""Score model outputs vs ground truth.

Sub-scores
----------
- name_sim         : food name match (containment-aware F1, lenient)
- macros_avg       : aggregate calorie/macro tolerance scoring (weighted)
- ingredient_f1    : F1 of best-match ingredient pairs by name
- weight_acc       : avg per-matched-ingredient quantity tolerance score
- health_acc       : grade (A..E) match with graded penalty for off-by-N
- overall          : weighted combination (configurable)

Composite leaderboard score then folds in latency + cost.
"""
from __future__ import annotations

import math
import re
from typing import Any

NUTRIENT_KEYS = ["calories", "protein_g", "carbs_g", "fat_g", "fiber_g", "sugar_g", "sodium_mg"]

NUTRIENT_TOL: dict[str, dict[str, float]] = {
    "calories":  {"abs": 50.0,  "rel": 0.12, "weight": 3.0},
    "protein_g": {"abs": 5.0,   "rel": 0.20, "weight": 1.8},
    "carbs_g":   {"abs": 8.0,   "rel": 0.20, "weight": 1.8},
    "fat_g":     {"abs": 4.0,   "rel": 0.25, "weight": 1.5},
    "sodium_mg": {"abs": 200.0, "rel": 0.35, "weight": 1.0},
    "sugar_g":   {"abs": 4.0,   "rel": 0.40, "weight": 0.8},
    "fiber_g":   {"abs": 3.0,   "rel": 0.40, "weight": 0.6},
}
DECAY_MULTIPLIER = 3.0

# Sub-score weights for the row-level overall score
OVERALL_WEIGHTS = {
    "macros":         0.35,
    "ingredient_f1":  0.30,
    "weight_acc":     0.15,
    "health":         0.15,
    "name":           0.05,
}

# Quantity tolerance for ingredient weight accuracy
WEIGHT_ABS_TOL = 10.0   # ±10 g/ml
WEIGHT_REL_TOL = 0.30   # ±30%

INGREDIENT_MATCH_THRESHOLD = 0.40   # name similarity required to count as a match


# ----------- text similarity -----------
# Pure connectives (no food identity) and macro-only modifiers.
#
# These tokens are stripped before name comparison. The principle is
# **no double penalty**: any difference whose only effect is on macros
# (calories, fat, fibre, sugar, salt, …) is ALREADY captured by the
# nutrition sub-score. If the rule-based name match also penalised it,
# the same difference would count twice.
#
# Examples this set lets through as 1.00 matches:
#   • "milk" ↔ "skim milk" / "whole milk" / "full-fat milk"
#   • "rice" ↔ "brown rice" / "white rice" / "wholegrain rice"
#   • "chicken" ↔ "chicken breast" / "grilled chicken thigh"
#   • "yogurt" ↔ "unsweetened yogurt" / "low-fat yogurt"
#
# What is NOT here (kept identity-bearing): different species or SKUs —
# olive oil vs vegetable oil, soy milk vs cow milk, almond flour vs
# wheat flour. Those tokens stay in the comparison and produce honest
# disagreement.
_STOP = {
    # connectives
    "the", "a", "an", "of", "with", "and", "or", "in", "to",
    # prep / cooking state (macro-only — how it was cooked, not what it is)
    "raw", "cooked", "fresh", "dried", "frozen", "processed",
    "homemade", "instant", "natural",
    "scrambled", "boiled", "fried", "baked", "grilled", "steamed",
    "roasted", "poached",
    "blended", "whipped", "toasted", "mashed", "crushed",
    "chopped", "sliced", "grated", "ground", "shredded", "diced",
    "peeled", "seeded", "deboned", "minced",
    # cut / portion (macro-only — different cut of the same animal/plant)
    "breast", "thigh", "fillet", "drumstick", "leg", "wing",
    "chunk", "chunks", "piece", "pieces",
    # fat / dairy grade (macro-only — fat content captured in macros)
    "whole", "full", "skim", "skimmed", "semi", "fat", "low",
    "reduced", "free",
    # grain refinement (macro-only — fibre / carb captured in macros).
    # Note: "wheat" is intentionally NOT here — it's a grain identity
    # ("almond flour" vs "wheat flour" must stay different).
    "brown", "white", "refined", "wholegrain", "wholewheat", "multigrain",
    # sweetness / salt (macro-only — sugar / sodium captured in macros)
    "sweetened", "unsweetened", "salted", "unsalted",
    "sweet", "savoury", "savory",
    # ripeness / colour / processing (macro-only)
    "dark", "light", "smoked", "ripe", "unripe", "organic", "lean",
    # NOTE: flavour-prep words (spiced / seasoned / marinated / herbed /
    # flavoured) are deliberately KEPT in the comparison. They describe a
    # real difference between SKUs (shawarma-spiced chicken ≠ plain chicken)
    # even though both share the same head noun. Per user request the
    # matcher stays conservative on naming.
}
# Kept as an empty set so older code paths that still reference it don't
# need a coordinated rewrite. The "identity-conflict" gate that used it
# was removed when the no-double-penalty rule was introduced.
_IDENTITY_MODS: set[str] = set()

# Pairs of tokens the semantic model (all-MiniLM) confuses because of
# shared character prefixes — they're embedded close (cosine ~0.5) but
# refer to entirely different foods. If pred contains one element and
# truth contains the other, the semantic boost is suppressed and the
# token pipeline's score stands. Extend this list as new false friends
# surface in real comparisons.
_SEMANTIC_FALSE_FRIENDS = [
    frozenset({"chicken", "chickpea"}),
    frozenset({"chicken", "chickpeas"}),
    frozenset({"chicken", "garbanzo"}),
]


# When the only shared token between two short ingredient names is a
# "category word" (a container, form, or vehicle rather than a food), the
# match is almost always coincidental: the contents differ even when the
# form is identical — strawberry juice ≠ watermelon juice; olive oil ≠
# vegetable oil; apple pie ≠ cherry pie. The short-name false-pair guard
# below only fires when the shared token is in this set, so genuine food
# kinds (chicken, beef, lettuce, rice, pasta) don't get rejected when only
# the cut/sub-type differs.
_GENERIC_CATEGORY_TOKENS = {
    # liquid derivatives
    "juice", "oil", "sauce", "syrup", "dressing",
    "soup", "stew", "broth", "stock",
    # beverages
    "wine", "beer", "tea", "coffee", "drink", "beverage",
    # baked / sweet forms
    "pie", "cake", "bread", "bun", "roll", "biscuit", "cookie",
    "muffin", "tart", "pastry", "pancake", "waffle",
    # processed forms
    "paste", "powder", "marinade", "spread",
}


# Domain synonyms — applied BEFORE tokenisation so two strings expressing
# the same ingredient with different vocabulary collapse to identical
# token sets and score 1.0 via the token pipeline alone (no semantic
# fallback needed).
#
# Only bidirectional certainties go here. If any reasonable doubt
# remains about whether two terms denote the same SKU (e.g. yogurt vs
# labneh — same base food, different processing/water content), leave
# them out and let token + semantic logic do its honest job. This list
# is intentionally short and curated for the benchmark's Middle Eastern
# / Mediterranean food domain.
#
# Format: canonical → list of equivalent phrases. The phrases get
# replaced (case-insensitive, word-boundary-safe) with the canonical
# before _tokens() runs.
SYNONYMS: dict[str, list[str]] = {
    # Middle Eastern / Levantine
    "tahini":     ["sesame paste", "tahina", "tahin"],
    "mutabbal":   ["baba ganoush", "baba ghanoush", "moutabal", "muttabal"],
    "hummus":     ["hommos", "hummous", "houmous"],
    "labneh":     ["labne", "labna"],
    "ghee":       ["samneh", "samna", "clarified butter"],
    "halloumi":   ["halloum"],
    "shawarma":   ["shwarma", "shawerma"],
    "kibbeh":     ["kibbe", "kebbeh", "kibbi"],
    "manakish":   ["manaqish", "manoushe", "manakeesh"],
    "fattoush":   ["fattouche", "fatoush"],
    "tabbouleh":  ["taboulleh", "taboule", "tabouli"],
    "molokhia":   ["mlukhiyah", "molokhiya", "moroheya", "mloukhia"],
    "freekeh":    ["frekeh", "frikeh", "freek"],
    "bulgur":     ["bulgar", "burghul", "cracked wheat"],
    "fava":       ["broad bean", "broad beans", "ful"],
    "kishk":      ["kashk"],
    # English botanical / regional synonyms common in food labels
    "eggplant":   ["aubergine", "brinjal"],
    "cilantro":   ["coriander leaves", "fresh coriander"],
    "scallion":   ["green onion", "spring onion"],
    "chickpea":   ["garbanzo", "garbanzo bean"],
    "zucchini":   ["courgette"],
    "arugula":    ["rocket"],
    "bell_pepper":["bell pepper", "capsicum", "sweet pepper"],
    "yogurt":     ["yoghurt"],
}

# Build a (phrase → canonical) lookup, longest-phrase-first so multi-word
# phrases like "sesame paste" are tried before single words.
_SYN_LOOKUP: list[tuple[re.Pattern, str]] = []
for canonical, phrases in SYNONYMS.items():
    for phrase in sorted(set(phrases) | {canonical}, key=lambda p: -len(p)):
        # \b word boundaries so "olive oil" doesn't gobble part of "olive-oil-cured"
        pat = re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE)
        _SYN_LOOKUP.append((pat, canonical))
# Sort overall list by phrase length descending so longest matches first
_SYN_LOOKUP.sort(key=lambda x: -len(x[0].pattern))


def _apply_synonyms(s: str) -> str:
    if not s:
        return s
    for pat, canonical in _SYN_LOOKUP:
        s = pat.sub(canonical, s)
    return s


_PAREN_RE = re.compile(r"\(([^)]*)\)")


def _split_paren(s: str) -> tuple[str, str]:
    """Return (outer, inner) where outer = original with (...) stripped,
    inner = content concatenated. Either may be empty."""
    if not s:
        return "", ""
    inner_parts = _PAREN_RE.findall(s)
    outer = _PAREN_RE.sub(" ", s)
    return outer, " ".join(inner_parts)


def _stem(t: str) -> str:
    """Conservative English plural stripping so 'strawberries' matches
    'strawberry' through the token pipeline alone (without falling back
    to the semantic-similarity floor with its quality cap).

    Rules apply only when len(t) > 3 to avoid mangling short words like
    "is", "as", "us", "bus":
      • -ies → -y           berries → berry, strawberries → strawberry
      • -oes → -o           tomatoes → tomato, potatoes → potato
      • -ses → -s           losses → loss
      • -s (not -ss / -us)  eggs → egg, beans → bean, legumes → legume
    Words ending in -ss / -us / -is keep their final letters (avoids
    citrus → citru, glass → glas, analysis → analysi).
    """
    if len(t) <= 3:
        return t
    if t.endswith("ies"):
        return t[:-3] + "y"
    if t.endswith("oes"):
        return t[:-2]
    if t.endswith("ses") and len(t) > 4:
        return t[:-2]
    if t.endswith("s") and not t.endswith(("ss", "us", "is", "os")):
        return t[:-1]
    return t


def _tokens(s: str) -> set[str]:
    if not s:
        return set()
    # Apply synonym normalisation BEFORE tokenisation so multi-word
    # phrases (e.g. "sesame paste" → "tahini") collapse cleanly. After
    # this, "tahini, sesame paste" tokenises to {"tahini"} (because
    # "sesame paste" was rewritten to a duplicate "tahini") and matches
    # a pred of "tahini" at 1.0 through the token pipeline alone.
    s = _apply_synonyms(str(s).lower())
    toks = {_stem(t) for t in re.findall(r"[a-z0-9_]+", s) if len(t) > 1}
    # Drop generic descriptors that don't carry food identity
    return toks - _STOP if len(toks) > 1 else toks


def _sim_pair(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    # Identity-conflict guard: when both sides explicitly use a product-variant
    # modifier (full-fat/skim/whole, brown/white, sweetened/unsweetened, etc.)
    # and the modifiers don't overlap, this is a real different-SKU mismatch
    # — bail out at 0.0 so e.g. "milk, full-fat" ≠ "milk, skim".
    a_id = a & _IDENTITY_MODS
    b_id = b & _IDENTITY_MODS
    if a_id and b_id and not (a_id & b_id):
        return 0.0
    # Short-name coincidence guard. Triggers only when ALL of these hold:
    #   1. Both sides ≤2 tokens.
    #   2. Share exactly 1 token (real disagreement, not containment).
    #   3. Neither set is a subset of the other.
    #   4. The single shared token is a generic CATEGORY word
    #      (juice / oil / pie / sauce / …) rather than a substantive
    #      food kind (chicken / lettuce / rice / pasta).
    # This rejects "strawberry juice" vs "watermelon juice" (share
    # {juice}) and "olive oil" vs "vegetable oil" (share {oil}) while
    # letting "chicken breast" vs "chicken whole" through at honest F1
    # — same chicken, different cut.
    shared = a & b
    if (len(a) <= 2 and len(b) <= 2 and inter < 2
            and not (a <= b or b <= a)
            and (shared & _GENERIC_CATEGORY_TOKENS)):
        return 0.0
    # Plain F1 — no subset/containment bonus.
    # Earlier code returned 1.0 whenever a ⊆ b, but that gave full credit
    # to "chicken, breast" matching "chicken, breast, shawarma-spiced",
    # which the user flagged as too generous. Letting F1 stand on its own
    # produces honest similarity scores: 0.80 for that case, 0.67 for
    # {milk} vs {milk, full-fat}, 1.0 only when sets are actually equal.
    return (2.0 * inter) / (len(a) + len(b))


def text_similarity(pred: str | None, truth: str | None) -> float:
    """Containment-aware F1 — gives full credit when one name is a clean
    subset of the other (e.g. "dates" inside "dates (deglet nour)").

    Parentheticals (often regional/native variant names like "(gibna beida)"
    or "(zabadi)") are scored both as part of the outer name and as a
    standalone alternative; we take the best pairing across all four
    combinations.
    """
    if not pred or not truth:
        return 0.0
    p_outer, p_inner = _split_paren(pred)
    t_outer, t_inner = _split_paren(truth)
    sets = {
        "p_outer": _tokens(p_outer),
        "p_inner": _tokens(p_inner),
        "t_outer": _tokens(t_outer),
        "t_inner": _tokens(t_inner),
    }
    best = 0.0
    for pk in ("p_outer", "p_inner"):
        for tk in ("t_outer", "t_inner"):
            s = _sim_pair(sets[pk], sets[tk])
            if s > best:
                best = s

    # NOTE: previous versions had a semantic-embedding floor here
    # (all-MiniLM-L6-v2 fallback for synonym cases the token pipeline
    # missed). Removed entirely — the LLM judge handles synonyms much
    # better than a 23M-param embedding model, and users running the
    # rule-based path prefer pure-token honesty over a semantic model
    # that loads ~80MB at startup and conflates similar-sounding
    # different foods (chicken vs chickpea, salmon vs tuna). See
    # SYNONYMS dict above for the curated rewrite list that runs first.
    return best


# ----------- nutrient scoring -----------
def nutrient_score(p: float, t: float, abs_tol: float, rel_tol: float,
                   decay: float = DECAY_MULTIPLIER) -> float:
    allowed = max(abs_tol, rel_tol * abs(t))
    if allowed <= 0:
        return 1.0 if p == t else 0.0
    delta = abs(p - t)
    if delta <= allowed:
        return 1.0
    cutoff = (1.0 + decay) * allowed
    if delta >= cutoff:
        return 0.0
    return max(0.0, 1.0 - (delta - allowed) / (decay * allowed))


def nutrient_accuracy(pred: dict, truth: dict) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for k in NUTRIENT_KEYS:
        if k not in truth:
            continue
        cfg = NUTRIENT_TOL[k]
        try:
            t = float(truth[k])
        except (TypeError, ValueError):
            continue
        if k not in pred or pred[k] is None:
            out[k] = {"score": 0.0, "in_tol": False, "allowed": None,
                      "delta": None, "weight": cfg["weight"], "missing": True,
                      "truth": t, "pred": None}
            continue
        try:
            p = float(pred[k])
        except (TypeError, ValueError):
            out[k] = {"score": 0.0, "in_tol": False, "allowed": None,
                      "delta": None, "weight": cfg["weight"], "missing": True,
                      "truth": t, "pred": None}
            continue
        allowed = max(cfg["abs"], cfg["rel"] * abs(t))
        delta = abs(p - t)
        s = nutrient_score(p, t, cfg["abs"], cfg["rel"])
        out[k] = {"score": round(s, 4), "in_tol": delta <= allowed,
                  "allowed": round(allowed, 2), "delta": round(delta, 2),
                  "weight": cfg["weight"], "missing": False,
                  "truth": t, "pred": p}
    return out


def weighted_avg(per: dict[str, dict]) -> float:
    if not per:
        return 0.0
    tot_w = sum(d["weight"] for d in per.values())
    if tot_w <= 0:
        return 0.0
    return sum(d["score"] * d["weight"] for d in per.values()) / tot_w


# ----------- ingredient matching -----------
def _quantity_score(pq: float | None, tq: float | None) -> float | None:
    if pq is None or tq is None:
        return None
    return nutrient_score(pq, tq, WEIGHT_ABS_TOL, WEIGHT_REL_TOL)


def ingredient_match(pred_list: list[dict], truth_list: list[dict],
                     threshold: float = INGREDIENT_MATCH_THRESHOLD) -> dict:
    """Global-greedy bipartite match by name similarity.

    Builds the full pred×truth similarity matrix once, then iteratively
    picks the highest-similarity unused pair until no pair clears the
    threshold. This avoids the prior bug where truth-order-greedy
    paired the wrong items (e.g. truth "garlic, raw" claimed pred
    "olive oil" because "olive oil" was the highest-sim leftover at
    that point in the loop, leaving the actual truth "olive oil" with
    nothing to match).

    Returns a structured object with matches + unmatched + summary stats.
    """
    pred_list = pred_list or []
    truth_list = truth_list or []
    matches: list[dict] = []
    used_pred: set[int] = set()
    matched_truth: set[int] = set()

    # Build the full similarity matrix once.
    n_p, n_t = len(pred_list), len(truth_list)
    sim_matrix: list[list[float]] = [
        [text_similarity(pred_list[pi].get("name"), truth_list[ti].get("name"))
         for ti in range(n_t)]
        for pi in range(n_p)
    ]

    # Global-greedy: enumerate all candidate pairs, sort by similarity
    # descending, walk down the list and take the first unused pair each
    # time. Equivalent to a 1-step approximation of the Hungarian
    # assignment; for our well-separated similarity scores (token-equal
    # pairs at 1.0 dominate cross-ingredient noise around 0.4–0.5) this
    # produces the optimal pairing in practice.
    candidates = [(sim_matrix[pi][ti], pi, ti)
                  for pi in range(n_p) for ti in range(n_t)
                  if sim_matrix[pi][ti] >= threshold]
    candidates.sort(key=lambda x: -x[0])

    for sim, pi, ti in candidates:
        if pi in used_pred or ti in matched_truth:
            continue
        used_pred.add(pi)
        matched_truth.add(ti)
        p = pred_list[pi]
        t = truth_list[ti]
        qs = _quantity_score(p.get("quantity"), t.get("quantity"))
        matches.append({
            "truth_idx": ti, "pred_idx": pi,
            "truth_name": t.get("name"), "pred_name": p.get("name"),
            "truth_qty": t.get("quantity"), "pred_qty": p.get("quantity"),
            "unit": t.get("unit") or p.get("unit") or "g",
            "name_sim": round(sim, 3),
            "weight_score": (None if qs is None else round(qs, 3)),
        })

    # Sort matches back by truth order for display
    matches.sort(key=lambda m: m["truth_idx"])

    matched = len(matches)
    n_pred = len(pred_list)
    n_truth = len(truth_list)
    precision = matched / n_pred if n_pred else 0.0
    recall = matched / n_truth if n_truth else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    valid_w = [m["weight_score"] for m in matches if m["weight_score"] is not None]
    weight_acc = sum(valid_w) / len(valid_w) if valid_w else 0.0

    unmatched_truth = [
        {"truth_idx": i, "name": truth_list[i].get("name"),
         "qty": truth_list[i].get("quantity"), "unit": truth_list[i].get("unit") or "g"}
        for i in range(n_truth) if i not in matched_truth
    ]
    unmatched_pred = [
        {"pred_idx": i, "name": pred_list[i].get("name"),
         "qty": pred_list[i].get("quantity"), "unit": pred_list[i].get("unit") or "g"}
        for i in range(n_pred) if i not in used_pred
    ]

    return {
        "matches": matches,
        "unmatched_truth": unmatched_truth,
        "unmatched_pred": unmatched_pred,
        "n_pred": n_pred, "n_truth": n_truth, "matched": matched,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "weight_acc": round(weight_acc, 4),
        "judged_by": "rules",
    }


# ----------- health grade -----------
_GRADES = "ABCDEF"


def health_score(pred: str | None, truth: str | None) -> dict:
    """A-E grade match: equal=1.0, off-by-1=0.6, off-by-2=0.3, else=0.0.

    If either value isn't a recognised grade, fall back to exact-string match.
    """
    if not pred and not truth:
        return {"score": None, "delta": None, "pred": pred, "truth": truth}
    if not pred or not truth:
        return {"score": 0.0, "delta": None, "pred": pred, "truth": truth}
    pu, tu = str(pred).strip().upper()[:1], str(truth).strip().upper()[:1]
    if pu not in _GRADES or tu not in _GRADES:
        s = 1.0 if str(pred).strip() == str(truth).strip() else 0.0
        return {"score": s, "delta": None, "pred": pred, "truth": truth}
    diff = abs(_GRADES.index(pu) - _GRADES.index(tu))
    ladder = [1.0, 0.6, 0.3, 0.1, 0.0, 0.0]
    return {"score": ladder[min(diff, 5)], "delta": diff, "pred": pu, "truth": tu}


# ----------- row-level scoring -----------
def score_row(pred: dict, truth_row: dict, weights: dict | None = None,
              use_llm_judge: bool = False, judge_model: str | None = None,
              judge_api_key: str | None = None,
              judge_base_url: str | None = None) -> dict:
    """Returns full scoring detail used by both leaderboard and per-row UI.

    When `use_llm_judge=True`, the ingredient matching step uses an LLM
    judge (see backend.llm_judge) instead of the rule-based bipartite
    matcher. The scoring math (tolerance bands, weights, F1, weight_acc)
    is identical in both modes — only the matching decision changes.

    `judge_model` defaults to the LLM_JUDGE_MODEL env var (gpt-5-mini).
    `judge_api_key` overrides the env-var key for the chosen provider —
    used by the runner to pass through this run's own api_key when the
    user opts in to "judge with the same model".
    """
    w = {**OVERALL_WEIGHTS, **(weights or {})}

    pred_food = pred.get("food")
    truth_food = truth_row.get("food")

    # Macros
    nut_per = nutrient_accuracy(pred.get("nutrition") or {},
                                 truth_row.get("nutrition_truth") or {})
    macros_avg = weighted_avg(nut_per)

    # Ingredients — either rule-based or LLM-as-judge.
    # When the LLM judge is enabled, the SAME call also scores the food
    # name (food_score in the result), avoiding a second round-trip.
    pred_ings_in = pred.get("ingredients") or []
    truth_ings_in = truth_row.get("ingredients_truth") or []
    if use_llm_judge:
        from . import llm_judge
        ing = llm_judge.match(pred_ings_in, truth_ings_in,
                              threshold=INGREDIENT_MATCH_THRESHOLD,
                              model=judge_model,
                              api_key=judge_api_key,
                              base_url=judge_base_url,
                              pred_food=pred_food,
                              truth_food=truth_food)
    else:
        ing = ingredient_match(pred_ings_in, truth_ings_in)
    ing_f1 = ing["f1"]
    weight_acc = ing["weight_acc"]

    # Food-name similarity: prefer the LLM-judge's food_score when we ran
    # the judge AND it returned a number. Fall back to the rule-based
    # text_similarity() otherwise (judge off, or judge couldn't score the
    # name — e.g. one side missing the field).
    judge_food_score = ing.get("food_score") if use_llm_judge else None
    if judge_food_score is not None:
        name_sim = float(judge_food_score)
        name_judged_by = "llm"
    else:
        name_sim = text_similarity(pred_food, truth_food)
        name_judged_by = "rules"

    # Patch 1 — empty-list floor:
    # When the model returns no ingredients but the dish name matches well
    # enough (name_sim >= 0.5) AND the truth has only 1–2 ingredients (likely
    # implicit in the food name like "oatmeal with milk"), award soft credit
    # so we don't double-penalise a correctly-named dish for under-enumeration.
    truth_ings = truth_row.get("ingredients_truth") or []
    pred_ings  = pred.get("ingredients") or []
    implicit_floor = None
    if truth_ings and not pred_ings and len(truth_ings) <= 2 and name_sim >= 0.5:
        implicit_floor = min(name_sim * 0.6, 0.6)
        ing_f1 = max(ing_f1, implicit_floor)

    # Health grade
    health = health_score(pred.get("health_score"), truth_row.get("health_score_truth"))

    # Build overall, weighting only the dimensions we actually have ground truth for
    parts: list[tuple[str, float, float]] = []
    if truth_row.get("food"):
        parts.append(("name", name_sim, w["name"]))
    if nut_per:
        parts.append(("macros", macros_avg, w["macros"]))
    if truth_row.get("ingredients_truth"):
        parts.append(("ingredient_f1", ing_f1, w["ingredient_f1"]))
        # Patch 2 — decouple weight_acc from f1:
        # Only fold weight_acc into overall when there is at least one matched
        # ingredient. With zero matches, weight_acc=0 was double-counting the
        # same failure already captured by f1=0.
        if ing.get("matched", 0) > 0:
            parts.append(("weight_acc", weight_acc, w["weight_acc"]))
    if health["score"] is not None and truth_row.get("health_score_truth"):
        parts.append(("health", health["score"], w["health"]))

    if not parts:
        overall = 0.0
    else:
        wsum = sum(wi for _, _, wi in parts)
        overall = sum(s * wi for _, s, wi in parts) / wsum if wsum > 0 else 0.0

    return {
        # legacy compat
        "food_sim": name_sim,
        "desc_sim": 0.0,
        "nutrition_per": {k: v["score"] for k, v in nut_per.items()},
        # rich detail
        "name_sim": name_sim,
        "name_judged_by": name_judged_by,
        "name_reason": ing.get("food_reason") if name_judged_by == "llm" else "",
        "macros_avg": macros_avg,
        "nutrition_detail": nut_per,
        "ingredients": ing,
        "ingredient_f1": ing_f1,
        "weight_acc": weight_acc,
        "implicit_floor": implicit_floor,    # non-null when Patch 1 kicked in
        "health": health,
        "overall": overall,
        "weights_used": {k: v for k, _, v in [(p[0], p[1], p[2]) for p in parts]},
    }


def composite_score(accuracy: float, avg_latency_ms: float, total_cost_usd: float,
                    weights: dict | None = None) -> float:
    w = weights or {"accuracy": 0.70, "speed": 0.15, "cost": 0.15}
    # Patch 3 — softer latency curve via exponential decay:
    #   1.0 at 0s · ~85% at 10s · ~61% at 30s · ~37% at 60s · ~5% at 180s
    # The previous linear cap at 30s gave local Ollama runs (typically 60–200s)
    # zero speed credit, distorting comparisons against cloud providers.
    speed = math.exp(-max(0.0, avg_latency_ms) / 60000.0)
    cost = max(0.0, 1.0 - min(total_cost_usd, 1.0) / 1.0)
    return 100.0 * (w["accuracy"] * accuracy + w["speed"] * speed + w["cost"] * cost)
