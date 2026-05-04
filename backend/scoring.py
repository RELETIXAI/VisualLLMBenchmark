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
_STOP = {"the", "a", "an", "of", "with", "and", "or", "in", "to", "raw"}


def _tokens(s: str) -> set[str]:
    if not s:
        return set()
    toks = {t for t in re.findall(r"[a-z0-9]+", str(s).lower()) if len(t) > 1}
    # Drop generic descriptors that don't carry food identity
    return toks - _STOP if len(toks) > 1 else toks


def text_similarity(pred: str | None, truth: str | None) -> float:
    """Containment-aware F1 — gives full credit when one name is a clean
    subset of the other (e.g. "dates" inside "dates (deglet nour)")."""
    if not pred or not truth:
        return 0.0
    a, b = _tokens(pred), _tokens(truth)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    f1 = (2.0 * inter) / (len(a) + len(b))
    overlap = inter / min(len(a), len(b))   # 1.0 when one is subset of other
    return max(f1, overlap)


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
    """Greedy bipartite match by name similarity.

    Returns a structured object with matches + unmatched + summary stats.
    """
    pred_list = pred_list or []
    truth_list = truth_list or []
    matches: list[dict] = []
    used_pred: set[int] = set()
    matched_truth: set[int] = set()

    # Sort truth by descending name length (most specific first) for stable matching
    truth_order = sorted(range(len(truth_list)),
                         key=lambda i: -len(str(truth_list[i].get("name") or "")))

    for ti in truth_order:
        t = truth_list[ti]
        best_pi, best_sim = None, 0.0
        for pi, p in enumerate(pred_list):
            if pi in used_pred:
                continue
            sim = text_similarity(p.get("name"), t.get("name"))
            if sim > best_sim:
                best_sim, best_pi = sim, pi
        if best_pi is not None and best_sim >= threshold:
            used_pred.add(best_pi)
            matched_truth.add(ti)
            p = pred_list[best_pi]
            qs = _quantity_score(p.get("quantity"), t.get("quantity"))
            matches.append({
                "truth_idx": ti, "pred_idx": best_pi,
                "truth_name": t.get("name"), "pred_name": p.get("name"),
                "truth_qty": t.get("quantity"), "pred_qty": p.get("quantity"),
                "unit": t.get("unit") or p.get("unit") or "g",
                "name_sim": round(best_sim, 3),
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
def score_row(pred: dict, truth_row: dict, weights: dict | None = None) -> dict:
    """Returns full scoring detail used by both leaderboard and per-row UI."""
    w = {**OVERALL_WEIGHTS, **(weights or {})}

    name_sim = text_similarity(pred.get("food"), truth_row.get("food"))

    # Macros
    nut_per = nutrient_accuracy(pred.get("nutrition") or {},
                                 truth_row.get("nutrition_truth") or {})
    macros_avg = weighted_avg(nut_per)

    # Ingredients
    ing = ingredient_match(pred.get("ingredients") or [],
                           truth_row.get("ingredients_truth") or [])
    ing_f1 = ing["f1"]
    weight_acc = ing["weight_acc"]

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
