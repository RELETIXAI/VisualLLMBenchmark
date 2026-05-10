#!/usr/bin/env python3
"""Aggregate wave-2 agent results and apply 667 fixes + 308 skips.

Reads /tmp/llmbench_results/wave2/*.json (84 agent verdict files), merges
them, and writes corrections to the DB via db.upsert_correction.

Each verdict has:
  - image_id (bare WILLMA id, e.g. '1773851234567-...uuid')
  - status: ok | fix | skip
  - reasoning: short explanation
  - fix: { dish?, grade?, macros?, ingredients? } when status=fix

For status=skip, we write a correction with note='skip — <reason>' so the
export script's skip-list catches it. For status=fix, we partially override
WILLMA truth with the agent's proposed corrections.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
sys.path.insert(0, str(REPO))

from backend import db  # noqa: E402

DATASET_ID = 6
RESULTS_DIR = Path("/tmp/llmbench_results/wave2")


# ── Load WILLMA xlsx once for partial-fix base truths ───────────────────
_willma_cache: dict | None = None


def get_willma_row(image_id_bare: str) -> dict | None:
    global _willma_cache
    if _willma_cache is None:
        import pandas as pd
        df = pd.read_excel(REPO / "data" / "uploads" /
                           "WILLMA Meal Scans Extract 20-4-2026.xlsx")
        _willma_cache = {
            row["image"]: row.to_dict()
            for _, row in df.iterrows()
            if isinstance(row.get("image"), str)
        }
    return _willma_cache.get(image_id_bare)


def parse_ingr(raw):
    if isinstance(raw, list): return raw
    if isinstance(raw, str):
        try: return json.loads(raw)
        except: return None
    return None


def truth_from_willma(image_id_bare: str, *, override_food=None,
                      override_grade=None, override_macros=None,
                      override_ingredients=None) -> dict:
    """Build a corrections-schema truth from the WILLMA row, with optional
    field overrides. Used when the agent only proposes a partial fix."""
    row = get_willma_row(image_id_bare) or {}

    food = override_food if override_food else (row.get("name") or "")
    grade = override_grade if override_grade else (
        str(row.get("healthScore") or "").strip().upper()
    )

    if override_macros:
        nutrition = {
            "calories":  float(override_macros[0]) if override_macros[0] is not None else 0,
            "protein_g": float(override_macros[1]) if override_macros[1] is not None else 0,
            "carbs_g":   float(override_macros[2]) if override_macros[2] is not None else 0,
            "fat_g":     float(override_macros[3]) if override_macros[3] is not None else 0,
        }
    else:
        try: kcal = float(row.get("calories")  or 0)
        except: kcal = 0
        try: p = float(row.get("protein")   or 0)
        except: p = 0
        try: c = float(row.get("carbohydrates") or 0)
        except: c = 0
        try: f = float(row.get("fat")        or 0)
        except: f = 0
        nutrition = {"calories": kcal, "protein_g": p, "carbs_g": c, "fat_g": f}

    if override_ingredients is not None:
        ingredients = []
        for it in override_ingredients:
            if isinstance(it, dict):
                name = it.get("name", "")
                grams = it.get("grams") or it.get("quantity") or 0
                try: grams = float(grams)
                except: grams = 0
                ingredients.append({
                    "name": name,
                    "quantity": grams,
                    "unit": "g",
                })
    else:
        raw_ingr = parse_ingr(row.get("ingredients")) or []
        ingredients = [
            {"name": it.get("name", ""), "quantity": it.get("quantity", 0),
             "unit": it.get("unit", "g")}
            for it in raw_ingr if isinstance(it, dict)
        ]

    return {
        "food": food,
        "description": None,
        "nutrition": nutrition,
        "ingredients": ingredients,
        "health_score": grade,
    }


def main():
    # Load all 84 result JSONs
    files = sorted(RESULTS_DIR.glob("*.json"))
    print(f"Reading {len(files)} agent result files from {RESULTS_DIR}")

    all_verdicts = []
    for fp in files:
        try:
            data = json.loads(fp.read_text())
            for v in data.get("verdicts", []):
                v["__agent_id"] = data.get("agent_id", fp.stem)
                v["__category"] = data.get("category", "?")
                all_verdicts.append(v)
        except Exception as e:
            print(f"  ⚠ failed to read {fp.name}: {e}")

    print(f"  total verdicts: {len(all_verdicts)}")
    print()

    # Apply each fix/skip; group OK rows for reporting only
    n_fix = n_skip = n_ok = n_err = n_dup = 0
    seen_iids = set()
    for v in all_verdicts:
        iid = v.get("image_id", "").strip()
        status = v.get("status", "ok")
        if not iid:
            continue
        if iid in seen_iids:
            n_dup += 1
            continue  # skip dups within wave-2 (rare)
        seen_iids.add(iid)

        ref = f"6/{iid}.jpg"
        reasoning = v.get("reasoning", "")[:160]
        agent_id = v.get("__agent_id", "?")
        cat = v.get("__category", "?")

        try:
            if status == "ok":
                n_ok += 1
            elif status == "skip":
                tj = truth_from_willma(iid)
                db.upsert_correction(
                    dataset_id=DATASET_ID,
                    image_id=ref,
                    truth_json=json.dumps(tj),
                    note=f"skip — wave2 {agent_id} {cat}: {reasoning}",
                )
                n_skip += 1
            elif status == "fix":
                fix = v.get("fix") or {}
                tj = truth_from_willma(
                    iid,
                    override_food=fix.get("dish"),
                    override_grade=fix.get("grade"),
                    override_macros=fix.get("macros"),
                    override_ingredients=fix.get("ingredients"),
                )
                # Skip writing fix if truth ended up missing food or grade —
                # the row will be excluded by build_assistant_text anyway
                if not tj["food"] or tj["health_score"] not in "ABCDE":
                    db.upsert_correction(
                        dataset_id=DATASET_ID,
                        image_id=ref,
                        truth_json=json.dumps(tj),
                        note=f"skip — wave2 {agent_id} {cat}: fix lacked food/grade ({reasoning})",
                    )
                    n_skip += 1
                else:
                    db.upsert_correction(
                        dataset_id=DATASET_ID,
                        image_id=ref,
                        truth_json=json.dumps(tj),
                        note=f"wave2 {agent_id} {cat}: {reasoning}",
                    )
                    n_fix += 1
        except Exception as e:
            n_err += 1
            print(f"  ⚠ {iid}: {e}")

    print(f"=== Wave-2 application complete ===")
    print(f"  fixes written : {n_fix}")
    print(f"  skips written : {n_skip}")
    print(f"  ok (no change): {n_ok}")
    print(f"  duplicates    : {n_dup}")
    print(f"  errors        : {n_err}")
    print()

    con = db._conn()
    n_active = con.execute(
        "SELECT COUNT(*) FROM corrections WHERE dataset_id=?",
        (DATASET_ID,)).fetchone()[0]
    n_skip_total = con.execute(
        "SELECT COUNT(*) FROM corrections WHERE dataset_id=? AND note LIKE 'skip%'",
        (DATASET_ID,)).fetchone()[0]
    v_now = db.current_dataset_version(DATASET_ID)
    print(f"DB state: {n_active} active corrections "
          f"({n_skip_total} skip-marked) · dataset @ v{v_now}")


if __name__ == "__main__":
    sys.exit(main() or 0)
