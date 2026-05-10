#!/usr/bin/env python3
"""Triage the full WILLMA training dataset before agent review.

Two outputs in one pass over all 35K rows:

1. **Mojibake auto-fix** (--apply-mojibake-fixes):
   Detects bytes-as-mac-roman corruption (`b√©chamel`, `d√∂ner`, etc.) in
   dish names, descriptions, and ingredient names. Uses ftfy. Applies as
   a correction in the DB with note='auto: mojibake fix'. Deterministic,
   no vision needed — these are pure encoding errors.

2. **Triage queue** (always):
   Categorizes every row into clean / suspect buckets and writes
   `data/sft/review_queue.json` so we can dispatch agents only at the
   suspect rows. Categories (a row may be in several):

   - dish_ingredient_mismatch — content noun in dish name doesn't appear
     in any ingredient (e.g. "oats with milk" but no oats ingredient)
   - macro_math_off — kcal differs from 4·protein + 4·carbs + 9·fat by
     more than 30%
   - gram_total_low / gram_total_high — sum of ingredient grams < 10g or
     > 2500g (most plates are 100–800g)
   - tiny_ingredient — any ingredient with quantity < 2g but unit is g
     (e.g. the 1g falafel issue)
   - duplicate_ingredient — same ingredient name twice in the list
   - missing_required — no dish name, no ingredients, or no health grade
   - dish_token_only_in_one_ingredient — soft signal, dish has multiple
     content nouns but only one matches an ingredient

Usage:
    python scripts/triage_train_dataset.py                  # report only
    python scripts/triage_train_dataset.py --apply-mojibake # also fix mojibake
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
sys.path.insert(0, str(REPO))

from backend import db  # noqa: E402

XLSX = REPO / "data" / "uploads" / "WILLMA Meal Scans Extract 20-4-2026.xlsx"
OUT = REPO / "data" / "sft" / "review_queue.json"
DATASET_ID = 6

# Words to ignore when checking dish-vs-ingredient overlap
STOPWORDS = {
    "with", "and", "or", "in", "on", "of", "the", "a", "an",
    "served", "topped", "garnished", "filled", "stuffed", "side",
    "fresh", "raw", "cooked", "boiled", "grilled", "fried", "baked",
    "roasted", "steamed", "mashed", "sliced", "chopped", "diced",
    "whole", "half", "small", "large", "medium",
    "style", "egyptian", "lebanese", "turkish", "arabic", "kuwaiti",
    "white", "brown", "red", "green", "yellow", "black",
    "plain", "regular", "light", "low", "fat", "skim", "skinless",
    "homemade", "ready", "to", "eat", "instant", "extra",
    "for", "as", "by", "kcal", "g",
}


def tokens(s: str) -> set[str]:
    return set(re.findall(r"[a-z]+", (s or "").lower())) - STOPWORDS


def detect_mojibake(s) -> bool:
    """Quick heuristic — true if string contains the typical
    UTF-8-as-MacRoman corruption marker `√` followed by a char."""
    if not s:
        return False
    return bool(re.search(r"√[©®°§¶•æ±]", str(s)))


def fix_mojibake(s):
    """Run ftfy if present. Returns (fixed_string, was_changed)."""
    if not s:
        return s, False
    try:
        import ftfy
    except ImportError:
        return s, False
    fixed = ftfy.fix_text(str(s))
    return fixed, fixed != str(s)


def parse_ingredients(raw):
    """ingredients column may be a JSON string or a list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return None


def macro_math_off(kcal, p, c, f, tol=0.30) -> bool:
    """Is kcal more than `tol` away from 4p + 4c + 9f?
    Also flags rows where any value is missing."""
    try:
        kcal = float(kcal); p = float(p); c = float(c); f = float(f)
    except (TypeError, ValueError):
        return False
    if kcal < 5:  # likely water — exempt
        return False
    expected = 4*p + 4*c + 9*f
    if expected < 5:
        return kcal > 50
    return abs(kcal - expected) / max(expected, 1.0) > tol


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-mojibake", action="store_true",
                    help="Write mojibake fixes as DB corrections")
    ap.add_argument("--dataset-id", type=int, default=DATASET_ID)
    args = ap.parse_args()

    print(f"[1/4] Reading WILLMA xlsx ({XLSX.name})")
    import pandas as pd
    df = pd.read_excel(XLSX)
    rows = df.to_dict("records")
    print(f"      → {len(rows)} rows")

    # Restrict to rows whose image is hydrated locally — others can't be
    # trained on regardless, no point triaging them.
    print(f"[2/4] Filtering to hydrated images")
    images_dir = REPO / "data" / "images" / str(args.dataset_id)
    hydrated_iids: set[str] = set()
    if images_dir.exists():
        for p in images_dir.iterdir():
            if p.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp"):
                hydrated_iids.add(p.stem)
    print(f"      → {len(hydrated_iids)} hydrated image_ids on disk")

    # Existing corrections — don't re-flag rows we already corrected
    existing_corr = {}
    for c in db.list_corrections(args.dataset_id):
        ref = c.get("image_id") or ""
        bare = ref.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        existing_corr[bare] = c
    print(f"      → {len(existing_corr)} existing corrections in DB")

    # Holdout — exclude eval rows from train triage
    holdout = set()
    try:
        lock = json.loads((REPO / "data" / "sft" / "HOLDOUT.lock").read_text())
        for ref in lock.get("image_refs", []):
            bare = ref.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            holdout.add(bare)
    except Exception:
        pass
    print(f"      → {len(holdout)} holdout image_ids (eval, excluded)")

    print(f"[3/4] Categorizing rows")
    categories = defaultdict(list)  # category -> list of row dicts
    mojibake_corrections = []       # (image_id, fixes) tuples
    n_clean = 0
    n_no_image = 0
    n_no_iid = 0
    n_holdout_skipped = 0
    n_already_corrected = 0
    n_unhydrated = 0

    for row in rows:
        iid_raw = row.get("image")
        iid = str(iid_raw).strip() if iid_raw is not None else ""
        if not iid or iid.lower() == "nan":
            n_no_iid += 1
            continue
        if iid in holdout:
            n_holdout_skipped += 1
            continue
        if iid not in hydrated_iids:
            n_unhydrated += 1
            continue
        if iid in existing_corr:
            n_already_corrected += 1
            continue

        dish = str(row.get("name") or "").strip()
        grade = str(row.get("healthScore") or "").strip().upper()
        kcal = row.get("calories")
        protein = row.get("protein")
        carbs = row.get("carbohydrates")
        fat = row.get("fat")
        ingr_raw = row.get("ingredients")
        ingr = parse_ingredients(ingr_raw)

        flags = []

        # Required fields
        if not dish or grade not in {"A", "B", "C", "D", "E"} or not isinstance(ingr, list) or not ingr:
            flags.append("missing_required")

        # Mojibake — examine dish, ingredient names, and description
        moji_fixes = {}
        if detect_mojibake(dish):
            fixed, changed = fix_mojibake(dish)
            if changed:
                moji_fixes["dish"] = (dish, fixed)
        if isinstance(ingr, list):
            for i, item in enumerate(ingr):
                if not isinstance(item, dict):
                    continue
                name = item.get("name", "")
                if detect_mojibake(name):
                    fixed, changed = fix_mojibake(name)
                    if changed:
                        moji_fixes.setdefault("ingredients", []).append((i, name, fixed))
        if moji_fixes:
            flags.append("mojibake")
            mojibake_corrections.append({"iid": iid, "fixes": moji_fixes,
                                          "original_row": row})

        # Macro math sanity
        if macro_math_off(kcal, protein, carbs, fat):
            flags.append("macro_math_off")

        # Gram totals
        if isinstance(ingr, list) and ingr:
            grams_total = 0.0
            for item in ingr:
                if not isinstance(item, dict):
                    continue
                qty = item.get("quantity")
                unit = str(item.get("unit") or "").lower()
                try:
                    q = float(qty) if qty is not None else 0
                except (TypeError, ValueError):
                    q = 0
                if unit == "kg":
                    q *= 1000
                elif unit == "mg":
                    q /= 1000
                grams_total += q
            if grams_total < 10:
                flags.append("gram_total_low")
            elif grams_total > 2500:
                flags.append("gram_total_high")

            # Tiny ingredient (1g or less when unit=g, but sum > 50g)
            for item in ingr:
                if not isinstance(item, dict):
                    continue
                try:
                    q = float(item.get("quantity") or 0)
                except (TypeError, ValueError):
                    q = 0
                unit = str(item.get("unit") or "").lower()
                if unit == "g" and 0 < q <= 1.5 and grams_total > 50:
                    flags.append("tiny_ingredient")
                    break

            # Duplicate ingredient names (case-insensitive, basic strip)
            names = [str(it.get("name") or "").strip().lower() for it in ingr if isinstance(it, dict)]
            if len(names) != len(set(n for n in names if n)):
                flags.append("duplicate_ingredient")

            # Dish→ingredient mismatch: any content token in dish (≥4 chars)
            # that doesn't appear as a SUBSTRING in any ingredient name?
            # Substring-based to absorb common variants like
            # "chicken" ⊂ "chicken, breast, grilled".
            dish_t = {t for t in tokens(dish) if len(t) >= 4}
            ingr_text_lower = " ".join(
                str(it.get("name") or "").lower()
                for it in ingr if isinstance(it, dict)
            )
            unmatched = {t for t in dish_t if t not in ingr_text_lower}
            # Only flag if MOST dish tokens are missing — if at least one
            # major noun matches, the row is probably fine.
            if dish_t and unmatched and len(unmatched) / len(dish_t) >= 0.5:
                flags.append("dish_ingredient_mismatch")

        if flags:
            for cat in flags:
                categories[cat].append({
                    "image_id": iid,
                    "image_ref": f"{args.dataset_id}/{iid}.jpg",
                    "image_path": f"/Users/samueltoma/Documents/Reletix/LLMbenchmark/data/images/{args.dataset_id}/{iid}.jpg",
                    "dish": dish,
                    "grade": grade,
                    "macros": {"kcal": kcal, "protein": protein,
                                "carbs": carbs, "fat": fat},
                    "flags": flags,
                })
        else:
            n_clean += 1

    print(f"      → categorized {len(rows)} rows")
    print()
    print(f"  Excluded from triage:")
    print(f"    - no_image_id     : {n_no_iid}")
    print(f"    - holdout (eval)  : {n_holdout_skipped}")
    print(f"    - not hydrated    : {n_unhydrated}")
    print(f"    - already corrected: {n_already_corrected}")
    print()
    print(f"  Triage results (rows can be in multiple categories):")
    print(f"    - clean (no flags)        : {n_clean}")
    for cat, items in sorted(categories.items(), key=lambda x: -len(x[1])):
        print(f"    - {cat:<26}: {len(items)}")
    # Distinct flagged rows
    flagged_iids = {x["image_id"] for items in categories.values() for x in items}
    print(f"    distinct flagged rows     : {len(flagged_iids)}")

    # ── Apply mojibake fixes ───────────────────────────────────────────
    if args.apply_mojibake and mojibake_corrections:
        print()
        print(f"[4/4] Applying {len(mojibake_corrections)} mojibake corrections to DB")

        def _to_correction_truth(row, fixes):
            """Translate a WILLMA xlsx row + mojibake fixes into the
            corrections-table truth schema (food/nutrition/ingredients/
            health_score). Apply fixes to dish & ingredient names."""
            ingr = parse_ingredients(row.get("ingredients")) or []
            # Apply ingredient-name fixes
            for (i, _orig, fixed_name) in fixes.get("ingredients", []):
                if 0 <= i < len(ingr) and isinstance(ingr[i], dict):
                    ingr[i]["name"] = fixed_name
            return {
                "food": fixes.get("dish", (row.get("name"), row.get("name")))[1],
                "description": None,
                "nutrition": {
                    "calories":  float(row.get("calories")  or 0),
                    "protein_g": float(row.get("protein")   or 0),
                    "carbs_g":   float(row.get("carbohydrates") or 0),
                    "fat_g":     float(row.get("fat")        or 0),
                },
                "ingredients": [
                    {
                        "name":     it.get("name", ""),
                        "quantity": it.get("quantity", 0),
                        "unit":     it.get("unit", "g"),
                    } for it in ingr if isinstance(it, dict)
                ],
                "health_score": str(row.get("healthScore") or "").strip().upper(),
            }

        n_written = 0
        for mc in mojibake_corrections:
            iid = mc["iid"]
            fixes = mc["fixes"]
            row = mc["original_row"]
            truth = _to_correction_truth(row, fixes)
            # Skip if dish or grade ended up missing — the row is
            # already broken and shouldn't get a partial correction
            if not truth["food"] or truth["health_score"] not in "ABCDE":
                continue
            try:
                db.upsert_correction(
                    dataset_id=args.dataset_id,
                    image_id=f"{args.dataset_id}/{iid}.jpg",
                    truth_json=json.dumps(truth),
                    note="auto: mojibake fix (utf-8/mac-roman corruption)",
                )
                n_written += 1
            except Exception as e:
                print(f"    ⚠ failed for {iid}: {e}")
        print(f"      ✓ wrote {n_written}/{len(mojibake_corrections)} mojibake corrections")
    elif args.apply_mojibake:
        print()
        print(f"[4/4] No mojibake to fix")

    # ── Write triage queue ─────────────────────────────────────────────
    queue = {
        "summary": {
            "total_rows": len(rows),
            "clean": n_clean,
            "no_image_id": n_no_iid,
            "holdout_skipped": n_holdout_skipped,
            "unhydrated": n_unhydrated,
            "already_corrected": n_already_corrected,
            "distinct_flagged": len(flagged_iids),
            "by_category": {cat: len(items) for cat, items in categories.items()},
        },
        "categories": {cat: [
            {k: v for k, v in item.items() if k != "flags"}
            for item in items
        ] for cat, items in categories.items()},
        "all_flagged": [
            {"image_id": iid,
             "image_path": f"/Users/samueltoma/Documents/Reletix/LLMbenchmark/data/images/{args.dataset_id}/{iid}.jpg",
             "flags": list({f for items in categories.values() for x in items if x["image_id"] == iid for f in x["flags"]})}
            for iid in sorted(flagged_iids)
        ],
    }
    OUT.write_text(json.dumps(queue, indent=2, ensure_ascii=False))
    print(f"      ✓ wrote queue: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
