#!/usr/bin/env python3
"""Apply the wave-1 train review verdicts (24-batch sample of 190 rows).

41 fixes + 32 skips agreed by 24 reviewer agents that each Read'd the
image with vision and proposed corrections. The verdicts are hard-coded
below; image_ids are looked up from data/sft/review_batches.json by
(category, position-in-batch).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
sys.path.insert(0, str(REPO))

from backend import db  # noqa: E402

DATASET_ID = 6


def truth(food, grade, kcal, p, c, f, ingredients):
    return {
        "food": food,
        "description": None,
        "nutrition": {
            "calories":  float(kcal),
            "protein_g": float(p),
            "carbs_g":   float(c),
            "fat_g":     float(f),
        },
        "ingredients": [
            {"name": name, "quantity": qty, "unit": "g"}
            for name, qty in ingredients
        ],
        "health_score": grade,
    }


# ── Look up image_ids by (category, batch_idx, row_idx_in_batch) ────────
SAMPLES = json.load(open(REPO / "data" / "sft" / "review_batches.json"))


def iid(category: str, position: int) -> str:
    """Returns full image_ref form '6/<id>.jpg' suitable for corrections table."""
    rows = SAMPLES[category]
    if position - 1 >= len(rows):
        raise IndexError(f"position {position} out of bounds for {category} (len={len(rows)})")
    row = rows[position - 1]
    image_id = row["image_id"]
    return f"6/{image_id}.jpg"


# ── Helpers to load existing truth for partial-fix rows (e.g. only macros) ──
def get_willma_row(image_id_bare: str) -> dict | None:
    """Read the WILLMA xlsx row for a given bare image_id (no prefix/extension)."""
    import pandas as pd
    if not hasattr(get_willma_row, "_df"):
        get_willma_row._df = pd.read_excel(REPO / "data" / "uploads" /
                                            "WILLMA Meal Scans Extract 20-4-2026.xlsx")
    df = get_willma_row._df
    matched = df[df["image"] == image_id_bare]
    if len(matched) == 0:
        return None
    return matched.iloc[0].to_dict()


def parse_ingr(raw):
    if isinstance(raw, list): return raw
    if isinstance(raw, str):
        try: return json.loads(raw)
        except: return None
    return None


def truth_from_willma(image_ref: str, *, override_food=None, override_grade=None,
                      override_macros=None, override_ingredients=None) -> dict:
    """Build a corrections-schema truth from the WILLMA row, with optional
    field overrides. Used when the agent only proposes a partial fix."""
    bare = image_ref.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    row = get_willma_row(bare) or {}

    food = override_food if override_food is not None else (row.get("name") or "")
    grade = override_grade if override_grade is not None else (
        str(row.get("healthScore") or "").strip().upper()
    )

    if override_macros:
        nutrition = {
            "calories":  float(override_macros[0]),
            "protein_g": float(override_macros[1]),
            "carbs_g":   float(override_macros[2]),
            "fat_g":     float(override_macros[3]),
        }
    else:
        nutrition = {
            "calories":  float(row.get("calories")  or 0),
            "protein_g": float(row.get("protein")   or 0),
            "carbs_g":   float(row.get("carbohydrates") or 0),
            "fat_g":     float(row.get("fat")        or 0),
        }

    if override_ingredients is not None:
        ingredients = [
            {"name": name, "quantity": float(qty), "unit": "g"}
            for name, qty in override_ingredients
        ]
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


# ─────────────────────────────────────────────────────────────────────────
# Verdicts: 41 fixes + 32 skips from the 24-agent wave-1 review
# ─────────────────────────────────────────────────────────────────────────
# Format:
#   ("category", row_position_in_batch, "fix"|"skip", payload, note)
# For "fix", payload is a dict with optional keys: dish, grade, macros, ingredients
# For "skip", payload is None and note explains why
VERDICTS = [
    # ── BATCH 01 (dish_ingredient_mismatch) ─────────────────────────────
    ("dish_ingredient_mismatch", 5, "skip", None,
     "wave1 b01 r5: nutrition-facts label of Katilo cheese package, not a meal"),

    # ── BATCH 03 (dish_ingredient_mismatch) ─────────────────────────────
    ("dish_ingredient_mismatch", 21, "skip", None,
     "wave1 b03 r5: 'Heart gum' chewing-gum package label, not a meal"),

    # ── BATCH 05 (macro_math_off, rows 1-8) ─────────────────────────────
    ("macro_math_off", 1, "skip", None, "wave1 b05 r1: chip-bag nutrition label"),
    ("macro_math_off", 2, "skip", None, "wave1 b05 r2: cocoa-powder retail box"),
    ("macro_math_off", 3, "fix", {"macros": (108, 2, 27.6, 0.3)},
     "wave1 b05 r3: oranges — carbs were ~2x realistic"),
    ("macro_math_off", 5, "fix", {"macros": (247, 9, 50, 1.5)},
     "wave1 b05 r5: baladi flatbread — carbs were 144g (impossible >mass)"),
    ("macro_math_off", 6, "fix", {
        "macros": (116, 3.6, 23.4, 1.4),
        "ingredients": [("popcorn, air-popped", 30)]},
     "wave1 b05 r6: popcorn — carbs were ~2x realistic, gram total slightly off"),
    ("macro_math_off", 7, "fix", {"macros": (734, 22.5, 128, 12)},
     "wave1 b05 r7: stir-fried wheat noodles — carbs were ~half realistic"),
    ("macro_math_off", 8, "skip", None, "wave1 b05 r8: low-carb-toast bread bag"),

    # ── BATCH 06 (macro_math_off, rows 9-16) ────────────────────────────
    ("macro_math_off", 10, "fix", {"macros": (51, 1, 5, 4)},
     "wave1 b06 r2: chocolate hazelnut spread — 90g carbs in 10g portion impossible"),
    ("macro_math_off", 12, "fix", {"macros": (435, 29.2, 76, 1.6)},
     "wave1 b06 r4: ful medames — carbs were ~half realistic"),
    ("macro_math_off", 13, "fix", {"macros": (494, 9, 107, 1)},
     "wave1 b06 r5: white rice — carbs were ~half realistic"),
    ("macro_math_off", 14, "fix", {"macros": (115, 3.5, 22, 1.5)},
     "wave1 b06 r6: Egyptian bread rolls — 55g carbs in 43g impossible"),
    ("macro_math_off", 15, "fix", {
        "dish": "assorted muffins (chocolate and vanilla)",
        "macros": (230, 3, 30, 11),
        "ingredients": [("muffin, chocolate", 40), ("muffin, vanilla", 25)]},
     "wave1 b06 r7: photo shows 3 pastries (incl. vanilla), truth had only 1×20g chocolate"),
    ("macro_math_off", 16, "fix", {"macros": (89, 2, 20.7, 0.1)},
     "wave1 b06 r8: sweet potato — carbs were ~2x realistic"),

    # ── BATCH 07 (macro_math_off, rows 17-24) ───────────────────────────
    ("macro_math_off", 17, "skip", None, "wave1 b07 r1: chia-bread package screenshot"),
    ("macro_math_off", 18, "fix", {"macros": (165, 11, 2, 12)},
     "wave1 b07 r2: scrambled eggs — fat missing & kcal too low"),
    ("macro_math_off", 20, "fix", {"macros": (10, 0, 2.5, 0)},
     "wave1 b07 r4: black tea — carbs were ~2.4x realistic"),
    ("macro_math_off", 21, "fix", {"macros": (480, 18, 27, 35)},
     "wave1 b07 r5: halawa+cheese — 93g carbs impossible (cheese has none)"),
    ("macro_math_off", 22, "fix", {
        "macros": (145, 1, 12, 10),
        "ingredients": [("potatoes, fried, french fries", 30),
                         ("mayonnaise-based dipping sauce", 15)]},
     "wave1 b07 r6: French fries with missing mayo + carb error"),
    ("macro_math_off", 24, "skip", None, "wave1 b07 r8: hummus-bread package"),

    # ── BATCH 08 (duplicate_ingredient, rows 1-8) ───────────────────────
    ("duplicate_ingredient", 1, "fix", {"ingredients": [
        ("fish, whole, grilled", 180), ("rice, white, cooked", 130),
        ("tomato, cooked", 30), ("herbs and spices, mixed", 5)]},
     "wave1 b08 r1: each ingredient was duplicated — dedupe"),
    ("duplicate_ingredient", 2, "fix", {"ingredients": [
        ("Chicken, Breast, Breaded, Fried", 220),
        ("Qatayef/atayef Pancake, Plain (unfilled)", 180)]},
     "wave1 b08 r2: dup list — dedupe"),
    ("duplicate_ingredient", 3, "fix", {"ingredients": [
        ("egg, chicken, whole, cooked, omelette", 110),
        ("potato, fries, homemade, baked", 90),
        ("spices, mixed (black pepper, paprika, salt)", 2)]},
     "wave1 b08 r3: dup list — dedupe"),
    ("duplicate_ingredient", 4, "fix", {"ingredients": [
        ("white bread, crumbed and fried", 80),
        ("egg, scrambled, cooked", 40),
        ("mixed vegetables, cooked (onion, carrot, herbs)", 30),
        ("vegetable oil, absorbed (frying)", 14)]},
     "wave1 b08 r4: dup list — dedupe"),
    ("duplicate_ingredient", 8, "fix", {
        "dish": "grilled chicken with rice, tomato, and olive",
        "grade": "B", "macros": (360, 32, 38, 8),
        "ingredients": [("chicken breast, grilled", 110),
                         ("rice, white, cooked", 130),
                         ("tomato, raw", 30),
                         ("olive, green, pickled", 5)]},
     "wave1 b08 r8: row had null ingredients — agent built from photo"),

    # ── BATCH 09 (duplicate_ingredient, rows 9-16) ──────────────────────
    ("duplicate_ingredient", 9,  "skip", None, "wave1 b09 r1: empty mug, no food"),
    ("duplicate_ingredient", 10, "skip", None, "wave1 b09 r2: Starbucks app screenshot"),
    ("duplicate_ingredient", 11, "fix", {"ingredients": [
        ("oatmeal, cooked, plain", 100), ("cinnamon, ground", 4),
        ("strawberries, raw", 60), ("yogurt, plain, low fat", 50),
        ("blueberries, raw", 20)]},
     "wave1 b09 r3: cinnamon listed twice (3g+1g) — merge to 4g"),
    ("duplicate_ingredient", 12, "fix", {"ingredients": [
        ("rice, white, cooked", 120), ("Danone HiPRO protein yogurt drink, strawberry", 300),
        ("canned tuna, in sunflower oil, drained", 120),
        ("fried onions, vegetable oil", 25), ("chickpeas, cooked", 30),
        ("vermicelli, cooked", 20), ("lentils, brown, cooked", 60),
        ("macaroni, cooked", 70)]},
     "wave1 b09 r4: 8-item list duplicated to 16 — dedupe"),
    ("duplicate_ingredient", 13, "skip", None, "wave1 b09 r5: McDonald's marketing render"),
    ("duplicate_ingredient", 16, "skip", None, "wave1 b09 r8: nutrition-facts panel"),

    # ── BATCH 10 (duplicate_ingredient, rows 17-24) ─────────────────────
    ("duplicate_ingredient", 17, "fix", {"ingredients": [
        ("kunafa pastry, plain, baked", 60), ("chocolate, milk, plain", 20)]},
     "wave1 b10 r1: only one kunafa nest visible, dups removed"),
    ("duplicate_ingredient", 18, "fix", {"ingredients": [
        ("Yogurt, Plain, Whole Milk", 120), ("Cashew Nuts, Raw", 30),
        ("Walnuts, Raw", 10), ("Dates, Dried", 10), ("Banana, Raw", 118),
        ("Whey Protein Powder", 30)]},
     "wave1 b10 r2: cashews duplicated — dedupe"),
    ("duplicate_ingredient", 19, "fix", {"ingredients": [
        ("Lentil Soup", 160), ("Cucumber, Raw", 40),
        ("Egg, Whole, Hard-Boiled", 50)]},
     "wave1 b10 r3: dup egg entries merged"),
    ("duplicate_ingredient", 20, "skip", None, "wave1 b10 r4: delivery-app screenshot"),
    ("duplicate_ingredient", 21, "fix", {"ingredients": [
        ("multigrain cereal bar with dark chocolate", 50)]},
     "wave1 b10 r5: same bar listed twice — dedupe"),
    ("duplicate_ingredient", 22, "fix", {"ingredients": [
        ("Green Olives, Pitted", 35), ("White Cheese, Soft (labneh Style)", 40),
        ("Halloumi Cheese, Air Fried (no Oil)", 55),
        ("Boiled Egg, Whole", 100), ("Carrot, Raw", 120),
        ("Cucumber, Raw", 150), ("Protein Bread (12g Protein Per 2 Slices)", 60),
        ("Black Tea, Brewed, Sweetened", 200)]},
     "wave1 b10 r6: 8-item list triplicated — dedupe"),
    ("duplicate_ingredient", 23, "fix", {"ingredients": [
        ("egg, omelet/plain, pan-cooked", 110),
        ("potato, boiled, no skin", 180), ("bread, white, baguette", 90),
        ("greek yogurt, plain, 2% fat", 150),
        ("soft drink, regular, cola", 330)]},
     "wave1 b10 r7: cola duplicated — dedupe"),
    ("duplicate_ingredient", 24, "fix", {"ingredients": [
        ("beef, ground, lean, cooked (meatballs)", 90),
        ("rice, white, cooked", 100), ("onion, cooked", 40),
        ("green bell pepper, cooked", 25), ("corn, sweet, cooked", 50),
        ("lettuce, iceberg, raw", 25), ("cucumber, raw", 30),
        ("tomato, raw", 20), ("vegetable oil (used in cooking meatballs/sauce)", 8)]},
     "wave1 b10 r8: 9-item list repeated 7x — dedupe"),

    # ── BATCH 11 (tiny_ingredient) ──────────────────────────────────────
    ("tiny_ingredient", 3, "skip", None, "wave1 b11 r3: empty/near-empty cup"),

    # ── BATCH 13 (gram_total_low, rows 1-8) ─────────────────────────────
    ("gram_total_low", 1, "skip", None,
     "wave1 b13 r1: supplement-facts label (not food)"),
    ("gram_total_low", 3, "fix", {
        "dish": "assorted topped cracker flatbreads (strawberry, blueberry-mint, "
                "kiwi, tomato-basil, brie-pesto, banana-honey, beet hummus-avocado, radish)",
        "grade": "D", "macros": (720, 18, 95, 30),
        "ingredients": [
            ("square cracker flatbread", 240), ("cream cheese spread", 60),
            ("strawberries, fresh", 30), ("blueberries, fresh", 20),
            ("fresh mint leaves", 2), ("kiwi, fresh sliced", 40),
            ("cherry tomatoes, mixed", 40), ("fresh basil leaves", 3),
            ("brie cheese", 25), ("pesto", 15), ("banana, sliced", 40),
            ("honey", 10), ("beet hummus", 40), ("avocado, sliced", 30),
            ("radish, sliced", 25), ("microgreens", 5)]},
     "wave1 b13 r3: photo was crackers, truth claimed electrolyte drink — totally wrong dish"),
    ("gram_total_low", 7, "fix", {
        "dish": "kettle-cooked chips with sea salt, chicken tandoori wrap with raita sauce, fresh orange juice",
        "grade": "C", "macros": (980, 35, 130, 38),
        "ingredients": [
            ("kettle-cooked potato chips, sea salt", 67),
            ("flour tortilla wrap", 70),
            ("chicken, tandoori cooked", 90),
            ("raita sauce (yogurt-based)", 30),
            ("lettuce, shredded", 15),
            ("tomato, diced", 20),
            ("fresh orange juice", 275)]},
     "wave1 b13 r7: ingredients were 'None: NoneNone' placeholder — photo was Breadfast bundle"),

    # ── BATCH 14 (gram_total_low, rows 9-16) ────────────────────────────
    ("gram_total_low", 13, "fix", {
        "macros": (21, 0.8, 0.7, 1.8),
        "ingredients": [("almonds, raw", 4)]},
     "wave1 b14 r5: only 3 almonds visible (~4g), truth said 8g + ~2x macros"),
    ("gram_total_low", 14, "fix", {
        "grade": "B", "macros": (320, 22, 45, 6),
        "ingredients": [
            ("Lamb, Lean, Cooked, Diced", 30), ("Lentils, Cooked", 80),
            ("Chickpeas, Cooked", 40), ("Tomatoes, Crushed", 30),
            ("Vermicelli Noodles, Cooked", 60),
            ("Onion, Celery, Herbs, Spices", 20)]},
     "wave1 b14 r6: full bowl labelled with 1g per ingredient (6g total) but 1008 kcal"),

    # ── BATCH 15 (gram_total_high, rows 1-8) ────────────────────────────
    ("gram_total_high", 3, "skip", None, "wave1 b15 r3: family bake/holiday session, not a meal"),
    ("gram_total_high", 4, "fix", {
        "dish": "vegetable pizza slice with mayonnaise and ketchup",
        "grade": "B", "macros": (420, 14, 40, 23),
        "ingredients": [
            ("pizza, vegetable, with cheese", 150), ("mayonnaise", 15),
            ("ketchup", 15), ("olive, green, sliced", 5)]},
     "wave1 b15 r4: single pizza slice labelled with 64-item duplicated ingredient list"),
    ("gram_total_high", 7, "fix", {
        "dish": "beef meatballs with rice, corn, and salad",
        "grade": "B", "macros": (502, 22.5, 49.1, 24.0),
        "ingredients": [
            ("beef, ground, lean, cooked (meatballs)", 90),
            ("rice, white, cooked", 100), ("onion, cooked", 40),
            ("green bell pepper, cooked", 25), ("corn, sweet, cooked", 50),
            ("lettuce, iceberg, raw", 25), ("cucumber, raw", 30),
            ("tomato, raw", 20),
            ("vegetable oil (used in cooking meatballs/sauce)", 8)]},
     "wave1 b15 r7: single plate, truth had 9-item list duplicated 7x"),
    ("gram_total_high", 8, "skip", None, "wave1 b15 r8: blue plastic wash basin, not food"),

    # ── BATCH 16 (gram_total_high, rows 9-16) ───────────────────────────
    ("gram_total_high", 9, "skip", None,
     "wave1 b16 r1: single small water bottle labelled as 3600ml"),
    ("gram_total_high", 11, "skip", None, "wave1 b16 r3: bag of milk powder bulk container"),
    ("gram_total_high", 12, "fix", {
        "grade": "B", "macros": (350, 12, 50, 12),
        "ingredients": [
            ("bread roll, white, plain", 70),
            ("feta cheese (Egyptian white cheese, gebna bayda)", 40)]},
     "wave1 b16 r4: small white-cheese bread roll, truth labelled as 1680g + 960g (~20x off)"),

    # ── BATCH 17 (gram_total_high, rows 17-22) ──────────────────────────
    ("gram_total_high", 17, "fix", {
        "dish": "stuffed cabbage rolls (Egyptian mahshi koronb)",
        "grade": "B", "macros": (365, 6.3, 51.5, 15.8),
        "ingredients": [
            ("tomato, cooked", 22), ("onion, cooked", 15),
            ("sunflower oil", 7), ("dill, fresh", 4),
            ("parsley, fresh", 4), ("salt", 1),
            ("cabbage, boiled", 52), ("rice, white, cooked", 67)]},
     "wave1 b17 r1: 3 cabbage rolls, truth had 8-item list duplicated to 16 (5400g total)"),
    ("gram_total_high", 18, "fix", {
        "dish": "Egyptian feteer meshaltet with minced beef (feteer bel lahma)",
        "grade": "D", "macros": (1475, 45.4, 123.5, 89.1),
        "ingredients": [
            ("feteer dough, baked (wheat flour, butter, water)", 320),
            ("beef, ground, cooked", 180), ("onion, cooked", 40)]},
     "wave1 b17 r2: single feteer tray, truth had 3 ingredients × 12 dups (6840g)"),
    ("gram_total_high", 19, "skip", None, "wave1 b17 r3: bulk produce crate"),
    ("gram_total_high", 20, "skip", None, "wave1 b17 r4: pantry stockpile shelf"),
    ("gram_total_high", 22, "skip", None, "wave1 b17 r6: bakery display case"),

    # ── BATCH 18 (missing_required, rows 1-8) ───────────────────────────
    ("missing_required", 1, "skip", None, "wave1 b18 r1: empty tablecloth, no food"),
    ("missing_required", 2, "skip", None, "wave1 b18 r2: green can package back"),
    ("missing_required", 3, "skip", None, "wave1 b18 r3: orange package back"),
    ("missing_required", 6, "skip", None, "wave1 b18 r6: empty dark mug"),
    ("missing_required", 7, "fix", {
        "dish": "leftover sauce on small plate (orange/yellow, possibly tahini or hot sauce remnant)",
        "ingredients": [("sauce, mixed condiment remnant", 10)]},
     "wave1 b18 r7: leftover sauce remnant — name clean-up"),
    ("missing_required", 8, "skip", None, "wave1 b18 r8: Lion-brand snack package back"),

    # ── BATCH 19 (missing_required, rows 9-16) ──────────────────────────
    ("missing_required", 10, "skip", None, "wave1 b19 r2: TBS sandwich bag closed"),
    ("missing_required", 11, "skip", None, "wave1 b19 r3: package back, no name"),
    ("missing_required", 12, "skip", None, "wave1 b19 r4: torn snack bag back"),
    ("missing_required", 13, "fix", {"grade": "D"},
     "wave1 b19 r5: supplement bottle (Tribulus/Maca/Ginseng) — grade fix only"),
    ("missing_required", 14, "skip", None, "wave1 b19 r6: foil snack bag corner"),
    ("missing_required", 15, "skip", None, "wave1 b19 r7: empty paper cup"),
    ("missing_required", 16, "fix", {
        "dish": "multivitamin dietary supplement tablets (iron, biotin, vitamin A blend)",
        "grade": "D"},
     "wave1 b19 r8: identifiable as multivitamin from label"),

    # ── BATCH 22 (clean_qc) ─────────────────────────────────────────────
    ("__clean_qc__", 9, "skip", None,
     "wave1 b22 r1: nutrition-facts label of bread package — slipped through triage"),

    # ── BATCH 24 (clean_qc) ─────────────────────────────────────────────
    ("__clean_qc__", 39, "fix", {
        "dish": "salad (cucumber, lettuce, carrot)", "grade": "A",
        "macros": (45, 1.8, 10.5, 0.3),
        "ingredients": [("cucumber, raw", 120), ("lettuce, romaine, raw", 40),
                         ("carrot, raw", 50)]},
     "wave1 b24 r7: truth labelled tomato but no tomato visible — remove tomato"),
]


def main():
    print(f"Applying {len(VERDICTS)} wave-1 train-review verdicts to dataset {DATASET_ID}")
    print("=" * 78)

    # Map clean_qc positions: the script's iid() walks SAMPLES["__clean_qc__"]
    # but clean_qc batches each cover 8 rows so position 9 = batch 22 r1, etc.
    # The position numbers above (9, 39) are absolute positions in the
    # __clean_qc__ list (40 rows total), matching how batches were sliced.

    n_fix = n_skip = n_err = 0
    for cat, pos, action, payload, note in VERDICTS:
        try:
            ref = iid(cat, pos)
        except Exception as e:
            print(f"  ⚠ skip — couldn't lookup iid for {cat} pos {pos}: {e}")
            n_err += 1
            continue

        if action == "skip":
            tj = truth_from_willma(ref)  # preserve original truth, mark as skip via note
            db.upsert_correction(
                dataset_id=DATASET_ID,
                image_id=ref,
                truth_json=json.dumps(tj),
                note=f"skip — {note}",
            )
            n_skip += 1
            print(f"  SKIP  {cat} #{pos}  {ref}")
        else:
            p = payload or {}
            tj = truth_from_willma(
                ref,
                override_food=p.get("dish"),
                override_grade=p.get("grade"),
                override_macros=p.get("macros"),
                override_ingredients=p.get("ingredients"),
            )
            db.upsert_correction(
                dataset_id=DATASET_ID,
                image_id=ref,
                truth_json=json.dumps(tj),
                note=note,
            )
            n_fix += 1
            print(f"  FIX   {cat} #{pos}  {ref}")

    print()
    print(f"Result: {n_fix} fixes + {n_skip} skips + {n_err} errors")
    print(f"Total corrections in DB: ", end="")
    con = db._conn()
    n = con.execute("SELECT COUNT(*) FROM corrections WHERE dataset_id=?",
                    (DATASET_ID,)).fetchone()[0]
    v = db.current_dataset_version(DATASET_ID)
    print(f"{n} active · dataset @ v{v}")


if __name__ == "__main__":
    sys.exit(main() or 0)
