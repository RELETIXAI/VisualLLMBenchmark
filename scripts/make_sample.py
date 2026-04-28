"""Generate a small sample dataset (xlsx with embedded images) for smoke-testing.

Usage:  .venv/bin/python scripts/make_sample.py
Writes: data/sample_food_benchmark.xlsx
"""
from __future__ import annotations

import io
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "sample_food_benchmark.xlsx"
TMP = ROOT / "data" / "_tmp_sample_imgs"
TMP.mkdir(parents=True, exist_ok=True)

# Tiny synthetic "food" images (colored cards with text). Real images give better signal —
# this is just so the pipeline runs end-to-end without an external dataset.
SAMPLES = [
    ("Margherita pizza slice",  "Triangular slice of pizza with tomato, mozzarella and basil",
     {"calories": 285, "protein_g": 12, "carbs_g": 36, "fat_g": 10, "fiber_g": 2, "sugar_g": 4, "sodium_mg": 640},
     (210, 80, 60)),
    ("Caesar salad",            "Romaine, parmesan shavings, croutons and Caesar dressing",
     {"calories": 360, "protein_g": 9,  "carbs_g": 14, "fat_g": 30, "fiber_g": 3, "sugar_g": 3, "sodium_mg": 720},
     (110, 160, 90)),
    ("Cheeseburger",            "Beef patty, cheddar, lettuce, tomato, sesame bun",
     {"calories": 520, "protein_g": 28, "carbs_g": 41, "fat_g": 26, "fiber_g": 2, "sugar_g": 7, "sodium_mg": 980},
     (160, 100, 60)),
    ("Avocado toast",           "Sourdough toast topped with smashed avocado and chili flakes",
     {"calories": 290, "protein_g": 8,  "carbs_g": 30, "fat_g": 16, "fiber_g": 7, "sugar_g": 2, "sodium_mg": 410},
     (130, 170, 110)),
    ("Sushi roll (8 pcs)",      "California roll with crab, avocado, cucumber and rice",
     {"calories": 320, "protein_g": 10, "carbs_g": 50, "fat_g": 8,  "fiber_g": 4, "sugar_g": 5, "sodium_mg": 580},
     (200, 200, 200)),
]


def make_card(title: str, color: tuple[int, int, int], path: Path) -> None:
    img = Image.new("RGB", (480, 320), color)
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 28)
    except Exception:
        font = ImageFont.load_default()
    d.rectangle([20, 20, 460, 300], outline=(255, 255, 255), width=4)
    d.text((40, 140), title, fill=(255, 255, 255), font=font)
    img.save(path, format="JPEG", quality=85)


def main():
    wb = Workbook()
    ws = wb.active
    ws.title = "benchmark"
    headers = ["image", "food", "description", "calories", "protein_g",
               "carbs_g", "fat_g", "fiber_g", "sugar_g", "sodium_mg"]
    ws.append(headers)
    for i, col in enumerate(headers, 1):
        ws.column_dimensions[chr(64 + i)].width = 18 if i > 1 else 16
    ws.column_dimensions["A"].width = 12

    for i, (food, desc, nut, color) in enumerate(SAMPLES):
        row = i + 2
        ws.append(["", food, desc, nut["calories"], nut["protein_g"], nut["carbs_g"],
                   nut["fat_g"], nut["fiber_g"], nut["sugar_g"], nut["sodium_mg"]])
        ws.row_dimensions[row].height = 90

        path = TMP / f"sample_{i}.jpg"
        make_card(food, color, path)
        xli = XLImage(str(path))
        xli.width = 120
        xli.height = 80
        ws.add_image(xli, f"A{row}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
