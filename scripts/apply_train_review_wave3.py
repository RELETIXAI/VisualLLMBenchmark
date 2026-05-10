#!/usr/bin/env python3
"""Aggregate wave-3 agent results and apply fixes to the DB.

Reads /tmp/llmbench_results/wave3/*.json (agent verdict files), merges
them, and writes corrections to the DB via db.upsert_correction.

Each verdict file has:
  {
    "batch_id": "tiny_i_000",
    "category": "tiny_ingredient",
    "verdicts": [
      {
        "image_id": "<bare id>",
        "status": "ok" | "fix" | "skip",
        "reasoning": "...",
        "fix": {          # only when status=fix
          "dish": "...",          # optional
          "grade": "A-E",        # optional
          "macros": {"kcal":..., "protein":..., "carbs":..., "fat":...},  # optional
          "ingredients": [{"name":..., "quantity":..., "unit":"g"}, ...]  # optional
        }
      }, ...
    ]
  }
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
sys.path.insert(0, str(REPO))

from backend import db  # noqa: E402

DATASET_ID = 6
RESULTS_DIR = Path("/tmp/llmbench_results/wave3")


def _build_truth(existing: dict, fix: dict) -> dict:
    """Merge fix fields onto existing truth."""
    truth = {
        "food": existing.get("food"),
        "description": existing.get("description"),
        "nutrition": dict(existing.get("nutrition") or {}),
        "ingredients": list(existing.get("ingredients") or []),
        "health_score": existing.get("health_score"),
    }
    if "dish" in fix:
        truth["food"] = fix["dish"]
    if "grade" in fix:
        truth["health_score"] = fix["grade"]
    if "macros" in fix:
        m = fix["macros"]
        truth["nutrition"] = {
            "calories":  float(m.get("kcal", m.get("calories", 0))),
            "protein_g": float(m.get("protein", m.get("protein_g", 0))),
            "carbs_g":   float(m.get("carbs",   m.get("carbs_g",   0))),
            "fat_g":     float(m.get("fat",      m.get("fat_g",     0))),
        }
    if "ingredients" in fix:
        truth["ingredients"] = [
            {"name": i["name"], "quantity": float(i.get("quantity") or 0), "unit": i.get("unit","g")}
            for i in fix["ingredients"]
        ]
    return truth


def main():
    files = sorted(RESULTS_DIR.glob("*.json"))
    if not files:
        print(f"No verdict files in {RESULTS_DIR}")
        return

    total_ok = total_fix = total_skip = total_err = 0

    for f in files:
        try:
            data = json.loads(f.read_text())
        except Exception as e:
            print(f"[SKIP] {f.name}: parse error — {e}")
            continue

        batch_id = data.get("batch_id", f.stem)
        category = data.get("category", "unknown")
        verdicts = data.get("verdicts", [])

        for v in verdicts:
            image_id_bare = v.get("image_id", "")
            status = v.get("status", "")
            reasoning = v.get("reasoning", "")
            image_ref = f"6/{image_id_bare}.jpg"

            if status == "ok":
                total_ok += 1
                continue

            note_prefix = f"wave3 {batch_id} {category}"

            if status == "skip":
                db.upsert_correction(
                    dataset_id=DATASET_ID,
                    image_id=image_ref,
                    truth_json=json.dumps(None),
                    note=f"skip — {note_prefix}: {reasoning[:120]}",
                )
                total_skip += 1

            elif status == "fix":
                fix = v.get("fix", {})
                if not fix:
                    total_err += 1
                    continue
                # Load existing truth to merge into
                existing_row = db.get_correction(DATASET_ID, image_ref)
                existing_json = existing_row["truth_json"] if existing_row else None
                existing = json.loads(existing_json) if existing_json else {}
                if not isinstance(existing, dict):
                    existing = {}
                new_truth = _build_truth(existing, fix)
                db.upsert_correction(
                    dataset_id=DATASET_ID,
                    image_id=image_ref,
                    truth_json=json.dumps(new_truth),
                    note=f"{note_prefix}: {reasoning[:120]}",
                )
                total_fix += 1
            else:
                total_err += 1

    print(f"\nWave 3 apply complete:")
    print(f"  ok (no change): {total_ok}")
    print(f"  fixes applied:  {total_fix}")
    print(f"  skips written:  {total_skip}")
    print(f"  errors/unknown: {total_err}")
    print(f"  files processed: {len(files)}")


if __name__ == "__main__":
    main()
