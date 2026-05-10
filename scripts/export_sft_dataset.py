#!/usr/bin/env python3
"""
Export WILLMA truth + local images → SFT JSONL for mlx-vlm.

- Holdout: every distinct image_ref ever stored in row_results
  (data/benchmark.db). These are the benchmark images and must NEVER
  appear in train.jsonl. They go to eval.jsonl only when their truth
  rows are present in the WILLMA xlsx.
- Train: every other WILLMA row whose image is hydrated locally in
  data/images/<dataset_id>/<image_id>.jpg.
- Output: data/sft/{train,eval}.jsonl + manifest.json + HOLDOUT.lock
  (chmod 444; the trainer asserts this hash before starting).

Re-runnable: as more images hydrate, re-run and train.jsonl grows.

WILLMA xlsx columns (from inspection 2026-05-09):
  name, calories, protein, carbohydrates, fat, healthScore, sodium,
  sugar, fiber, time, createdAt, image, scanType, Image Hyperlink,
  imageSearchName, ingredients

  - `image` is a bare id (e.g. "1775370302683-3dea43be-...-d8922a463a73")
  - `ingredients` is a JSON string list of dicts with keys:
      name, quantity, unit, calories, protein, carbohydrates, fat,
      sodium, sugar, fiber, ...
  - `healthScore` is a single A-E letter

row_results.image_ref format: "<dataset_id>/<image_id>.jpg" (e.g.
"6/1776525552339-...uuid.jpg"). Hydrated files live at
data/images/<dataset_id>/<image_id>.jpg.
"""
from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import stat
import sys
from datetime import datetime
from pathlib import Path

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
DB = REPO / "data" / "benchmark.db"
XLSX = REPO / "data" / "uploads" / "WILLMA Meal Scans Extract 20-4-2026.xlsx"
IMAGES_DIR = REPO / "data" / "images"
OUT_DIR = REPO / "data" / "sft"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Default dataset_id used by the benchmark for the WILLMA xlsx (id=6 in
# datasets table). The holdout set is built from row_results.image_ref
# regardless, so this is only used to find local image files for the
# *non-holdout* WILLMA rows where we just have a bare image id.
DEFAULT_DATASET_ID = "6"

SYSTEM = (
    "You analyse food photos. Return STRICT JSON with this exact shape: "
    '{"dish": str, "ingredients": [{"name": str, "grams": number}], '
    '"macros": {"kcal": number, "protein_g": number, "carbs_g": number, "fat_g": number}, '
    '"health_grade": "A"|"B"|"C"|"D"|"E"}. '
    "No prose, no markdown, no commentary."
)
USER = "Analyse this meal photo."


# ---------------------------------------------------------------------------
# Holdout
# ---------------------------------------------------------------------------

def get_holdout() -> set[str]:
    """Return the set of image_refs (e.g. '6/<id>.jpg') used by any
    benchmark run. These are off-limits for training."""
    if not DB.exists():
        return set()
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT DISTINCT image_ref FROM row_results WHERE image_ref IS NOT NULL"
    ).fetchall()
    con.close()
    return {r[0] for r in rows if r[0]}


def holdout_to_image_ids(holdout: set[str]) -> set[str]:
    """Reduce holdout image_refs to their bare image_id (drop any
    '<dsid>/' prefix and trailing extension), so we can match against
    the WILLMA `image` column directly."""
    out = set()
    for ref in holdout:
        name = ref.rsplit("/", 1)[-1]
        if "." in name:
            name = name.rsplit(".", 1)[0]
        out.add(name)
    return out


# ---------------------------------------------------------------------------
# WILLMA loader
# ---------------------------------------------------------------------------

def load_willma_rows() -> list[dict]:
    import pandas as pd
    df = pd.read_excel(XLSX)
    # Keep original case for `image` etc. — we use exact column names below.
    return df.to_dict("records")


def load_corrections(dataset_id: str = DEFAULT_DATASET_ID) -> tuple[dict[str, dict], set[str]]:
    """Return ({bare_image_id: corrected_truth_dict}, {bare_image_id: skip})
    from the DB corrections table.

    Corrections are keyed in the table by full image_ref like '6/<id>.jpg';
    we strip prefix + extension so the key matches the bare `image` column
    in the WILLMA xlsx.

    A correction whose `note` starts with 'skip' is treated as a SKIP marker:
    the row is excluded from both train.jsonl and eval.jsonl. Used for
    photos that aren't a meal (package labels, blurry shots, etc.). The
    truth_json is preserved so the benchmark UI still has something to show.
    """
    sys.path.insert(0, str(REPO))
    try:
        from backend import db  # type: ignore
    except Exception as e:
        print(f"[corrections] WARNING: could not import backend.db ({e}); "
              f"corrections will NOT be applied.", file=sys.stderr)
        return {}, set()
    out: dict[str, dict] = {}
    skip: set[str] = set()
    try:
        rows = db.list_corrections(int(dataset_id))
    except Exception as e:
        print(f"[corrections] WARNING: list_corrections failed: {e}", file=sys.stderr)
        return {}, set()
    for r in rows:
        ref = r.get("image_id") or ""
        bare = ref.rsplit("/", 1)[-1]
        if "." in bare:
            bare = bare.rsplit(".", 1)[0]
        note = (r.get("note") or "").strip().lower()
        if note.startswith("skip"):
            skip.add(bare)
            continue
        try:
            out[bare] = json.loads(r.get("truth_json") or "{}")
        except Exception:
            continue
    return out, skip


def apply_correction(row: dict, correction: dict) -> dict:
    """Return a new row dict with the correction's fields written over
    the WILLMA columns we read in build_assistant_text(). Maps from the
    corrections schema (food/nutrition/ingredients/health_score) to the
    WILLMA column names (name/calories/.../healthScore/ingredients)."""
    new = dict(row)  # shallow copy — don't mutate caller
    if "food" in correction and correction["food"]:
        new["name"] = correction["food"]
    nutr = correction.get("nutrition") or {}
    if "calories" in nutr:    new["calories"]      = nutr["calories"]
    if "protein_g" in nutr:   new["protein"]       = nutr["protein_g"]
    if "carbs_g" in nutr:     new["carbohydrates"] = nutr["carbs_g"]
    if "fat_g" in nutr:       new["fat"]           = nutr["fat_g"]
    if "ingredients" in correction:
        # build_assistant_text expects ingredients as JSON string OR list of dicts
        new["ingredients"] = correction["ingredients"]
    if "health_score" in correction and correction["health_score"]:
        new["healthScore"] = correction["health_score"]
    return new


def image_path_for(image_id: str) -> Path | None:
    """Find a local file for a bare WILLMA image id. Tries the default
    dataset bucket first (data/images/6/<id>.jpg), then falls back to
    any dataset bucket that already has the file."""
    if not image_id:
        return None
    # Most common: data/images/6/<id>.jpg
    primary = IMAGES_DIR / DEFAULT_DATASET_ID / f"{image_id}.jpg"
    if primary.exists():
        return primary
    # Fallbacks: try other extensions and other dataset buckets
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        for bucket in IMAGES_DIR.iterdir() if IMAGES_DIR.exists() else []:
            if not bucket.is_dir():
                continue
            cand = bucket / f"{image_id}{ext}"
            if cand.exists():
                return cand
    return None


# ---------------------------------------------------------------------------
# Truth → assistant JSON
# ---------------------------------------------------------------------------

def _to_float(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default


def build_assistant_text(row: dict) -> str | None:
    """Construct the strict-JSON answer the model should emit for this
    WILLMA row. Return None when the row is unusable."""
    try:
        dish = str(row.get("name") or "").strip()
        if not dish:
            return None

        grade_raw = row.get("healthScore")
        grade = str(grade_raw).strip().upper() if grade_raw is not None else ""
        if grade not in {"A", "B", "C", "D", "E"}:
            return None

        ingr_raw = row.get("ingredients")
        if isinstance(ingr_raw, str):
            try:
                ingr = json.loads(ingr_raw)
            except Exception:
                return None
        elif isinstance(ingr_raw, list):
            ingr = ingr_raw
        else:
            return None
        if not isinstance(ingr, list) or not ingr:
            return None

        ingredients_out = []
        for it in ingr:
            if not isinstance(it, dict):
                continue
            name = str(it.get("name") or "").strip()
            if not name:
                continue
            qty = _to_float(it.get("quantity"), 0.0)
            unit = str(it.get("unit") or "").strip().lower()
            # Convert kg→g, mg→g; treat empty/g/ml as grams (ml ≈ g for
            # liquids and the truth-set is dominated by gram quantities)
            if unit in ("kg",):
                grams = qty * 1000.0
            elif unit in ("mg",):
                grams = qty / 1000.0
            else:
                grams = qty
            if grams <= 0:
                continue
            ingredients_out.append({"name": name, "grams": float(grams)})
        if not ingredients_out:
            return None

        out = {
            "dish": dish,
            "ingredients": ingredients_out,
            "macros": {
                "kcal":      _to_float(row.get("calories")),
                "protein_g": _to_float(row.get("protein")),
                "carbs_g":   _to_float(row.get("carbohydrates")),
                "fat_g":     _to_float(row.get("fat")),
            },
            "health_grade": grade,
        }
        return json.dumps(out, ensure_ascii=False)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# JSONL message format (mlx-vlm conversational)
# ---------------------------------------------------------------------------

def make_message(image_path: Path, assistant_text: str) -> dict:
    # mlx_vlm trainer expects:
    #   - top-level "images" list with the absolute image path(s)
    #   - plain-string "content" in each message (NOT nested typed objects)
    # The trainer extracts images from the top-level key and inserts image
    # tokens into the user message via apply_chat_template(num_images=1).
    return {
        "messages": [
            {"role": "system",    "content": SYSTEM},
            {"role": "user",      "content": USER},
            {"role": "assistant", "content": assistant_text},
        ],
        "images": [str(image_path.resolve())],
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main():
    print(f"[1/5] Reading holdout from {DB}")
    holdout_refs = get_holdout()
    holdout_ids = holdout_to_image_ids(holdout_refs)
    print(f"      → {len(holdout_refs)} image_refs in holdout, "
          f"{len(holdout_ids)} distinct image_ids")

    print(f"[2/5] Reading WILLMA truth from {XLSX}")
    if not XLSX.exists():
        print(f"ERROR: WILLMA xlsx not found at {XLSX}", file=sys.stderr)
        return 2
    rows = load_willma_rows()
    corrections, skip_set = load_corrections(DEFAULT_DATASET_ID)
    print(f"      → {len(rows)} truth rows · {len(corrections)} corrections · {len(skip_set)} skips in DB")

    print(f"[3/5] Filtering, building messages, writing JSONL")
    counters = {
        "no_image_id": 0,
        "no_local_image": 0,
        "in_holdout_no_image": 0,
        "bad_truth": 0,
        "ok_train": 0,
        "ok_eval": 0,
        "corrected": 0,
        "skipped_marker": 0,
    }
    train_path = OUT_DIR / "train.jsonl"
    eval_path  = OUT_DIR / "eval.jsonl"
    with train_path.open("w") as ftr, eval_path.open("w") as fev:
        for row in rows:
            iid_raw = row.get("image")
            iid = str(iid_raw).strip() if iid_raw is not None else ""
            if not iid or iid.lower() == "nan":
                counters["no_image_id"] += 1
                continue
            # Skip marker — exclude from train AND eval
            if iid in skip_set:
                counters["skipped_marker"] += 1
                continue
            in_hold = iid in holdout_ids
            # Overlay correction (if any) before building the SFT answer
            if iid in corrections:
                row = apply_correction(row, corrections[iid])
                counters["corrected"] += 1
            assist = build_assistant_text(row)
            if assist is None:
                counters["bad_truth"] += 1
                continue
            ipath = image_path_for(iid)
            if ipath is None:
                counters["in_holdout_no_image" if in_hold else "no_local_image"] += 1
                continue
            msg = make_message(ipath, assist)
            if in_hold:
                fev.write(json.dumps(msg) + "\n")
                counters["ok_eval"] += 1
            else:
                ftr.write(json.dumps(msg) + "\n")
                counters["ok_train"] += 1

    print(f"[4/5] Writing manifest + HOLDOUT.lock (read-only)")
    manifest = {
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "train_count": counters["ok_train"],
        "eval_count":  counters["ok_eval"],
        "counters":    counters,
        "holdout_image_refs": sorted(holdout_refs),
        "holdout_image_ids":  sorted(holdout_ids),
        "system_prompt": SYSTEM,
        "user_prompt":   USER,
        "xlsx":          str(XLSX),
        "db":            str(DB),
        "images_dir":    str(IMAGES_DIR),
    }
    holdout_blob = json.dumps(manifest["holdout_image_ids"], sort_keys=True).encode()
    holdout_sha = hashlib.sha1(holdout_blob).hexdigest()
    manifest["holdout_sha1"] = holdout_sha
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))

    lock = OUT_DIR / "HOLDOUT.lock"
    # If a previous run made it read-only, we must restore write to overwrite.
    if lock.exists():
        os.chmod(lock, stat.S_IRUSR | stat.S_IWUSR)
    lock.write_text(json.dumps({
        "sha1": holdout_sha,
        "image_ids":  manifest["holdout_image_ids"],
        "image_refs": manifest["holdout_image_refs"],
    }, indent=2))
    os.chmod(lock, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # 0444

    print(f"[5/5] Done.")
    print(f"      train: {counters['ok_train']:>6} → {train_path}")
    print(f"      eval:  {counters['ok_eval']:>6} → {eval_path}")
    print(f"      counters: {counters}")
    print(f"      manifest: {OUT_DIR/'manifest.json'}")
    print(f"      lock:     {lock} (read-only, sha1={holdout_sha[:12]}…)")


if __name__ == "__main__":
    sys.exit(main() or 0)
