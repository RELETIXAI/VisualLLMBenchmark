# Fine-Tuning Gemma 4 26B-A4B for Food-Photo Analysis — Hands-On Guide

> Target audience: someone who has the Reletix LLMbenchmark repo, an Apple Silicon Mac with ≥ 48 GB unified memory, the WILLMA dataset, and wants to fine-tune Gemma 4 26B-A4B (the 26 B-parameter Mixture-of-Experts model with ~3.8 B active per token) on their food-photo dataset.
>
> Reading time: ~30 min. Total wall-clock if you actually run it: ~24 h (mostly the overnight training step). Active hands-on time: ~6 h spread across two days.

---

## Table of contents

1. [What you'll have at the end](#1-what-youll-have-at-the-end)
2. [Hardware & software prerequisites](#2-hardware--software-prerequisites)
3. [The mental model in two minutes](#3-the-mental-model-in-two-minutes)
4. [Inventory: what's on your disk right now](#4-inventory-whats-on-your-disk-right-now)
5. [Phase 0 — Local-only safety + model inventory script](#phase-0--local-only-safety--model-inventory-script)
6. [Phase 1 — Export the SFT dataset (50-image holdout)](#phase-1--export-the-sft-dataset-50-image-holdout)
7. [Phase 2 — Smoke training on E4B (~1 hour)](#phase-2--smoke-training-on-e4b-1-hour)
8. [Phase 3 — Backend training module](#phase-3--backend-training-module)
9. [Phase 4 — Training tab in the frontend](#phase-4--training-tab-in-the-frontend)
10. [Phase 5 — Wire adapters into MLXProvider](#phase-5--wire-adapters-into-mlxprovider)
11. [Phase 6 — The real run: 26B-A4B QLoRA overnight](#phase-6--the-real-run-26b-a4b-qlora-overnight)
12. [Phase 7 — Evaluate against the 50-image holdout](#phase-7--evaluate-against-the-50-image-holdout)
13. [Phase 8 (optional) — Merge adapter into bf16 + re-quantize](#phase-8-optional--merge-adapter-into-bf16--re-quantize)
14. [Troubleshooting](#14-troubleshooting)
15. [Appendix A — File layout reference](#appendix-a--file-layout-reference)
16. [Appendix B — Commands cheat sheet](#appendix-b--commands-cheat-sheet)

---

## 1. What you'll have at the end

After completing all phases:

- A **LoRA adapter** (~80–200 MB file) that, when loaded alongside the 4-bit Gemma 4 26B-A4B base, makes the model produce strict-JSON food analysis matching your benchmark schema.
- A **Training tab** in the existing FastAPI app where you can launch new fine-tunes, watch live loss curves, and load adapters back into benchmark runs.
- A **closed evaluation loop**: train → click "benchmark this adapter" → see scores against the same 50 held-out images you've been using all along, scored by the same pipeline.
- A **reproducible recipe** so future Gemma releases or new datasets reuse the same wiring with only path changes.

If you stop after Phase 2 (smoke run): you'll have a working pipeline and a tiny adapter on E4B that proves the chain works. That alone is valuable.

---

## 2. Hardware & software prerequisites

### Hardware

| Item | Requirement | Why |
|---|---|---|
| Apple Silicon Mac | M-series (M3+ recommended) | MLX requires Metal |
| Unified RAM | **≥ 48 GB** | 26B-A4B QLoRA needs ~28–30 GB during a step |
| Wired RAM ceiling | **≥ 36 GB** (set via `iogpu.wired_limit_mb`) | MLX won't use RAM beyond the wired limit during training |
| Free disk | ≥ 100 GB | bf16 source 48 GB + 4-bit base 15 GB + dataset + checkpoints |

To check / set wired RAM:

```bash
# read current
sudo sysctl iogpu.wired_limit_mb

# raise it (this Mac is at 45008)
sudo sysctl iogpu.wired_limit_mb=45008

# make it persist across reboots
sudo bash -c 'echo "iogpu.wired_limit_mb=45008" >> /etc/sysctl.conf'
```

### Software

| Item | Version | Notes |
|---|---|---|
| macOS | 14+ (Sonoma) | required by recent MLX |
| Python | 3.11 or 3.13 | the user's venv at `/Users/samueltoma/AI/venv` is 3.13 |
| `mlx` | latest | installed via `pip install -U mlx` |
| `mlx-vlm` | latest | installed via `pip install -U mlx-vlm` |
| `transformers` | latest | for tokenizer / processor |
| `huggingface_hub` | latest | for `hf download` and `hf_transfer` |

Quick install in the existing venv:

```bash
source /Users/samueltoma/AI/venv/bin/activate
pip install -U mlx mlx-vlm mlx-lm transformers huggingface_hub safetensors pillow datasets
```

Verify:

```bash
python -c "import mlx, mlx_vlm; print('mlx', mlx.__version__); print('mlx_vlm', mlx_vlm.__version__)"
```

### Repo layout assumption

```
/Users/samueltoma/Documents/Reletix/LLMbenchmark      ← the repo (you are here)
/Users/samueltoma/AI/models/source                    ← bf16 originals
/Users/samueltoma/AI/models/mlx                       ← MLX quants (4-bit, 8-bit)
/Users/samueltoma/AI/venv                             ← Python virtualenv
```

If your paths differ, search-and-replace `/Users/samueltoma/AI` throughout this guide.

---

## 3. The mental model in two minutes

Gemma 4 26B-A4B is a **Mixture of Experts** model:

```
For each token:
  ┌─────────────────────────────────────────┐
  │ Self-attention (16 heads)               │ ← always runs (~few hundred M params)
  ├─────────────────────────────────────────┤
  │ Shared dense MLP                        │ ← always runs (~few hundred M params)
  ├─────────────────────────────────────────┤
  │ Router picks 8 of 128 experts           │
  │ Expert 17 ↘                             │
  │ Expert 42 ──→ weighted sum              │ ← only 8 of 128 fire per token
  │ Expert 99 ↗                             │   (~3.8 B active params)
  └─────────────────────────────────────────┘
```

**Key consequence for fine-tuning**: you put LoRA adapters on the **always-active path** (attention + shared MLP) and **leave the 128 routed experts frozen**. They already encode broad world knowledge from pretraining; what you need to teach is the output style, JSON schema, and food-domain emphasis — that's a "shared path" job.

Memory math on a 48 GB Mac for the **4-bit base + LoRA on shared path**:

```
4-bit weights (mixed precision quant)        15 GB
Activations (because only ~3.8B is active)   10 GB
Gradients (LoRA params only, ~50M)            0.2 GB
Adam optimizer state for LoRA                 0.4 GB
Misc (KV cache, embeddings, OS, FastAPI)      4 GB
─────────────────────────────────────────────────
Total during a training step                ~30 GB   ✅ fits in 45 GB wired
```

Compared to fine-tuning the **bf16 base directly** (impossible on this Mac):

```
bf16 weights                                 48 GB    ❌ already over wired limit
+ activations + grads + opt state            +20 GB
```

**That's why the bf16 source you downloaded is for *conversion* and *adapter merging*, not for training directly.**

---

## 4. Inventory: what's on your disk right now

```
/Users/samueltoma/AI/models/
├── source/
│   ├── google--gemma-4-26B-A4B-it/      48 GB · bf16 original ✓ (use as conversion source)
│   ├── google--gemma-4-E4B/             ~16 GB · bf16, downloading now
│   ├── Qwen--Qwen3.6-35B-A3B/
│   └── Qwen--Qwen2.5-1.5B-Instruct/
└── mlx/
    ├── google--gemma-4-26B-A4B-it/      15 GB · MLX 4-bit mixed (8-bit on dense+router)
    ├── gemma-4-31b-it/                  17 GB · MLX 4-bit
    ├── gemma4-e2b/                      0.7 GB
    ├── gemma4-e1b/                      ~0.5 GB
    └── Qwen--Qwen3.6-35B-A3B-mlx4 / -mlx8
```

**For training, only two of these matter:**

| Purpose | Path |
|---|---|
| Smoke training (Phase 2) | `/Users/samueltoma/AI/models/source/google--gemma-4-E4B` |
| Real training (Phase 6) | `/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it` |
| Adapter merge target (Phase 8) | `/Users/samueltoma/AI/models/source/google--gemma-4-26B-A4B-it` |

---

## Phase 0 — Local-only safety + model inventory script

**Goal**: never touch S3, never silently download something. Two small scripts.

**Time**: 20 minutes.

### Step 0.1 — Add a local-only image guard

Edit `backend/main.py`. Find where image_refs are resolved (the `_resolve_image_for_row` or similar function). Add at the top of the file:

```python
import os
LOCAL_ONLY_IMAGES = os.environ.get("LLMBENCH_IMAGES_ONLY_LOCAL", "0") == "1"
```

Then inside whatever function resolves an image_ref to bytes, add at the very top:

```python
if LOCAL_ONLY_IMAGES and (image_ref.startswith("http") or image_ref.startswith("s3://")):
    raise RuntimeError(
        f"LLMBENCH_IMAGES_ONLY_LOCAL=1 but image_ref is remote: {image_ref!r}. "
        f"Make sure the image has been hydrated to data/images/."
    )
```

Set the env var permanently for fine-tuning workflows:

```bash
echo 'export LLMBENCH_IMAGES_ONLY_LOCAL=1' >> ~/.zshrc
source ~/.zshrc
```

### Step 0.2 — Model inventory script

Create `scripts/list_models.py`:

```python
#!/usr/bin/env python3
"""List local Gemma / Qwen / etc. models and report what's tunable on this Mac."""
import json, os, sys
from pathlib import Path

ROOT = Path("/Users/samueltoma/AI/models")
WIRED_GB = 45  # change if your iogpu.wired_limit_mb is different

def estimate(cfg):
    q = cfg.get("quantization")
    txt = cfg.get("text_config", {}) or {}
    n_experts = txt.get("num_experts") or 0
    moe = bool(n_experts)
    bits = (q or {}).get("bits") if isinstance(q, dict) else None
    return {"quant_bits": bits, "moe": moe, "n_experts": n_experts}

def directory_size_gb(p: Path) -> float:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e9

def model_status(p: Path):
    cfg_path = p / "config.json"
    if not cfg_path.exists():
        return None
    cfg = json.loads(cfg_path.read_text())
    info = estimate(cfg)
    info["path"] = str(p)
    info["size_gb"] = round(directory_size_gb(p), 1)
    info["model_type"] = cfg.get("model_type")
    # Rough tunability assessment
    weights_gb = info["size_gb"]
    fits_train = (weights_gb + 14) <= WIRED_GB  # +14 GB headroom for activations
    info["tunable_on_this_mac"] = fits_train
    info["use_for"] = (
        "TRAINING base" if fits_train and info["quant_bits"] in (4, 8) else
        "smoke training" if fits_train and info["quant_bits"] is None else
        "conversion source / adapter-merge target" if not fits_train else
        "?"
    )
    return info

def main():
    rows = []
    for sub in ("source", "mlx"):
        d = ROOT / sub
        if not d.exists(): continue
        for child in sorted(d.iterdir()):
            if not child.is_dir(): continue
            r = model_status(child)
            if r: rows.append(r)
    # Pretty print
    print(f"{'Path':<70} {'Size GB':>8} {'Quant':<6} {'MoE':<5} {'Tunable':<8} Use for")
    print("-" * 130)
    for r in rows:
        print(f"{r['path']:<70} {r['size_gb']:>8} "
              f"{(str(r['quant_bits'])+'-bit') if r['quant_bits'] else 'bf16':<6} "
              f"{('Y('+str(r['n_experts'])+')') if r['moe'] else 'N':<5} "
              f"{'YES' if r['tunable_on_this_mac'] else 'no':<8} "
              f"{r['use_for']}")

if __name__ == "__main__":
    main()
```

Run it:

```bash
cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
python scripts/list_models.py
```

Expected output (abridged):

```
Path                                                              Size GB Quant  MoE   Tunable  Use for
-----------------------------------------------------------------------------------------------------
.../source/google--gemma-4-26B-A4B-it                                48.0 bf16   Y(128) no       conversion source / adapter-merge target
.../source/google--gemma-4-E4B                                       16.0 bf16   N      YES      smoke training
.../mlx/google--gemma-4-26B-A4B-it                                   15.0 4-bit  Y(128) YES      TRAINING base
.../mlx/gemma-4-31b-it                                               17.0 4-bit  N      YES      TRAINING base
```

You now know — exactly — which model goes in which step.

---

## Phase 1 — Export the SFT dataset (50-image holdout)

**Goal**: turn `WILLMA Meal Scans Extract.xlsx` into JSONL files mlx-vlm can train on, with the 50 benchmark images **physically excluded** and the holdout list locked into a read-only file.

**Time**: 1–2 h depending on image hydration progress.

### Step 1.1 — Confirm what's in the holdout

```bash
sqlite3 /Users/samueltoma/Documents/Reletix/LLMbenchmark/data/benchmark.db \
  "SELECT COUNT(DISTINCT image_ref) FROM row_results;"
```

Expect `50` (or whatever your benchmark size is). These are your **never-train-on-these** images.

```bash
# See a few of them
sqlite3 /Users/samueltoma/Documents/Reletix/LLMbenchmark/data/benchmark.db \
  "SELECT DISTINCT image_ref FROM row_results LIMIT 5;"
```

### Step 1.2 — Create the export script

Create `scripts/export_sft_dataset.py`:

```python
#!/usr/bin/env python3
"""
Export WILLMA truth + local images → SFT JSONL for mlx-vlm.

- Holdout: all image_refs ever used in benchmark runs (read from data/benchmark.db).
- Train: every other row whose image is hydrated locally in data/images/.
- Output: data/sft/{train,eval}.jsonl + manifest.json + HOLDOUT.lock.

Re-runnable: as you hydrate more images, rerun and train.jsonl grows.
"""
import json, hashlib, sqlite3, sys, os, stat
from pathlib import Path
from datetime import datetime

REPO = Path("/Users/samueltoma/Documents/Reletix/LLMbenchmark")
DB = REPO / "data" / "benchmark.db"
XLSX = REPO / "data" / "uploads" / "WILLMA Meal Scans Extract 20-4-2026.xlsx"
IMAGES_DIR = REPO / "data" / "images"
OUT_DIR = REPO / "data" / "sft"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM = (
    "You analyse food photos. Return STRICT JSON with this exact shape: "
    '{"dish": str, "ingredients": [{"name": str, "grams": number}], '
    '"macros": {"kcal": number, "protein_g": number, "carbs_g": number, "fat_g": number}, '
    '"health_grade": "A"|"B"|"C"|"D"|"E"}. '
    "No prose, no markdown, no commentary."
)
USER = "Analyse this meal photo."

def get_holdout():
    con = sqlite3.connect(DB)
    rows = con.execute("SELECT DISTINCT image_ref FROM row_results").fetchall()
    con.close()
    return {r[0] for r in rows if r[0]}

def load_willma_rows():
    """Load WILLMA xlsx; expected columns: image_ref (or url), dish, ingredients (JSON), macros (JSON), health_grade."""
    import pandas as pd
    df = pd.read_excel(XLSX)
    # Normalize column names — adjust to your sheet
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df.to_dict("records")

def image_path_for(image_ref: str) -> Path | None:
    """Map an image_ref to a local file. Adjust if your scheme is different."""
    if not image_ref: return None
    # If image_ref is already a local path/sha
    candidates = [
        IMAGES_DIR / image_ref,
        IMAGES_DIR / f"{image_ref}.jpg",
        IMAGES_DIR / f"{image_ref}.png",
    ]
    # If it's a URL, try the basename
    if "/" in image_ref:
        name = image_ref.rsplit("/", 1)[-1].split("?")[0]
        candidates += [IMAGES_DIR / name]
    for c in candidates:
        if c.exists() and c.is_file():
            return c
    return None

def build_assistant_text(row: dict) -> str | None:
    """Construct the strict-JSON answer the model should emit. Return None to skip."""
    try:
        dish = (row.get("dish") or row.get("food_name") or "").strip()
        ingr = row.get("ingredients")
        macros = row.get("macros")
        grade = row.get("health_grade") or row.get("grade")
        if isinstance(ingr, str): ingr = json.loads(ingr)
        if isinstance(macros, str): macros = json.loads(macros)
        if not dish or not isinstance(ingr, list) or not isinstance(macros, dict):
            return None
        if not grade or grade not in "ABCDE":
            return None
        out = {
            "dish": dish,
            "ingredients": [
                {"name": str(i["name"]), "grams": float(i.get("grams", 0))}
                for i in ingr if i.get("name")
            ],
            "macros": {
                "kcal": float(macros.get("kcal", 0)),
                "protein_g": float(macros.get("protein_g", 0)),
                "carbs_g": float(macros.get("carbs_g", 0)),
                "fat_g": float(macros.get("fat_g", 0)),
            },
            "health_grade": grade,
        }
        return json.dumps(out, ensure_ascii=False)
    except Exception:
        return None

def make_message(image_path: Path, assistant_text: str) -> dict:
    return {
        "messages": [
            {"role": "system", "content": [{"type": "text", "text": SYSTEM}]},
            {"role": "user",   "content": [
                {"type": "image", "image": str(image_path.resolve())},
                {"type": "text",  "text": USER},
            ]},
            {"role": "assistant", "content": [{"type": "text", "text": assistant_text}]},
        ]
    }

def main():
    print(f"[1/5] Reading holdout from {DB}")
    holdout = get_holdout()
    print(f"      → {len(holdout)} image_refs in holdout")

    print(f"[2/5] Reading WILLMA truth from {XLSX}")
    rows = load_willma_rows()
    print(f"      → {len(rows)} truth rows")

    print(f"[3/5] Filtering, building messages, writing JSONL")
    train_rows, eval_rows, skipped = [], [], {"no_image": 0, "in_holdout_no_image": 0,
                                              "bad_truth": 0, "ok_train": 0, "ok_eval": 0}
    train_path = OUT_DIR / "train.jsonl"
    eval_path  = OUT_DIR / "eval.jsonl"
    with train_path.open("w") as ftr, eval_path.open("w") as fev:
        for row in rows:
            iref = row.get("image_ref") or row.get("image_url") or row.get("image")
            if not iref:
                skipped["no_image"] += 1
                continue
            ipath = image_path_for(iref)
            in_hold = iref in holdout
            assist = build_assistant_text(row)
            if assist is None:
                skipped["bad_truth"] += 1
                continue
            if ipath is None:
                skipped["no_image" if not in_hold else "in_holdout_no_image"] += 1
                continue
            msg = make_message(ipath, assist)
            if in_hold:
                fev.write(json.dumps(msg) + "\n")
                skipped["ok_eval"] += 1
            else:
                ftr.write(json.dumps(msg) + "\n")
                skipped["ok_train"] += 1

    print(f"[4/5] Writing manifest + HOLDOUT.lock (read-only)")
    manifest = {
        "built_at": datetime.now().isoformat(),
        "train_count": skipped["ok_train"],
        "eval_count": skipped["ok_eval"],
        "skipped": skipped,
        "holdout_image_refs": sorted(list(holdout)),
        "system_prompt": SYSTEM,
        "user_prompt": USER,
    }
    holdout_sha = hashlib.sha1(
        json.dumps(manifest["holdout_image_refs"]).encode()
    ).hexdigest()
    manifest["holdout_sha1"] = holdout_sha
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))
    lock = OUT_DIR / "HOLDOUT.lock"
    lock.write_text(json.dumps({"sha1": holdout_sha,
                                "image_refs": manifest["holdout_image_refs"]}, indent=2))
    os.chmod(lock, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # 0444

    print(f"[5/5] Done.")
    print(f"      train: {skipped['ok_train']:>6} → {train_path}")
    print(f"      eval:  {skipped['ok_eval']:>6} → {eval_path}")
    print(f"      skipped: {skipped}")
    print(f"      manifest: {OUT_DIR/'manifest.json'}")
    print(f"      lock:     {lock} (read-only)")

if __name__ == "__main__":
    main()
```

**Important**: the WILLMA xlsx column names in this script are guesses. **Open the file once** and confirm:

```bash
python -c "import pandas as pd; df = pd.read_excel('/Users/samueltoma/Documents/Reletix/LLMbenchmark/data/uploads/WILLMA Meal Scans Extract 20-4-2026.xlsx'); print(df.columns.tolist()); print(df.head(2).to_dict('records'))"
```

Then adjust `load_willma_rows`, `image_path_for`, `build_assistant_text` to match the real columns.

### Step 1.3 — Run the export

```bash
cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
python scripts/export_sft_dataset.py
```

Expected output:

```
[1/5] Reading holdout from data/benchmark.db
      → 50 image_refs in holdout
[2/5] Reading WILLMA truth from .../WILLMA Meal Scans Extract 20-4-2026.xlsx
      → 35803 truth rows
[3/5] Filtering, building messages, writing JSONL
[4/5] Writing manifest + HOLDOUT.lock (read-only)
[5/5] Done.
      train: 12345 → data/sft/train.jsonl
      eval:     50 → data/sft/eval.jsonl
      skipped: {'no_image': 23408, 'in_holdout_no_image': 0, 'bad_truth': 0, 'ok_train': 12345, 'ok_eval': 50}
```

The `no_image` count drops as you hydrate more images. Re-run any time.

### Step 1.4 — Audit the export

```bash
# Count
wc -l data/sft/train.jsonl data/sft/eval.jsonl

# Eyeball 3 random training examples
shuf -n 3 data/sft/train.jsonl | python -m json.tool

# Validate every line is valid JSON
python -c "
import json
ok = bad = 0
for line in open('data/sft/train.jsonl'):
    try: json.loads(line); ok += 1
    except: bad += 1
print(f'OK: {ok}  BAD: {bad}')
"

# HOLDOUT.lock should be read-only
ls -l data/sft/HOLDOUT.lock
# expected: -r--r--r--
```

If `BAD > 0`, fix the export script and re-run. Garbage in → broken adapter.

---

## Phase 2 — Smoke training on E4B (~1 hour)

**Goal**: prove the entire pipeline works end-to-end with a tiny model and 200 examples before committing to overnight runs. If this fails, you find out in an hour, not in 12 hours.

**Time**: 1–2 h.

### Step 2.1 — Confirm E4B is fully downloaded

```bash
ls -la /Users/samueltoma/AI/models/source/google--gemma-4-E4B/
# you should see model.safetensors (or shards) totalling ~16 GB
# and config.json, tokenizer.json, processor_config.json
```

If still downloading, wait. Check progress:

```bash
du -sh /Users/samueltoma/AI/models/source/google--gemma-4-E4B/
ps aux | grep "hf download" | grep -v grep
```

### Step 2.2 — Make a tiny smoke dataset

```bash
cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
mkdir -p data/sft/smoke
shuf -n 200 data/sft/train.jsonl > data/sft/smoke/train.jsonl
shuf -n 20  data/sft/eval.jsonl  > data/sft/smoke/eval.jsonl
wc -l data/sft/smoke/*
```

### Step 2.3 — Run the smoke fine-tune via mlx-vlm CLI

```bash
source /Users/samueltoma/AI/venv/bin/activate

mkdir -p data/adapters/smoke-e4b

python -m mlx_vlm.lora \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-E4B \
  --train \
  --data data/sft/smoke \
  --iters 200 \
  --batch-size 1 \
  --lora-layers 16 \
  --learning-rate 1e-4 \
  --grad-checkpoint \
  --adapter-path data/adapters/smoke-e4b \
  2>&1 | tee data/adapters/smoke-e4b/train.log
```

> `mlx_vlm.lora` flags can drift between releases. Always check `python -m mlx_vlm.lora --help` and use the printed flag names. The above is the typical 2026 set.

What you should see in the log:

```
Loading model: /Users/samueltoma/AI/models/source/google--gemma-4-E4B
...
Trainable params: 5.2M / 8000M (0.07%)
Iter 10: loss 2.81 ...
Iter 20: loss 2.43 ...
Iter 200: loss 1.94 ...
Saving adapter: data/adapters/smoke-e4b/adapters.safetensors
```

**Time**: ~30–60 min on M5 Max for 200 iters.

### Step 2.4 — Smoke-test the adapter

Quick inference with the adapter loaded:

```bash
python -m mlx_vlm.generate \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-E4B \
  --adapter-path data/adapters/smoke-e4b \
  --image data/images/<some-eval-image>.jpg \
  --prompt "Analyse this meal photo." \
  --max-tokens 512 \
  --temp 0.0
```

The output should be **valid JSON in your schema**, even if the values are imperfect (200 examples isn't enough for accuracy — you're checking *format*).

### Step 2.5 — Decide: did the smoke pass?

Pass criteria:
- ✅ training loss decreased
- ✅ adapter saved without errors
- ✅ inference with the adapter produces parseable JSON
- ✅ peak RAM usage stayed below your wired limit (`sudo memory_pressure` or Activity Monitor)

If all four ✅: proceed to Phase 3. If anything failed, see [Troubleshooting](#14-troubleshooting).

---

## Phase 3 — Backend training module

**Goal**: a FastAPI module so the existing app can launch / monitor / cancel training jobs. Subprocess-driven for isolation.

**Time**: 3–4 h.

### Step 3.1 — Schema migration

Add to `backend/db.py` (or wherever you do migrations):

```python
def _migrate_training_jobs(con):
    con.execute("""
      CREATE TABLE IF NOT EXISTS training_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL DEFAULT 'queued',
        base_model TEXT NOT NULL,
        config_json TEXT NOT NULL,
        dataset_dir TEXT NOT NULL,
        adapter_path TEXT,
        log_path TEXT,
        metrics_path TEXT,
        pid INTEGER,
        started_at REAL,
        ended_at REAL,
        error TEXT
      )
    """)
    # Allow benchmark runs to reference an adapter
    cols = {r[1] for r in con.execute("PRAGMA table_info(runs)")}
    if "adapter_path" not in cols:
        con.execute("ALTER TABLE runs ADD COLUMN adapter_path TEXT")
```

Call `_migrate_training_jobs(con)` from your existing migration entry point.

### Step 3.2 — `backend/training.py`

```python
"""Training job manager — subprocess-based for memory isolation."""
import json, os, signal, subprocess, sys, time, uuid
from pathlib import Path
import sqlite3

REPO = Path(__file__).resolve().parent.parent
ADAPTERS_DIR = REPO / "data" / "adapters"
ADAPTERS_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = REPO / "data" / "benchmark.db"

DEFAULTS = {
    "lora_rank": 16, "lora_alpha": 16, "lora_dropout": 0.05,
    "lr": 2e-4, "iters": 4000, "batch_size": 1,
    "grad_checkpoint": True, "seq_len": 1024,
    "save_every": 200, "eval_every": 500,
    "freeze_vision": True, "freeze_routed_experts": True,
}

def _con():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def list_jobs(limit=50):
    with _con() as c:
        rows = c.execute(
            "SELECT * FROM training_jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

def get_job(job_id: int):
    with _con() as c:
        r = c.execute("SELECT * FROM training_jobs WHERE id=?", (job_id,)).fetchone()
        return dict(r) if r else None

def create_job(base_model: str, dataset_dir: str, config: dict | None = None) -> int:
    cfg = {**DEFAULTS, **(config or {})}
    with _con() as c:
        cur = c.execute(
            "INSERT INTO training_jobs (status, base_model, config_json, dataset_dir) VALUES (?,?,?,?)",
            ("queued", base_model, json.dumps(cfg), dataset_dir)
        )
        job_id = cur.lastrowid
    # Allocate output paths
    job_dir = ADAPTERS_DIR / f"{job_id:04d}-{Path(base_model).name}"
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = job_dir / "train.log"
    metrics_path = job_dir / "metrics.jsonl"
    with _con() as c:
        c.execute(
            "UPDATE training_jobs SET adapter_path=?, log_path=?, metrics_path=? WHERE id=?",
            (str(job_dir), str(log_path), str(metrics_path), job_id)
        )
    return job_id

def start_job(job_id: int) -> int:
    job = get_job(job_id)
    if not job: raise ValueError(f"job {job_id} not found")
    cfg = json.loads(job["config_json"])
    cmd = [
        sys.executable, "-m", "mlx_vlm.lora",
        "--model", job["base_model"],
        "--train",
        "--data", job["dataset_dir"],
        "--iters", str(cfg["iters"]),
        "--batch-size", str(cfg["batch_size"]),
        "--lora-layers", str(cfg["lora_rank"]),
        "--learning-rate", str(cfg["lr"]),
        "--adapter-path", job["adapter_path"],
        "--save-every", str(cfg["save_every"]),
    ]
    if cfg.get("grad_checkpoint"): cmd.append("--grad-checkpoint")
    log_f = open(job["log_path"], "w", buffering=1)
    proc = subprocess.Popen(
        cmd, stdout=log_f, stderr=subprocess.STDOUT,
        cwd=str(REPO), env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    with _con() as c:
        c.execute(
            "UPDATE training_jobs SET status='running', pid=?, started_at=? WHERE id=?",
            (proc.pid, time.time(), job_id)
        )
    return proc.pid

def cancel_job(job_id: int):
    job = get_job(job_id)
    if not job or job["status"] != "running": return
    try: os.kill(job["pid"], signal.SIGTERM)
    except ProcessLookupError: pass
    with _con() as c:
        c.execute(
            "UPDATE training_jobs SET status='cancelled', ended_at=? WHERE id=?",
            (time.time(), job_id)
        )

def reap_finished():
    """Call periodically; updates status of running jobs whose pid no longer exists."""
    with _con() as c:
        rows = c.execute("SELECT id, pid FROM training_jobs WHERE status='running'").fetchall()
    for r in rows:
        pid = r["pid"]
        if pid is None: continue
        try: os.kill(pid, 0)
        except ProcessLookupError:
            # Process gone — figure out if success or fail
            job = get_job(r["id"])
            adapter_files = list(Path(job["adapter_path"]).glob("*.safetensors"))
            status = "completed" if adapter_files else "failed"
            with _con() as c:
                c.execute(
                    "UPDATE training_jobs SET status=?, ended_at=? WHERE id=?",
                    (status, time.time(), r["id"])
                )

def read_metrics(job_id: int, since: int = 0) -> list[dict]:
    job = get_job(job_id)
    if not job or not job.get("metrics_path"): return []
    p = Path(job["metrics_path"])
    if not p.exists(): return []
    out = []
    with p.open() as f:
        for line in f:
            try:
                m = json.loads(line)
                if m.get("step", 0) > since:
                    out.append(m)
            except: pass
    return out

def tail_log(job_id: int, n: int = 100) -> str:
    job = get_job(job_id)
    if not job or not job.get("log_path"): return ""
    p = Path(job["log_path"])
    if not p.exists(): return ""
    lines = p.read_text(errors="replace").splitlines()
    return "\n".join(lines[-n:])
```

### Step 3.3 — Wire endpoints in `backend/main.py`

```python
from backend import training as tr

@app.get("/api/training/jobs")
def list_training_jobs():
    tr.reap_finished()
    return {"jobs": tr.list_jobs()}

@app.post("/api/training/jobs")
async def create_training_job(payload: dict = Body(...)):
    base_model = payload["base_model"]
    dataset_dir = payload.get("dataset_dir", "data/sft")
    config = payload.get("config") or {}
    job_id = tr.create_job(base_model, dataset_dir, config)
    pid = tr.start_job(job_id)
    return {"id": job_id, "pid": pid}

@app.get("/api/training/jobs/{job_id}")
def get_training_job(job_id: int):
    tr.reap_finished()
    j = tr.get_job(job_id)
    if not j: raise HTTPException(404)
    j["log_tail"] = tr.tail_log(job_id, 200)
    return j

@app.get("/api/training/jobs/{job_id}/metrics")
def get_training_metrics(job_id: int, since: int = 0):
    return {"metrics": tr.read_metrics(job_id, since)}

@app.post("/api/training/jobs/{job_id}/cancel")
def cancel_training_job(job_id: int):
    tr.cancel_job(job_id)
    return {"ok": True}
```

### Step 3.4 — Smoke-test the API

```bash
# In one terminal: start the FastAPI app
./run.sh   # or whatever your dev script is

# In another terminal:
curl -X POST localhost:8000/api/training/jobs \
  -H 'content-type: application/json' \
  -d '{"base_model":"/Users/samueltoma/AI/models/source/google--gemma-4-E4B","dataset_dir":"data/sft/smoke","config":{"iters":50}}'
# → {"id": 1, "pid": 12345}

curl localhost:8000/api/training/jobs/1
# → status running, log_tail starting to show iters

curl localhost:8000/api/training/jobs/1/metrics
# → {"metrics": [{"step": 10, "loss": 2.4, ...}, ...]}
```

If all three commands work, the backend is wired.

---

## Phase 4 — Training tab in the frontend

**Goal**: launch jobs and watch live loss without using curl.

**Time**: 4–5 h. (UI work — the bulk of "real wall-clock", but no waiting on training.)

### Step 4.1 — Add a tab button

In `frontend/index.html`, find the existing tab nav and add:

```html
<button class="tab-btn" data-tab="train">Train</button>
```

Plus the tab content section:

```html
<section id="tab-train" class="tab hidden">
  <h2>Fine-tuning</h2>

  <details open class="card">
    <summary>New training job</summary>
    <form id="train-form">
      <label>Base model
        <select id="train-base">
          <!-- populated by JS from /api/training/models -->
        </select>
      </label>
      <label>Dataset
        <input id="train-data" value="data/sft" />
      </label>
      <details class="advanced">
        <summary>Advanced</summary>
        <label>LoRA rank <input type="number" id="train-rank" value="16"/></label>
        <label>Learning rate <input id="train-lr" value="2e-4"/></label>
        <label>Iters <input type="number" id="train-iters" value="4000"/></label>
        <label>Seq len <input type="number" id="train-seq" value="1024"/></label>
        <label><input type="checkbox" id="train-grad-ckpt" checked/> Gradient checkpointing</label>
        <label><input type="checkbox" id="train-freeze-vision" checked/> Freeze vision tower</label>
        <label><input type="checkbox" id="train-freeze-experts" checked/> Freeze routed experts (MoE)</label>
      </details>
      <button type="submit" id="train-start">Start training</button>
    </form>
  </details>

  <div id="train-active"></div>   <!-- live job card -->
  <div id="train-history"></div>  <!-- finished jobs list -->
</section>
```

### Step 4.2 — JS controller

Add to `frontend/app.js`:

```javascript
async function fetchJSON(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function trainStartJob() {
  const body = {
    base_model: document.getElementById("train-base").value,
    dataset_dir: document.getElementById("train-data").value,
    config: {
      lora_rank: +document.getElementById("train-rank").value,
      lr: +document.getElementById("train-lr").value,
      iters: +document.getElementById("train-iters").value,
      seq_len: +document.getElementById("train-seq").value,
      grad_checkpoint: document.getElementById("train-grad-ckpt").checked,
      freeze_vision: document.getElementById("train-freeze-vision").checked,
      freeze_routed_experts: document.getElementById("train-freeze-experts").checked,
    },
  };
  const { id } = await fetchJSON("/api/training/jobs", {
    method: "POST", headers: {"content-type":"application/json"},
    body: JSON.stringify(body),
  });
  trainPollJob(id);
}

async function trainPollJob(id) {
  let lastStep = 0;
  const tick = async () => {
    const job = await fetchJSON(`/api/training/jobs/${id}`);
    const { metrics } = await fetchJSON(`/api/training/jobs/${id}/metrics?since=${lastStep}`);
    if (metrics.length) lastStep = metrics[metrics.length - 1].step;
    renderActiveJob(job, metrics);
    if (job.status === "running") setTimeout(tick, 3000);
  };
  tick();
}

function renderActiveJob(job, newMetrics) {
  const el = document.getElementById("train-active");
  el.innerHTML = `
    <div class="card">
      <h3>Job #${job.id} · ${job.base_model.split("/").pop()} · ${job.status}</h3>
      <div id="train-loss-chart"></div>
      <pre class="log">${escapeHtml(job.log_tail || "")}</pre>
      ${job.status === "running" ?
        `<button onclick="cancelJob(${job.id})">Cancel</button>` : ""}
    </div>`;
  // append loss data points to a sparkline / chart of your choice
  // (use Chart.js, uPlot, or a hand-rolled SVG — design choice)
}

async function cancelJob(id) {
  await fetchJSON(`/api/training/jobs/${id}/cancel`, { method: "POST" });
}

document.getElementById("train-form").addEventListener("submit", e => {
  e.preventDefault();
  trainStartJob();
});
```

### Step 4.3 — Backend: serve the model registry

Add to `backend/main.py`:

```python
@app.get("/api/training/models")
def list_training_models():
    """List local models that are tunable on this machine."""
    bases = [
        ("/Users/samueltoma/AI/models/source/google--gemma-4-E4B", "Gemma 4 E4B (bf16) — smoke"),
        ("/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it", "Gemma 4 26B-A4B (4-bit MoE) — production"),
    ]
    out = []
    for path, label in bases:
        p = Path(path)
        if not p.exists(): continue
        # Check it has weights
        has_weights = any(p.glob("*.safetensors"))
        out.append({"path": path, "label": label, "available": has_weights})
    return {"models": out}
```

JS at the top of the Train tab loader:

```javascript
async function trainLoadModels() {
  const { models } = await fetchJSON("/api/training/models");
  const sel = document.getElementById("train-base");
  sel.innerHTML = models.map(m =>
    `<option value="${m.path}" ${m.available?"":"disabled"}>${m.label}${m.available?"":" (not downloaded)"}</option>`
  ).join("");
}
trainLoadModels();
```

---

## Phase 5 — Wire adapters into MLXProvider

**Goal**: when running a benchmark, optionally load a LoRA adapter on top of the base model. Two-line change.

**Time**: 15 min.

### Step 5.1 — Patch `backend/providers/mlx_provider.py`

Find `MLXProvider.run` where it calls `load(...)` to load the model. Right after that, add:

```python
adapter_path = (run_config or {}).get("adapter_path")
if adapter_path:
    from mlx_vlm.utils import load_adapters  # version-dependent import path
    load_adapters(model, adapter_path)
    print(f"[mlx] loaded adapter: {adapter_path}", flush=True)
```

### Step 5.2 — Surface the option in the benchmark form

In `frontend/index.html`, add a field to the existing benchmark form:

```html
<label>LoRA adapter (optional)
  <input id="r-adapter" placeholder="data/adapters/0003-google--gemma-4-26B-A4B-it"/>
</label>
```

In `frontend/app.js` `startRun()`:

```javascript
const adapter = document.getElementById("r-adapter").value.trim();
if (adapter) body.adapter_path = adapter;
```

In `backend/main.py` `RunIn` schema:

```python
class RunIn(BaseModel):
    # ... existing fields ...
    adapter_path: Optional[str] = None
```

And forward to the runner / store with the run record.

### Step 5.3 — Verify

Start a benchmark run with `adapter_path = data/adapters/smoke-e4b`. Watch for the `[mlx] loaded adapter: ...` line in the run log. Compare output JSON to a run with the same model but no adapter — outputs should differ.

---

## Phase 6 — The real run: 26B-A4B QLoRA overnight

**Goal**: train on the full ~12k–35k row training set. Overnight.

**Time**: 8–14 h wall-clock depending on dataset size.

### Step 6.1 — Pre-flight checks

```bash
# 1. Free up RAM — quit Chrome, VS Code, Slack, anything heavy
# 2. Confirm you're on AC power, plugged in, energy mode = high performance
# 3. Disable display sleep so the laptop doesn't suspend mid-training:
sudo pmset -c displaysleep 0
sudo pmset -c sleep 0
sudo pmset -c disksleep 0
# (revert with: sudo pmset -c displaysleep 10 sleep 30 disksleep 10)

# 4. Verify wired RAM ceiling
sudo sysctl iogpu.wired_limit_mb
# expect: iogpu.wired_limit_mb: 45008

# 5. Verify dataset
wc -l data/sft/train.jsonl
ls -lh data/sft/HOLDOUT.lock

# 6. Verify base model is the 4-bit MLX (NOT the bf16!)
python scripts/list_models.py | grep 26B-A4B
# the row used for training MUST say "4-bit" and "TRAINING base"
```

### Step 6.2 — Compute iters from dataset size

```bash
N=$(wc -l < data/sft/train.jsonl)
# 1 epoch at batch_size 1 ≈ N iters
# Train 1 epoch first; if loss is still falling at the end, do a second
echo "1 epoch = $N iters; aim for $N to $((N*2)) iters"
```

### Step 6.3 — Launch

Either via the UI ("Start training" button on the Train tab) or via curl:

```bash
N=$(wc -l < data/sft/train.jsonl)
curl -X POST localhost:8000/api/training/jobs \
  -H 'content-type: application/json' \
  -d "{
    \"base_model\": \"/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it\",
    \"dataset_dir\": \"data/sft\",
    \"config\": {
      \"lora_rank\": 16,
      \"lora_alpha\": 16,
      \"lr\": 2e-4,
      \"iters\": $N,
      \"batch_size\": 1,
      \"seq_len\": 1024,
      \"grad_checkpoint\": true,
      \"freeze_vision\": true,
      \"freeze_routed_experts\": true,
      \"save_every\": 500
    }
  }"
```

### Step 6.4 — Monitor

Open the Training tab, watch the loss curve. Healthy signs:

- **Loss decreasing roughly monotonically** (with noise) from ~2.5 → ~1.0–1.3 over 1 epoch
- **Tokens/sec stable** (~400–800 on M5 Max for 26B-A4B-4bit)
- **Wired RAM steady** around 28–32 GB (use Activity Monitor)
- **Periodic adapter checkpoints** in `data/adapters/<job>/` every `save_every` steps

Bad signs and what to do:

| Symptom | Cause | Fix |
|---|---|---|
| Loss plateaus at the start value | LR too low, or the prompts are masked out incorrectly | Raise LR to 5e-4, verify only assistant tokens are unmasked |
| Loss explodes (→ NaN) | LR too high, or bf16/fp16 numerical issue | Lower LR to 1e-4, ensure `--grad-checkpoint` is on |
| RAM creeps over 40 GB | Sequence too long or `enable_thinking` accidentally on | Lower `seq_len` to 768; verify dataset has no over-long rows |
| Throughput drops over time | Disk swap / page-out | Quit other apps; reboot the Mac before launch |

### Step 6.5 — Save sample outputs at checkpoints

Every 500 steps, the training tab can fire a "try 5 eval images" button. Click it occasionally — even before training finishes you'll see the model improving. This is more honest than loss alone.

---

## Phase 7 — Evaluate against the 50-image holdout

**Goal**: produce a number you can compare to base.

**Time**: 30 min.

### Step 7.1 — Run benchmark on the base (without adapter) — baseline

In the existing benchmark form:
- Provider: `MLX`
- Model: `/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it`
- Adapter: *(leave empty)*
- Dataset: same as your existing 50-image benchmark

Run. Note the composite score. This is your baseline.

### Step 7.2 — Run benchmark with the adapter

Same form, this time:
- Adapter: `data/adapters/<your-job-id>-google--gemma-4-26B-A4B-it`

Run. Note the composite score.

### Step 7.3 — Compare

Open the Compare view, select both runs, view side-by-side. Ingredients you should look at:

- **Δ name_sim** — usually the biggest mover after fine-tuning on this kind of data
- **Δ ingredient_f1** — measures whether the model learned to enumerate the right list
- **Δ macros_avg** — measures macro estimation
- **Δ overall composite** — the headline number

A good Phase-6 run typically shows +5 to +15 composite points over the un-tuned base on this style of task. If you got that, you're done.

---

## Phase 8 (optional) — Merge adapter into bf16 + re-quantize

**Goal**: ship a single fused model that doesn't need the adapter to load. Slightly higher accuracy than serving "4-bit + adapter" because the merge happens in bf16 before quantization noise is reintroduced.

**Time**: 30 min.

### Step 8.1 — Merge

```bash
python -m mlx_vlm.fuse \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-26B-A4B-it \
  --adapter-path data/adapters/<your-job-id>-google--gemma-4-26B-A4B-it \
  --save-path /Users/samueltoma/AI/models/source/gemma-4-26B-A4B-it-willma-v1 \
  --de-quantize false
```

> Flag names vary by mlx-vlm version. Check `python -m mlx_vlm.fuse --help`.

This produces a new bf16 model with the LoRA permanently baked in.

### Step 8.2 — Quantize for serving

```bash
python -m mlx_vlm.convert \
  --hf-path /Users/samueltoma/AI/models/source/gemma-4-26B-A4B-it-willma-v1 \
  --mlx-path /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-it-willma-v1-q4 \
  -q --q-bits 4 --q-group-size 64
```

### Step 8.3 — Benchmark the fused-and-quantized model

Same as Phase 7, but no adapter; just point at `gemma-4-26B-A4B-it-willma-v1-q4`. Score should be within ~0.5 points of the "4-bit + adapter" version, possibly slightly better.

### Step 8.4 — (optional) Push to LM Studio

If you serve via LM Studio:

```bash
ln -s /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-it-willma-v1-q4 \
      ~/.lmstudio/models/mlx/gemma-4-26B-A4B-it-willma-v1-q4
```

LM Studio picks up the symlink on next refresh.

---

## 14. Troubleshooting

### "Out of memory" during training

- Check wired limit: `sudo sysctl iogpu.wired_limit_mb`. Bump to 45008.
- Drop `seq_len` from 1024 → 768 → 512.
- Set `--batch-size 1` (it should already be).
- Enable `--grad-checkpoint`.
- Quit other apps. Activity Monitor → Memory → look for top consumers.

### Loss is NaN

- Lower learning rate by 10× (2e-4 → 2e-5).
- Verify your training data has no rows where `assistant_text` is empty / non-JSON.
- Try `--lora-layers 8` instead of 16 (smaller adapter, more stable).

### Adapter doesn't visibly change outputs

- Confirm adapter file exists: `ls -lh data/adapters/<job>/*.safetensors`.
- Confirm MLXProvider logged the load line: search `[mlx] loaded adapter` in your run log.
- Confirm you're loading against the *same base* you trained on (4-bit MLX 26B-A4B for a 26B-A4B adapter — adapters don't transfer across model sizes or quants).

### Benchmark scores got worse after fine-tuning

- Possible: catastrophic forgetting from over-training. Try the checkpoint at half the steps.
- Possible: WILLMA truth has systematic biases your model is now imitating. Spot-check 20 disagreeing rows by eye — does the *adapter* output match WILLMA but disagree with your visual judgement? Then it's truth-quality, not model.
- Possible: holdout leakage. Verify `data/sft/HOLDOUT.lock` against the 50 image_refs in `row_results`.

### `mlx_vlm.lora` flags don't match this guide

The CLI evolves. Always run `python -m mlx_vlm.lora --help` to see the actual flags in your installed version. The conceptual mapping (`--data` for dataset, `--adapter-path` for output, `--lora-layers` for rank) is stable; flag spellings shift.

### Disk fills up during training

Check `data/adapters/<job>/`. If it's saving every 100 steps and each checkpoint is ~80 MB, that's GB over a long run. Set `save_every: 500` or higher.

### Training is much slower than expected

- Check throughput in tokens/sec. M5 Max should hit ~600–1200 tok/s on 26B-A4B 4-bit.
- If much lower, check Activity Monitor for thermal throttling.
- Quit Spotlight indexing during training: `sudo mdutil -a -i off` (re-enable later).

---

## Appendix A — File layout reference

Tree after a successful end-to-end run:

```
/Users/samueltoma/Documents/Reletix/LLMbenchmark/
├── backend/
│   ├── main.py                      ← +5 endpoints for training
│   ├── training.py                  ← NEW
│   ├── db.py                        ← +1 migration
│   └── providers/mlx_provider.py    ← +adapter_path support
├── frontend/
│   ├── index.html                   ← +Train tab
│   └── app.js                       ← +trainStartJob/trainPollJob
├── scripts/
│   ├── list_models.py               ← NEW
│   └── export_sft_dataset.py        ← NEW
├── data/
│   ├── benchmark.db                 ← +training_jobs table; runs.adapter_path column
│   ├── images/                      ← all WILLMA images, hydrated locally
│   ├── sft/
│   │   ├── train.jsonl              ← ~12k–35k rows
│   │   ├── eval.jsonl               ← 50 rows (the holdout)
│   │   ├── manifest.json
│   │   ├── HOLDOUT.lock             ← read-only (chmod 444)
│   │   └── smoke/
│   │       ├── train.jsonl          ← 200 rows
│   │       └── eval.jsonl           ← 20 rows
│   └── adapters/
│       ├── smoke-e4b/
│       │   ├── adapters.safetensors
│       │   ├── train.log
│       │   └── metrics.jsonl
│       └── 0003-google--gemma-4-26B-A4B-it/
│           ├── adapters.safetensors
│           ├── train.log
│           └── metrics.jsonl
└── docs/
    └── FINETUNING_GEMMA4.md         ← this file

/Users/samueltoma/AI/models/
├── source/
│   ├── google--gemma-4-26B-A4B-it/         ← bf16, conversion source
│   ├── google--gemma-4-E4B/                ← bf16, smoke training
│   └── gemma-4-26B-A4B-it-willma-v1/       ← (Phase 8) bf16 with adapter merged in
└── mlx/
    ├── google--gemma-4-26B-A4B-it/         ← 4-bit, training base
    └── gemma-4-26B-A4B-it-willma-v1-q4/    ← (Phase 8) merged + re-quantized for serving
```

---

## Appendix B — Commands cheat sheet

Print this and tape it next to your monitor.

```bash
# ─── ENV ───
source /Users/samueltoma/AI/venv/bin/activate
cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
export LLMBENCH_IMAGES_ONLY_LOCAL=1

# ─── INVENTORY ───
python scripts/list_models.py

# ─── DATASET (re-runnable) ───
python scripts/export_sft_dataset.py
wc -l data/sft/train.jsonl data/sft/eval.jsonl

# ─── SMOKE TRAIN (E4B, 200 examples, ~30 min) ───
mkdir -p data/sft/smoke data/adapters/smoke-e4b
shuf -n 200 data/sft/train.jsonl > data/sft/smoke/train.jsonl
shuf -n 20  data/sft/eval.jsonl  > data/sft/smoke/eval.jsonl
python -m mlx_vlm.lora \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-E4B \
  --train --data data/sft/smoke \
  --iters 200 --batch-size 1 --lora-layers 16 \
  --learning-rate 1e-4 --grad-checkpoint \
  --adapter-path data/adapters/smoke-e4b \
  2>&1 | tee data/adapters/smoke-e4b/train.log

# ─── SMOKE INFER ───
python -m mlx_vlm.generate \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-E4B \
  --adapter-path data/adapters/smoke-e4b \
  --image data/images/<somefile>.jpg \
  --prompt "Analyse this meal photo." --max-tokens 512 --temp 0.0

# ─── PRODUCTION TRAIN (26B-A4B, overnight) ───
N=$(wc -l < data/sft/train.jsonl)
sudo pmset -c displaysleep 0 sleep 0 disksleep 0
python -m mlx_vlm.lora \
  --model /Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it \
  --train --data data/sft \
  --iters $N --batch-size 1 --lora-layers 16 \
  --learning-rate 2e-4 --grad-checkpoint \
  --adapter-path data/adapters/willma-v1 \
  --save-every 500 \
  2>&1 | tee data/adapters/willma-v1/train.log
sudo pmset -c displaysleep 10 sleep 30 disksleep 10  # restore

# ─── BENCHMARK WITH ADAPTER ───
# (use the UI; adapter_path = data/adapters/willma-v1)

# ─── (OPTIONAL) MERGE + REQUANT ───
python -m mlx_vlm.fuse \
  --model /Users/samueltoma/AI/models/source/google--gemma-4-26B-A4B-it \
  --adapter-path data/adapters/willma-v1 \
  --save-path /Users/samueltoma/AI/models/source/gemma-4-26B-A4B-it-willma-v1
python -m mlx_vlm.convert \
  --hf-path /Users/samueltoma/AI/models/source/gemma-4-26B-A4B-it-willma-v1 \
  --mlx-path /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-it-willma-v1-q4 \
  -q --q-bits 4 --q-group-size 64

# ─── MONITORING DURING ANY TRAIN ───
# Loss / progress
tail -f data/adapters/<job>/train.log
# Memory pressure
sudo memory_pressure   # or Activity Monitor
# Cancel
kill -TERM $(pgrep -f "mlx_vlm.lora")
```

---

**Document version**: 1.0 — May 2026
**Maintainer**: Reletix LLMbenchmark
**Repo**: `/Users/samueltoma/Documents/Reletix/LLMbenchmark`

If you find a step that doesn't match reality (a CLI flag changed, a path differs), update this file. The whole point is that anyone — including future-you who's forgotten the context — can run it cold.
