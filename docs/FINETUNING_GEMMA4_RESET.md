# Reletix LLMbenchmark — Gemma-4 fine-tuning RESET plan

This document supersedes `docs/FINETUNING_GEMMA4.md` for the second
attempt at fine-tuning. The first attempt (Job #6, E4B, 4000 iters)
trained but produced a broken LoRA — repetition collapse + schema
regurgitation at inference. This plan starts over with cleaner data
and four upstream fixes incorporated as lessons.

## Mission

Fine-tune a Gemma-4 vision-language model to analyse food photos and
emit strict JSON (dish + ingredients + macros + A-E health grade).

The user is producing **cleaned training data in a separate
workstream**. That output is the only external blocker. Everything
else can proceed.

## Hardware / environment

- Mac M5 Max, 40 GPU cores, 48 GB unified RAM, `iogpu.wired_limit_mb: 45008`
- Repo: `/Users/samueltoma/Documents/Reletix/LLMbenchmark/`
- venv: `/Users/samueltoma/Documents/Reletix/LLMbenchmark/.venv`
- Server: `./run.sh` → `uvicorn backend.main:app --host 127.0.0.1 --port 8765 --reload`
- Models live in `/Users/samueltoma/AI/models/{source,mlx}/`
- Data: `/Users/samueltoma/Documents/Reletix/LLMbenchmark/data/`
- Hydrated images: `data/images/6/<image_id>.jpg`
- `LLMBENCH_IMAGES_ONLY_LOCAL=1` should be set so no S3 fetches happen during training.

Background doc: `docs/FINETUNING_GEMMA4.md` (1437 lines) — the original
plan. Read it but treat it as authoritative only where this plan
agrees. Where they disagree, this plan wins (it incorporates lessons
learned).

## What we keep (verified working)

| Component | Status |
|---|---|
| `backend/db.py` migration: `training_jobs` + `runs.adapter_path` | ✅ live |
| `backend/training.py` (subprocess job manager + reaper + 6 endpoints) | ✅ live |
| `backend/main.py` `/api/training/*` endpoints + `RunIn.adapter_path` | ✅ live |
| `backend/providers/mlx_provider.py` adapter loading + system-prompt fix | ✅ live |
| Frontend Train tab (`frontend/index.html` + `frontend/app.js`) | ✅ live |
| MLX model scanner extended to `~/AI/models/source/` | ✅ live |
| `scripts/list_models.py` (model inventory + tunability) | ✅ live |
| `scripts/export_sft_dataset.py` (WILLMA xlsx → train/eval JSONL + HOLDOUT.lock) | ✅ live, but needs adaptation to user's new cleaned data format |
| `scripts/fuse_adapter.py` (LoRA → merged base, manual W_merged math because mlx-vlm 0.5 has no fuse CLI) | ✅ live |
| `scripts/diagnose_scoring.py` | ✅ live |
| 64-image holdout in `data/sft/HOLDOUT.lock` (chmod 444, sha1 `a967c847…`) | ✅ locked |

## What we throw out (broken artefacts to delete)

```bash
# ~30 GB of bad outputs from Job #6
rm -rf /Users/samueltoma/Documents/Reletix/LLMbenchmark/data/adapters/0006-google--gemma-4-E4B
rm -rf /Users/samueltoma/AI/models/source/google--gemma-4-E4B-merged
rm -rf /Users/samueltoma/AI/models/mlx/gemma-4-E4B-ft-q4
```

`data/sft/HOLDOUT.lock` and `data/sft/manifest.json` — KEEP.
`data/sft/train.jsonl` and `data/sft/eval.jsonl` — will be overwritten
by re-export from cleaned data.

Also clean up DB rows for the failed/cancelled MLX runs (#107, #108,
#109) and Job #6 if desired — optional.

## Hard lessons / gotchas (READ THESE)

These will trip you up if you don't know them.

1. **mlx-vlm 0.5.0 CLI flag mapping** (changed from older versions):
   - `--model-path` (not `--model`)
   - `--dataset` (not `--data` / `--train`)
   - `--split train` (default)
   - `--lora-rank` (not `--lora-layers`)
   - `--lora-alpha`, `--lora-dropout` exist but only at training startup
   - `--output-path` (not `--adapter-path`; the latter means *resume from*)
   - `--train-on-completions` (masks prompt tokens from loss — IMPORTANT, leave on)
   - `--steps-per-report 20`, `--steps-per-save 200`
   - `--grad-checkpoint` (no value)
   - **No** `--val-dataset` flag in 0.5.0 train CLI as of last check.
     **Verify** by running `python -m mlx_vlm.lora --help` before
     assuming. If absent, validation requires patching
     `backend/training.py::_build_command`.

2. **HuggingFace dataset loader chokes on extra files** in the dataset
   dir. mlx_vlm.lora calls `load_dataset(path, split="train")` which
   reads ALL `*.jsonl` and `*.json` files. If `manifest.json` and
   `eval.jsonl` are alongside `train.jsonl`, schema mismatch → crash.
   Current workaround in `backend/training.py::_prepare_clean_dataset_dir`:
   symlinks ONLY `train.jsonl` into `/tmp/llmbench_train_only/`.
   **Extend this** to support a parallel `/tmp/llmbench_eval_only/`
   with eval.jsonl when (and if) we add validation.

3. **mlx-vlm 0.5.0 has NO `fuse` CLI.** Use `scripts/fuse_adapter.py`
   (already in the repo). The
   `mlx_vlm.trainer.lora.replace_lora_with_linear` function is BROKEN
   (only iterates model.layers, missing nested modules; uses wrong
   matmul order). Don't rely on it.

4. **Correct LoRA merge math** (Gemma 4 dense E4B, our LoRaLayer class):
   - `LoRaLayer.A` shape: `(input_dims, rank)` (initialized random uniform)
   - `LoRaLayer.B` shape: `(rank, output_dims)` (initialized zeros)
   - Forward: `y = original_layer(x) + scale * (x @ A @ B)`
   - For nn.Linear `y = x @ W.T + b` with W shape `(out, in)`:
   - **`W_merged = W + scale * (A @ B).T`**, cast back to `W.dtype`
     to keep bf16 → bf16 (otherwise float32 promotion 2× the merged
     file size for no benefit).
   - This is what `scripts/fuse_adapter.py` does. Do not regress to
     the broken upstream version.

5. **`mlx_vlm convert` flag**: use `-q --q-bits 4`, NOT `-q-bits 4`
   (the `-q` is a no-arg flag).

6. **Training subprocess orphan cleanup**: when `mlx_vlm.lora` exits
   *naturally* (not cancelled), its HuggingFace DataLoader workers
   spawned via `multiprocessing.spawn` can survive the leader, holding
   Metal/MLX state and starving subsequent benchmark runs.
   `backend/training.py::reap_finished` now defensively
   `os.killpg(pid, SIGTERM)` when the leader pid is gone. Don't
   remove that.

7. **MLXProvider system prompt fix** (already shipped). When passing
   prompts via `apply_chat_template`, build a `[{role:system},
   {role:user}]` messages list — don't pass `user_prompt` as a bare
   string. The trainer reads train.jsonl with system role, so
   inference must match. See the patched code in
   `backend/providers/mlx_provider.py` around line 296.

8. **Holdout enforcement**: `data/sft/HOLDOUT.lock` is read-only
   (chmod 444) with sha1 of the 64 image ids. Any re-export of SFT
   data must verify this file's sha1 doesn't change OR rebuild it
   from `row_results.image_ref` in `data/benchmark.db`. Train.jsonl
   must NEVER contain a holdout image id.

9. **bf16 vs 4-bit inference performance**:
   - bf16 8B dense: ~275 sec/row (memory bandwidth bound — must read
     16 GB/token)
   - 4-bit dense 8B: ~7-10 sec/row
   - 4-bit MoE 26B-A4B: ~7-10 sec/row (only ~3.8B active params)
   - **Always benchmark on the 4-bit (post-fuse, post-quantize)
     variant**, never the bf16 source.

10. **The 26B-A4B is the production target**, not E4B. E4B is for
    smoke training. The original adapter
    `data/adapters/prod-26b-a4b-ep1` (Job #2-5) achieved 72.85% on
    run #106 — that's the bar to beat.

11. **Repetition collapse / EOS handling**: the previous E4B
    fine-tune produced degenerate output (`{"name": "tomato, raw",
    "grams": 10.0}` × 23). Two suspected root causes (verify before
    retraining):
    - mlx_vlm.lora may not insert an EOS token at end of each
      assistant message in the trainer's tokenized stream → model
      never learns to stop.
    - Heavy ingredient frequency bias in training data (`rice, white,
      cooked` 10%, `tomato, raw` 6.5%, `cucumber, raw` 6.4%) →
      autoregressive distribution gets stuck in local minima.
    - Diagnose by checking the trainer's tokenisation: does the
      encoded `train.jsonl` row end with the model's EOS token id? If
      not, append `<end_of_turn>` (Gemma 4) to each assistant content
      during export.

12. **Data quality issues** found in the previous training set (need
    to be fixed in cleaning):
    - **Mojibake**: `b√©chamel` should be `béchamel`, `d√∂ner` should
      be `döner`. UTF-8-as-MacRoman corruption. Fully reversible.
    - **Duplicated ingredient lines** within a single row (e.g. eval
      row 41: zubaidi fish + machboos listed twice).
    - **Dish-name vs ingredient mismatch** (e.g. eval row 56: dish is
      "oats with milk" but ingredients only list milk).
    - **Implausible portions**: e.g. eval row 54 lists 500g dry oats
      = 1725 kcal (technically correct, dietary implausible).
    - **Generic dish names** like "food", "snack", "meal" with
      non-specific ingredients.

## The plan — six steps

### Step 1 — Cleanup (5 min)

Confirm with user before each `rm`:
```bash
cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
rm -rf data/adapters/0006-google--gemma-4-E4B
rm -rf /Users/samueltoma/AI/models/source/google--gemma-4-E4B-merged
rm -rf /Users/samueltoma/AI/models/mlx/gemma-4-E4B-ft-q4

# Optional: clean up DB rows for failed runs
.venv/bin/python -c "
import sqlite3
con = sqlite3.connect('data/benchmark.db')
con.execute(\"DELETE FROM row_results WHERE run_id IN (107,108,109)\")
con.execute(\"DELETE FROM runs WHERE id IN (107,108,109)\")
con.execute(\"DELETE FROM training_jobs WHERE id=6\")
con.commit()
print('cleaned')
"
```

Verify: `du -sh data/adapters/ /Users/samueltoma/AI/models/source/google--gemma-4-E4B*` should show only the bf16 base remaining.

### Step 2 — Pre-training code fixes (1-2 h)

Apply these BEFORE the next training run. Each is small and surgical.

**2.1 — Lower `max_tokens` default in MLX provider**

File: `backend/providers/mlx_provider.py`, in the gen_kwargs
construction (around line 320-325 — search for `"max_tokens"`):
```python
"max_tokens": int(gp.get("max_tokens") or 1024),  # was 8192
```
Reason: when a model goes degenerate, max_tokens caps blast radius.
1024 is plenty for a single-meal JSON response.

**2.2 — Wire `--val-dataset` into trainer**

First verify mlx_vlm.lora 0.5.0 supports a val-set flag:
```bash
.venv/bin/python -m mlx_vlm.lora --help 2>&1 | grep -iE "val|eval"
```

If it does, extend `backend/training.py::_prepare_clean_dataset_dir`
to also create `/tmp/llmbench_eval_only/` containing only
`eval.jsonl`, and extend `_build_command` to pass
`--val-dataset /tmp/llmbench_eval_only` (or whatever the actual flag
is).

If it doesn't, this step is a no-op for now — note it for the user.
We'll early-stop manually by benchmarking each checkpoint.

**2.3 — Investigate EOS handling in mlx_vlm.lora**

This is the crucial one for fixing the repetition collapse. Two
probes:

```bash
# Probe 1: read the trainer's tokenize step
grep -n "EOS\|eos_token\|end_of_turn\|add_special_tokens" \
  .venv/lib/python3.13/site-packages/mlx_vlm/trainer/datasets.py \
  .venv/lib/python3.13/site-packages/mlx_vlm/trainer/lora.py \
  .venv/lib/python3.13/site-packages/mlx_vlm/lora.py
```

```bash
# Probe 2: tokenize one training sample manually and check if last token is EOS
.venv/bin/python <<'PY'
from mlx_vlm.utils import load_processor, load_config
from mlx_vlm.prompt_utils import apply_chat_template
import json
mp = '/Users/samueltoma/AI/models/source/google--gemma-4-E4B'
processor = load_processor(mp)
config = load_config(mp)

with open('data/sft/train.jsonl') as f:
    obj = json.loads(f.readline())
msgs = obj['messages']
prompt = apply_chat_template(processor, config, msgs, num_images=1, add_generation_prompt=False)
ids = processor.tokenizer.encode(prompt)
eos = processor.tokenizer.eos_token_id
end_of_turn = processor.tokenizer.encode('<end_of_turn>', add_special_tokens=False)
print(f'last 5 token ids: {ids[-5:]}')
print(f'eos_token_id: {eos}')
print(f'<end_of_turn> ids: {end_of_turn}')
print(f'last token decoded: {processor.tokenizer.decode([ids[-1]])!r}')
PY
```

**Decision tree:**
- If last token IS `<end_of_turn>` or EOS → mlx_vlm.lora is doing the
  right thing; the repetition collapse is data/training-duration
  related. Move to step 2.4.
- If last token is something else (the closing `}` of JSON, for
  example) → the model never sees the stop signal. Fix: in
  `scripts/export_sft_dataset.py`, append `<end_of_turn>` to every
  assistant content string. Re-export.

**2.4 — Reduce iters cap in `backend/training.py::DEFAULTS`**

```python
DEFAULTS = {
    "lora_rank": 16,
    "lora_alpha": 16,
    "lora_dropout": 0.05,
    "lr": 2e-4,
    "iters": 1500,            # was 4000 — previous run plateaued at step 600
    "batch_size": 1,
    "grad_checkpoint": True,
    "seq_len": 1024,
    "save_every": 100,        # was 200 — finer-grained checkpoints for early-stopping
    "eval_every": 500,
    "freeze_vision": True,
    "freeze_routed_experts": True,
}
```

Reason: previous run's loss plateaued at ~0.30 by step 600. Going to
4000 added no signal and likely amplified overfit-to-noise.

### Step 3 — Re-export SFT data from user's cleaned source

**Blocker**: needs the user's cleaned data from their other thread.
Coordinate before starting.

Once received:
1. Inspect the cleaned data's column names + format. Adjust
   `scripts/export_sft_dataset.py::load_willma_rows` and
   `build_assistant_text` to match. The current script targets the
   WILLMA xlsx schema — if the user's cleaned output is different,
   adapt.
2. Verify `HOLDOUT.lock`'s sha1 won't change after re-export. The
   script reads holdout from `data/benchmark.db::row_results.image_ref`
   so as long as that table doesn't change, the lock is stable.
3. Run:
   ```bash
   cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
   .venv/bin/python scripts/export_sft_dataset.py
   ```
4. Audit the output:
   ```bash
   .venv/bin/python <<'PY'
   import json, collections
   ing = collections.Counter()
   schemas_leaked = 0
   total = 0
   with open('data/sft/train.jsonl') as f:
       for line in f:
           total += 1
           obj = json.loads(line)
           for m in obj['messages']:
               if m['role'] == 'assistant':
                   txt = m['content'] if isinstance(m['content'], str) else ''
                   if '"name": str' in txt or '"kcal": number' in txt:
                       schemas_leaked += 1
                   try:
                       o = json.loads(txt)
                       for i in (o.get('ingredients') or []):
                           ing[i.get('name','').lower()] += 1
                   except: pass
   print(f'rows: {total}')
   print(f'schema leaks: {schemas_leaked} (must be 0)')
   print(f'top 10 ingredients:')
   for n,c in ing.most_common(10):
       print(f'  {c:5d} ({100*c/total:.1f}%)  {n}')
   PY
   ```
5. Verify holdout integrity (no train ⊆ holdout):
   ```bash
   .venv/bin/python <<'PY'
   import json, pathlib
   lock = json.loads(pathlib.Path('data/sft/HOLDOUT.lock').read_text())
   hold = set(lock['image_ids'])
   for line in open('data/sft/train.jsonl'):
       o = json.loads(line)
       img = o.get('images',[None])[0] or ''
       iid = pathlib.Path(img).stem
       assert iid not in hold, f'LEAK: {iid}'
   print('OK — no train images in holdout')
   PY
   ```

Acceptance criteria for the new `train.jsonl`:
- 0 schema leaks (regex `"name": str` or `"kcal": number` count).
- Top ingredient frequency < 8% (currently `rice, white, cooked` was 10.4%).
- 0 mojibake patterns (`re.search(r'√[©™≠ó∫°]')` count = 0).
- 0 duplicate ingredient lines within a single row.
- Holdout invariant: 0 train rows have an image_id in HOLDOUT.lock.

If any criterion fails, push back to the cleaning thread. Don't train
on bad data.

### Step 4 — Smoke training (~30 min)

Goal: validate the whole pipeline end-to-end before the long
production run.

```bash
# Use micro E4B with 200 iters — finishes in ~10-15 min
curl -X POST http://127.0.0.1:8765/api/training/jobs \
  -H 'content-type: application/json' \
  -d '{
    "base_model": "/Users/samueltoma/AI/models/source/google--gemma-4-E4B",
    "dataset_dir": "data/sft",
    "config": {"iters": 200, "save_every": 50, "lora_rank": 8, "batch_size": 1}
  }'
```

While it runs, watch the Train tab in the UI (live loss curve + log
tail).

After it finishes:
```bash
# Find the new adapter dir, e.g. data/adapters/0007-google--gemma-4-E4B
LATEST=$(ls -td data/adapters/*-google--gemma-4-E4B | head -1)
echo "Smoke adapter: $LATEST"

# Quick coherence test on 3 holdout images
.venv/bin/python <<PY
import json, time
from mlx_vlm import load, generate
from mlx_vlm.prompt_utils import apply_chat_template
from mlx_vlm.utils import load_config
from mlx_vlm.trainer.utils import apply_lora_layers

mp = '/Users/samueltoma/AI/models/source/google--gemma-4-E4B'
print('loading base...')
model, processor = load(mp)
config = load_config(mp)
print('loading adapter...')
model = apply_lora_layers(model, '$LATEST')

with open('data/sft/eval.jsonl') as f:
    eval_rows = [json.loads(l) for l in f]

SYSTEM = '...'  # paste from train.jsonl row 0
USER = 'Analyse this meal photo.'
for row in eval_rows[:3]:
    img = row['images'][0]
    msgs = [{'role':'system','content':SYSTEM},{'role':'user','content':USER}]
    prompt = apply_chat_template(processor, config, msgs, num_images=1)
    out = generate(model, processor, image=img, prompt=prompt, max_tokens=600, temperature=0.0, verbose=False)
    text = out if isinstance(out,str) else getattr(out,'text',str(out))
    print(f'\n--- {img.split("/")[-1]} ---')
    print(text[:800])
    try:
        json.loads(text); print('VALID JSON ✓')
    except: print('INVALID JSON ✗')
PY
```

Acceptance criteria for smoke:
- All 3 outputs are valid JSON.
- No repetition collapse (no ingredient appears more than 3× in one row).
- Output stops on its own (doesn't hit max_tokens=600).
- Dish names roughly match the truth.

If smoke passes → proceed to Step 5. If smoke fails:
- Revisit Step 2.3 (EOS handling) — most likely root cause.
- Or revisit data audit in Step 3 — unbalanced data could still cause collapse.
- Don't skip ahead to production training. Fix smoke first.

### Step 5 — Production training (~8-14 h overnight)

Target: 26B-A4B 4-bit MoE base, full cleaned dataset, with the four
upstream fixes in place.

```bash
# Pre-flight (do BEFORE starting training):
sudo pmset -c displaysleep 0 sleep 0 disksleep 0    # prevent sleep
# Quit Chrome / Slack / non-essential apps to free unified RAM.

# Start the production run:
curl -X POST http://127.0.0.1:8765/api/training/jobs \
  -H 'content-type: application/json' \
  -d '{
    "base_model": "/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it",
    "dataset_dir": "data/sft",
    "config": {
      "iters": 1500,
      "save_every": 100,
      "lora_rank": 16,
      "lora_alpha": 16,
      "lr": 2e-4,
      "batch_size": 1,
      "grad_checkpoint": true,
      "freeze_vision": true,
      "freeze_routed_experts": true
    }
  }'
```

Watch via the Train tab. Expected:
- ~1.0-1.4 it/s on 26B-A4B 4-bit
- Peak memory ~28-32 GB (well under 45 GB ceiling)
- Loss should drop from ~3-5 to ~0.4-0.6 by step 800-1000, then plateau
- Adapter checkpoints saved every 100 steps → ~15 checkpoints total

If memory pressure: reduce batch_size to 1 (already), or `seq_len`
to 768.
If loss diverges or NaN: lower `lr` to 1e-4, restart.

### Step 6 — Benchmark, fuse, quantize, deploy

For each saved checkpoint (or just the final one if you trust it):

**6.1** Benchmark adapter directly on the 64 holdout images:
- Run benchmark form: provider=MLX, model=base bf16 source,
  adapter_path=`data/adapters/<job>/<step>_adapters.safetensors`'s
  parent dir.
- Compare accuracy across checkpoints. Pick the highest-scoring one.

**6.2** Fuse the winning adapter into the base:
```bash
.venv/bin/python scripts/fuse_adapter.py \
  --model /Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it \
  --adapter-path data/adapters/<winning-job-dir> \
  --save-path /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-ft-merged
```

Note: fusing onto a 4-bit quantized base requires extending
`scripts/fuse_adapter.py::_merge_lora_layer` to handle
`nn.QuantizedLinear` (currently raises `NotImplementedError`). The
merge math becomes: dequantize → add LoRA delta → re-quantize.
Reference: `mlx_lm.fuse` source at
`.venv/lib/python3.13/site-packages/mlx_lm/fuse.py` shows the
pattern.

If extending fuse_adapter.py is too complex, alternative: train a
second time against the **bf16 source** (16 GB), fuse into bf16,
then quantize. Slower training (bf16) but simpler post-processing.

**6.3** Quantize the fused model (if step 6.2 was on bf16 base):
```bash
.venv/bin/python -m mlx_vlm convert \
  --hf-path /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-ft-merged \
  --mlx-path /Users/samueltoma/AI/models/mlx/gemma-4-26B-A4B-ft-q4 \
  -q --q-bits 4
```

**6.4** Final benchmark on the q4 fused model. Compare to:
- Base 26B-A4B (no fine-tune) — that's run #106 with 72.85% accuracy
- The previous prod-26b-a4b-ep1 adapter (if you still have it)
- Cloud baselines (gpt-5.4-mini, claude, gemini) on the same 64 rows

## Files reference (for the next agent)

```
backend/
├── db.py                       # SQLite schema + migrations (training_jobs added)
├── image_cache.py              # has LLMBENCH_IMAGES_ONLY_LOCAL guard
├── main.py                     # FastAPI; /api/training/* endpoints near end
├── runner.py                   # Benchmark thread; reads gen_params['adapter_path']
├── scoring.py                  # Per-row + composite scoring rules
├── llm_judge.py                # LLM-as-judge for ingredient_f1 (cached)
├── training.py                 # Subprocess job manager + reaper
└── providers/
    ├── base.py                 # BaseProvider; ProviderResult
    └── mlx_provider.py         # FIX #1 LIVES HERE (system-prompt plumbing)

scripts/
├── list_models.py              # Tunability inventory
├── export_sft_dataset.py       # WILLMA → train.jsonl (re-runnable)
├── fuse_adapter.py             # LoRA → merged base (manual W_merged math)
└── diagnose_scoring.py         # name_sim outlier diagnostic

frontend/
├── index.html                  # Train tab section
└── app.js                      # trainStartJob/trainPollJob/trainCancelJob/etc.

data/
├── benchmark.db                # SQLite — runs, row_results, training_jobs, holdout source
├── images/6/<id>.jpg           # Hydrated WILLMA images
├── sft/
│   ├── HOLDOUT.lock            # READ-ONLY (chmod 444), 64 image_ids, sha1 a967c847…
│   ├── manifest.json
│   ├── train.jsonl             # Will be regenerated from cleaned data
│   └── eval.jsonl
└── adapters/<id>-<model>/      # Output of training jobs

docs/
├── FINETUNING_GEMMA4.md        # Original plan (1437 lines) — read but supersede with this doc
└── FINETUNING_GEMMA4_RESET.md  # THIS DOCUMENT

/Users/samueltoma/AI/models/
├── source/                     # bf16 HuggingFace sources (16 GB E4B, 51 GB 26B)
└── mlx/                        # 4-bit/8-bit MLX-format models (15 GB 26B-A4B)
```

## Quick-reference commands

```bash
# Server lifecycle
./run.sh                                                   # start dev server
curl http://127.0.0.1:8765/api/health                      # health check
ps -ef | grep multiprocessing-fork | grep -v grep          # find orphan workers

# Training
curl -s http://127.0.0.1:8765/api/training/models | jq    # list bases
curl -s http://127.0.0.1:8765/api/training/jobs | jq      # list jobs
curl -X POST http://127.0.0.1:8765/api/training/jobs/<id>/cancel

# Benchmark
curl -s "http://127.0.0.1:8765/api/runs?status=running" | jq
curl -X POST http://127.0.0.1:8765/api/runs/<id>/cancel

# Inventory
.venv/bin/python scripts/list_models.py
```

## Definition of done

The reset is complete when:

1. ✅ Disk freed of broken artefacts (Step 1).
2. ✅ All four pre-training fixes applied (Step 2.1–2.4) and verified
   by re-imports / unit-style checks.
3. ✅ User's cleaned data exported to `data/sft/{train,eval}.jsonl`
   and audit script shows 0 schema leaks, 0 mojibake, 0 holdout
   leaks, ingredient frequencies < 8% (Step 3).
4. ✅ Smoke training (200 iters E4B) produces 3/3 valid JSON outputs
   that stop on their own (Step 4).
5. ✅ Production run (1500 iters 26B-A4B) completes with eval-loss
   not diverging from train-loss (or, if no validation, with stable
   train-loss plateau).
6. ✅ Best checkpoint scores ≥ 75% on the 64 holdout (improvement
   over base's 72.85%).
7. ✅ Fused + quantized q4 model performs equivalently to
   base+adapter (no degradation from quantization).

If any step fails, fix root cause before advancing. Do not band-aid.
