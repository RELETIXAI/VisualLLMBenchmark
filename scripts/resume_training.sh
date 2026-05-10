#!/bin/bash
# Resume 26B-A4B training from step-500 checkpoint after reboot.
# Run from inside the repo: bash scripts/resume_training.sh

set -e
REPO="/Users/samueltoma/Documents/Reletix/LLMbenchmark"
VENV="$REPO/.venv/bin/python3"
MODEL="/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it"
ADAPTER_OUT="$REPO/data/adapters/prod-26b-a4b-ep1"
LOG="$ADAPTER_OUT/train.log"

echo "=== Pre-flight ==="
echo "Checkpoint dir: $ADAPTER_OUT"
echo "adapters.safetensors: $(ls -lh $ADAPTER_OUT/adapters.safetensors | awk '{print $5, $9}')"
echo "Remaining iters: 34439  (34939 total − 500 done)"
echo ""

# Rebuild clean dataset dir (symlink survives reboot only if /tmp persists;
# on macOS /tmp is cleared on reboot, so we always recreate it)
mkdir -p /tmp/llmbench_train_only
ln -sf "$REPO/data/sft/train.jsonl" /tmp/llmbench_train_only/train.jsonl
echo "Dataset symlink: $(ls -la /tmp/llmbench_train_only/train.jsonl)"
echo ""

# Archive the old log so metrics chart starts clean for this run.
if [ -f "$LOG" ]; then
  mv "$LOG" "${LOG%.log}-prev-$(date +%Y%m%d-%H%M%S).log"
  echo "Old log archived."
fi

echo "=== Launching (nohup) ==="
nohup "$VENV" -m mlx_vlm.lora \
  --model-path  "$MODEL" \
  --dataset     /tmp/llmbench_train_only \
  --split       train \
  --adapter-path "$ADAPTER_OUT" \
  --iters       34439 \
  --batch-size  1 \
  --lora-rank   16 \
  --lora-alpha  16 \
  --learning-rate 2e-4 \
  --grad-checkpoint \
  --train-on-completions \
  --steps-per-report 50 \
  --steps-per-save   500 \
  --output-path "$ADAPTER_OUT" \
  >> "$LOG" 2>&1 &

PID=$!
echo "PID: $PID"
echo "Log: $LOG"

# Register the new job in the benchmark DB so the Training tab can see it
"$VENV" - << PYEOF
import sys, json, time
sys.path.insert(0, "$REPO")
from backend import training as tr

cfg = {
    "lora_rank": 16, "lora_alpha": 16, "lora_dropout": 0.0,
    "lr": 2e-4, "iters": 34939, "batch_size": 1,
    "grad_checkpoint": True, "seq_len": 1024,
    "save_every": 500,
    "resumed_from_step": 500,
}
con = tr._con()
with con:
    cur = con.execute(
        """INSERT INTO training_jobs
           (status, pid, base_model, config_json, dataset_dir,
            adapter_path, log_path, metrics_path, started_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        ("running", $PID,
         "$MODEL",
         json.dumps(cfg),
         "$REPO/data/sft",
         "data/adapters/prod-26b-a4b-ep1",
         "data/adapters/prod-26b-a4b-ep1/train.log",
         None, time.time())
    )
    print(f"Registered job id={cur.lastrowid} pid=$PID")
PYEOF

echo ""
echo "Monitor: tail -f $LOG"
echo "Dashboard: http://127.0.0.1:8765/#train  (after starting uvicorn)"
