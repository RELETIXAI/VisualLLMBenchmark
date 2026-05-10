#!/bin/bash
# Usage: ./scripts/monitor_training.sh [log_path]
LOG="${1:-/Users/samueltoma/Documents/Reletix/LLMbenchmark/data/adapters/prod-26b-a4b-ep1/train.log}"
TOTAL_ITERS=34939

echo "=== Training monitor ==="
echo "Log: $LOG"
echo ""

if [ ! -f "$LOG" ]; then
  echo "Log file not found."
  exit 1
fi

# Find process
PID=$(pgrep -f "mlx_vlm.lora.*prod-26b" | head -1)
if [ -n "$PID" ]; then
  echo "Process: running (PID $PID)"
else
  echo "Process: NOT running"
fi

# Last iter report
LAST_ITER=$(grep "^Iter " "$LOG" | tail -1)
if [ -n "$LAST_ITER" ]; then
  echo "Last iter: $LAST_ITER"
  ITER_NUM=$(echo "$LAST_ITER" | grep -o "^Iter [0-9]*" | grep -o "[0-9]*")
  PCT=$(echo "scale=1; $ITER_NUM * 100 / $TOTAL_ITERS" | bc 2>/dev/null || echo "?")
  echo "Progress: $ITER_NUM / $TOTAL_ITERS ($PCT%)"
else
  echo "No iter reports yet (still loading model)"
fi

echo ""
echo "Checkpoints saved:"
ls /Users/samueltoma/Documents/Reletix/LLMbenchmark/data/adapters/prod-26b-a4b-ep1/*.safetensors 2>/dev/null | while read f; do
  echo "  $(ls -lh "$f" | awk '{print $5, $9}')"
done

echo ""
echo "Last 5 lines of log:"
tail -5 "$LOG"
