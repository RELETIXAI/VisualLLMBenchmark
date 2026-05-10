#!/bin/bash
# Start uvicorn after reboot.
# Run from inside the repo: bash scripts/start_server.sh

REPO="/Users/samueltoma/Documents/Reletix/LLMbenchmark"
cd "$REPO"

echo "Starting uvicorn on http://127.0.0.1:8765"
.venv/bin/uvicorn backend.main:app \
  --host 127.0.0.1 \
  --port 8765 \
  --reload
