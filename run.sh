#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PORT="${PORT:-8765}"
exec .venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port "$PORT" --reload
