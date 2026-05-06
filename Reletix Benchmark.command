#!/usr/bin/env bash
# Double-clickable launcher for the Reletix Visual LLM Benchmark.
# - Kills any previously-running instance of THIS project's server.
# - Releases the port if some other process is sitting on it.
# - Starts uvicorn fresh and opens the browser.
# - Closing this Terminal window stops the server cleanly.

set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8765}"
PROJECT_TAG="backend.main"

cat <<EOF
═══════════════════════════════════════════════════════════
  Reletix · Visual LLM Benchmark
═══════════════════════════════════════════════════════════
  Repo:  $(pwd)
  Port:  $PORT
═══════════════════════════════════════════════════════════
EOF
echo

# 1. Stop any previous instance of THIS project (matches by module path,
#    so other unrelated uvicorns on the machine are not touched).
EXISTING=$(pgrep -f "uvicorn $PROJECT_TAG" 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
    echo "↻ Stopping previous instance(s): $EXISTING"
    pkill -f "uvicorn $PROJECT_TAG" 2>/dev/null || true
    sleep 1
fi

# 2. Release the port if something else is holding it (rare — happens
#    when a prior run crashed without cleanup, or another tool grabbed it).
PORT_HOLDER=$(lsof -t -i ":$PORT" -sTCP:LISTEN 2>/dev/null || true)
if [ -n "$PORT_HOLDER" ]; then
    echo "↻ Releasing port $PORT (held by PID $PORT_HOLDER)"
    kill "$PORT_HOLDER" 2>/dev/null || true
    sleep 1
fi

# 3. Sanity check the venv.
if [ ! -x .venv/bin/uvicorn ]; then
    echo
    echo "✗ .venv/bin/uvicorn not found at $(pwd)/.venv"
    echo "  Run setup first, e.g.:"
    echo "    python3 -m venv .venv"
    echo "    .venv/bin/pip install fastapi uvicorn[standard] sentence-transformers \\"
    echo "                          openai anthropic google-generativeai openpyxl python-multipart \\"
    echo "                          python-dotenv pillow httpx"
    echo
    read -n 1 -s -r -p "Press any key to close this window…"
    exit 1
fi

echo "▸ Starting uvicorn on http://localhost:$PORT"
echo "  Close this Terminal window to stop the server."
echo

# 4. Open the browser once the server is most likely up.
( sleep 3 && open "http://localhost:$PORT" ) &

# 5. Foreground exec — logs stream to this window, ⌘W kills the server.
exec .venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port "$PORT" --reload
