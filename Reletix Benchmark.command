#!/usr/bin/env bash
# Double-clickable launcher for the Reletix Visual LLM Benchmark.
# - Kills any previously-running instance of THIS project's server.
# - Polls until the port is actually free (uvicorn --reload spawns
#   parent + worker; SIGTERM can take >1s to release the bind).
# - Starts uvicorn fresh and opens the browser.
# - Closing this Terminal window stops the server cleanly.
#
# If anything fails, the window stays open with a "press any key"
# prompt so you can read the error before it disappears.

cd "$(dirname "$0")"

PORT="${PORT:-8765}"
PROJECT_TAG="backend.main"

# Trap any error and pause so the user can read the message before
# the Terminal window closes.
trap 'rc=$?; if [ $rc -ne 0 ]; then echo; echo "✗ Exited with code $rc"; read -n 1 -s -r -p "Press any key to close…"; fi' EXIT

cat <<EOF
═══════════════════════════════════════════════════════════
  Reletix · Visual LLM Benchmark
═══════════════════════════════════════════════════════════
  Repo:  $(pwd)
  Port:  $PORT
═══════════════════════════════════════════════════════════
EOF
echo

# 1. Stop previous instance(s) of THIS project's server.
EXISTING=$(pgrep -f "uvicorn $PROJECT_TAG" 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
    echo "↻ Stopping previous instance(s):"
    echo "$EXISTING" | sed 's/^/    PID /'
    # Polite first
    pkill -TERM -f "uvicorn $PROJECT_TAG" 2>/dev/null || true
    # Wait up to 5 s for them to exit
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 0.5
        if ! pgrep -f "uvicorn $PROJECT_TAG" >/dev/null 2>&1; then
            break
        fi
    done
    # Force any survivors
    if pgrep -f "uvicorn $PROJECT_TAG" >/dev/null 2>&1; then
        echo "    (escalating to SIGKILL)"
        pkill -KILL -f "uvicorn $PROJECT_TAG" 2>/dev/null || true
        sleep 0.5
    fi
fi

# 2. Force-release the port if anything is still bound to it. This
#    catches stragglers (a worker subprocess that didn't get the
#    signal, or some other tool grabbing 8765).
release_port() {
    local holder
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        holder=$(lsof -t -i ":$PORT" -sTCP:LISTEN 2>/dev/null || true)
        if [ -z "$holder" ]; then
            return 0
        fi
        echo "↻ Port $PORT held by PID $holder — sending SIGKILL"
        kill -KILL $holder 2>/dev/null || true
        sleep 0.5
    done
    # Last check
    holder=$(lsof -t -i ":$PORT" -sTCP:LISTEN 2>/dev/null || true)
    if [ -n "$holder" ]; then
        echo "✗ Could not free port $PORT (still held by PID $holder)"
        return 1
    fi
    return 0
}
release_port

# 3. Sanity check the venv.
if [ ! -x .venv/bin/uvicorn ]; then
    echo
    echo "✗ .venv/bin/uvicorn not found at $(pwd)/.venv"
    echo "  Run setup first, e.g.:"
    echo "    python3 -m venv .venv"
    echo "    .venv/bin/pip install fastapi 'uvicorn[standard]' sentence-transformers \\"
    echo "                          openai anthropic google-generativeai openpyxl python-multipart \\"
    echo "                          python-dotenv pillow httpx"
    exit 1
fi

echo
echo "▸ Starting uvicorn on http://localhost:$PORT"
echo "  Close this Terminal window to stop the server."
echo

# 4. Open the browser shortly after the server is most likely up.
#    Detached so it doesn't block the foreground exec below.
( sleep 3 && open "http://localhost:$PORT" >/dev/null 2>&1 ) &
disown

# 5. Foreground exec — logs stream to this window, ⌘W kills the server.
#    Replaces this shell so the trap above only fires on launch failures.
exec .venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port "$PORT" --reload
