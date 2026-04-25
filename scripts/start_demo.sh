#!/usr/bin/env bash
# Start 3-node cluster + REST gateway, then open browser.
# Usage: ./scripts/start_demo.sh [build_dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GATEWAY="$PROJECT_DIR/bin/raftkv_gateway"
PEERS="127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052"
PORT=8080
PID_DIR="$PROJECT_DIR/logs"
WEB_DIR="$PROJECT_DIR/web"

if [[ ! -x "$GATEWAY" ]]; then
  echo "ERROR: $GATEWAY not found. Build the project first." >&2
  exit 1
fi

cleanup() {
  echo ""
  echo "=== Stopping gateway ==="
  if [[ -f "$PID_DIR/gateway.pid" ]]; then
    pid=$(cat "$PID_DIR/gateway.pid")
    kill "$pid" 2>/dev/null && echo "  Gateway stopped (PID $pid)" || true
    rm -f "$PID_DIR/gateway.pid"
  fi
  "$SCRIPT_DIR/stop_cluster.sh"
}
trap cleanup EXIT

# ── Start cluster ────────────────────────────────────────────────
echo "=== Starting cluster ==="
"$SCRIPT_DIR/start_cluster.sh"
echo "Waiting for leader election..."
sleep 3

# ── Start gateway ────────────────────────────────────────────────
mkdir -p "$PID_DIR"
echo "=== Starting REST gateway on port $PORT ==="
"$GATEWAY" --peers "$PEERS" --port "$PORT" --web "$WEB_DIR" \
  > "$PID_DIR/gateway.log" 2>&1 &
echo $! > "$PID_DIR/gateway.pid"
echo "  Gateway started (PID $!) -> $PID_DIR/gateway.log"

sleep 1
echo ""
echo "=== Dashboard ready ==="
echo "  Open http://localhost:$PORT in your browser"
echo "  Press Ctrl+C to stop everything"
echo ""

# Wait until Ctrl+C
wait
