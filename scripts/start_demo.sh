#!/usr/bin/env bash
# Start cluster + REST gateway, then print the dashboard URL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT_CONFIG="$PROJECT_DIR/config/script_settings.sh"
GATEWAY="$PROJECT_DIR/bin/raftkv_gateway"

if [[ ! -x "$GATEWAY" ]]; then
  echo "ERROR: $GATEWAY not found. Build the project first." >&2
  exit 1
fi

if [[ ! -f "$SCRIPT_CONFIG" ]]; then
  echo "ERROR: script config not found: $SCRIPT_CONFIG" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$SCRIPT_CONFIG"

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s\n' "$path"
  else
    printf '%s\n' "$PROJECT_DIR/$path"
  fi
}

PID_DIR="$(resolve_path "$CLUSTER_PID_DIR")"
GATEWAY_CONFIG_PATH="$(resolve_path "$GATEWAY_CONFIG")"
WEB_DIR="$(resolve_path "$GATEWAY_WEB_DIR")"
GATEWAY_LOG_PATH="$(resolve_path "$GATEWAY_LOG_FILE")"
GATEWAY_PID_PATH="$(resolve_path "$GATEWAY_PID_FILE")"

cleanup() {
  echo ""
  echo "=== Stopping gateway ==="
  if [[ -f "$GATEWAY_PID_PATH" ]]; then
    pid=$(cat "$GATEWAY_PID_PATH")
    kill "$pid" 2>/dev/null && echo "  Gateway stopped (PID $pid)" || true
    rm -f "$GATEWAY_PID_PATH"
  fi
  "$SCRIPT_DIR/stop_cluster.sh"
}
trap cleanup EXIT

# ── Start cluster ────────────────────────────────────────────────
echo "=== Starting cluster ==="
"$SCRIPT_DIR/start_cluster.sh"
echo "Waiting for leader election..."
sleep "$DEMO_CLUSTER_WAIT_SEC"

# ── Start gateway ────────────────────────────────────────────────
mkdir -p "$PID_DIR" "$(dirname "$GATEWAY_LOG_PATH")" "$(dirname "$GATEWAY_PID_PATH")"
echo "=== Starting REST gateway on port $GATEWAY_PORT ==="
(
  cd "$PROJECT_DIR"
  exec "$GATEWAY" --config "$GATEWAY_CONFIG_PATH" --port "$GATEWAY_PORT" --web "$WEB_DIR" \
    > "$GATEWAY_LOG_PATH" 2>&1
) &
echo $! > "$GATEWAY_PID_PATH"
echo "  Gateway started (PID $!) -> $GATEWAY_LOG_PATH"

sleep "$GATEWAY_START_WAIT_SEC"
echo ""
echo "=== Dashboard ready ==="
echo "  Open http://localhost:$GATEWAY_PORT in your browser"
echo "  Press Ctrl+C to stop everything"
echo ""

# Wait until Ctrl+C
wait
