#!/usr/bin/env bash
# One-shot demo: start cluster -> write -> read -> kill node -> verify recovery.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT_CONFIG="$PROJECT_DIR/config/script_settings.sh"
CLI="$PROJECT_DIR/bin/raftkv_cli"

if [[ ! -x "$CLI" ]]; then
  echo "ERROR: $CLI not found. Build the project first." >&2
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

CLIENT_CONFIG="$(resolve_path "$DEMO_CLIENT_CONFIG")"
PID_DIR="$(resolve_path "$CLUSTER_PID_DIR")"

cleanup() {
  echo ""
  echo "=== Cleaning up ==="
  "$SCRIPT_DIR/stop_cluster.sh"
}
trap cleanup EXIT

# ── Step 1: Start cluster ────────────────────────────────────────
echo "=== Step 1: Starting 3-node cluster ==="
"$SCRIPT_DIR/start_cluster.sh"
echo "Waiting for leader election..."
sleep "$DEMO_CLUSTER_WAIT_SEC"

# ── Step 2: Write data ──────────────────────────────────────────
echo ""
echo "=== Step 2: Writing data ==="
"$CLI" --config "$CLIENT_CONFIG" put "$DEMO_KEY_PRIMARY" "$DEMO_VALUE_PRIMARY"
echo "  Put $DEMO_KEY_PRIMARY = $DEMO_VALUE_PRIMARY"

"$CLI" --config "$CLIENT_CONFIG" put "$DEMO_KEY_CITY" "$DEMO_VALUE_CITY"
echo "  Put $DEMO_KEY_CITY = $DEMO_VALUE_CITY"

"$CLI" --config "$CLIENT_CONFIG" append "$DEMO_APPEND_KEY" "$DEMO_APPEND_VALUE_1"
"$CLI" --config "$CLIENT_CONFIG" append "$DEMO_APPEND_KEY" "$DEMO_APPEND_VALUE_2"
echo "  Append $DEMO_APPEND_KEY = ${DEMO_APPEND_VALUE_1}${DEMO_APPEND_VALUE_2}"

# ── Step 3: Read data ───────────────────────────────────────────
echo ""
echo "=== Step 3: Reading data ==="
val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_KEY_PRIMARY")
echo "  Get $DEMO_KEY_PRIMARY = $val"

val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_KEY_CITY")
echo "  Get $DEMO_KEY_CITY = $val"

val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_APPEND_KEY")
echo "  Get $DEMO_APPEND_KEY = $val"

# ── Step 4: Kill configured node ───────────────────────────────
echo ""
echo "=== Step 4: Killing node $DEMO_FAIL_NODE_ID (configured target) ==="
pid_file="$PID_DIR/node${DEMO_FAIL_NODE_ID}.pid"
pid_to_kill=$(cat "$pid_file" 2>/dev/null || echo "")
if [[ -n "$pid_to_kill" ]] && kill -0 "$pid_to_kill" 2>/dev/null; then
  kill "$pid_to_kill"
  rm -f "$pid_file"
  echo "  Node $DEMO_FAIL_NODE_ID killed (PID $pid_to_kill)"
else
  echo "  Node $DEMO_FAIL_NODE_ID was not running"
fi

echo "Waiting for re-election..."
sleep "$DEMO_CLUSTER_WAIT_SEC"

# ── Step 5: Verify recovery ─────────────────────────────────────
echo ""
echo "=== Step 5: Verifying data after node failure ==="
val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_KEY_PRIMARY")
echo "  Get $DEMO_KEY_PRIMARY = $val"

val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_KEY_CITY")
echo "  Get $DEMO_KEY_CITY = $val"

# ── Step 6: New writes after failure ────────────────────────────
echo ""
echo "=== Step 6: Writing after failure ==="
"$CLI" --config "$CLIENT_CONFIG" put "$DEMO_KEY_AFTER_CRASH" "$DEMO_VALUE_AFTER_CRASH"
val=$("$CLI" --config "$CLIENT_CONFIG" get "$DEMO_KEY_AFTER_CRASH")
echo "  Put+Get $DEMO_KEY_AFTER_CRASH = $val"

echo ""
echo "=== Demo complete! ==="
