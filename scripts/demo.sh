#!/usr/bin/env bash
# One-shot demo: start cluster -> write -> read -> kill leader -> verify recovery.
# Usage: ./scripts/demo.sh [build_dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-$PROJECT_DIR/build}"
CLI="$BUILD_DIR/bin/raftkv_cli"
PEERS="127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052"
PID_DIR="$PROJECT_DIR/logs"

if [[ ! -x "$CLI" ]]; then
  echo "ERROR: $CLI not found. Build the project first." >&2
  exit 1
fi

cleanup() {
  echo ""
  echo "=== Cleaning up ==="
  "$SCRIPT_DIR/stop_cluster.sh"
}
trap cleanup EXIT

# ── Step 1: Start cluster ────────────────────────────────────────
echo "=== Step 1: Starting 3-node cluster ==="
"$SCRIPT_DIR/start_cluster.sh" "$BUILD_DIR"
echo "Waiting for leader election..."
sleep 3

# ── Step 2: Write data ──────────────────────────────────────────
echo ""
echo "=== Step 2: Writing data ==="
"$CLI" --peers "$PEERS" put demo_key "hello_raft"
echo "  Put demo_key = hello_raft"

"$CLI" --peers "$PEERS" put city "Beijing"
echo "  Put city = Beijing"

"$CLI" --peers "$PEERS" append greeting "Hello "
"$CLI" --peers "$PEERS" append greeting "World"
echo "  Append greeting = Hello World"

# ── Step 3: Read data ───────────────────────────────────────────
echo ""
echo "=== Step 3: Reading data ==="
val=$("$CLI" --peers "$PEERS" get demo_key)
echo "  Get demo_key = $val"

val=$("$CLI" --peers "$PEERS" get city)
echo "  Get city = $val"

val=$("$CLI" --peers "$PEERS" get greeting)
echo "  Get greeting = $val"

# ── Step 4: Kill leader ─────────────────────────────────────────
echo ""
echo "=== Step 4: Killing node 0 (may or may not be leader) ==="
pid0=$(cat "$PID_DIR/node0.pid" 2>/dev/null || echo "")
if [[ -n "$pid0" ]] && kill -0 "$pid0" 2>/dev/null; then
  kill "$pid0"
  rm -f "$PID_DIR/node0.pid"
  echo "  Node 0 killed (PID $pid0)"
else
  echo "  Node 0 was not running"
fi

echo "Waiting for re-election..."
sleep 3

# ── Step 5: Verify recovery ─────────────────────────────────────
echo ""
echo "=== Step 5: Verifying data after node failure ==="
val=$("$CLI" --peers "$PEERS" get demo_key)
echo "  Get demo_key = $val"

val=$("$CLI" --peers "$PEERS" get city)
echo "  Get city = $val"

# ── Step 6: New writes after failure ────────────────────────────
echo ""
echo "=== Step 6: Writing after failure ==="
"$CLI" --peers "$PEERS" put after_crash "still_works"
val=$("$CLI" --peers "$PEERS" get after_crash)
echo "  Put+Get after_crash = $val"

echo ""
echo "=== Demo complete! ==="
