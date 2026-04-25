#!/usr/bin/env bash
# Start a 3-node local Raft KV cluster.
# Usage: ./scripts/start_cluster.sh [build_dir]
#   build_dir defaults to ./build

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER="$PROJECT_DIR/bin/raftkv_server"

if [[ ! -x "$SERVER" ]]; then
  echo "ERROR: $SERVER not found. Build the project first." >&2
  exit 1
fi

PEERS="127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052"
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/logs"
DATA_DIR="$PROJECT_DIR/data"

mkdir -p "$LOG_DIR" "$DATA_DIR/node0" "$DATA_DIR/node1" "$DATA_DIR/node2"

for i in 0 1 2; do
  if [[ -f "$PID_DIR/node${i}.pid" ]]; then
    old_pid=$(cat "$PID_DIR/node${i}.pid")
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "Node $i already running (PID $old_pid). Stop first."
      exit 1
    fi
    rm -f "$PID_DIR/node${i}.pid"
  fi
done

echo "Starting 3-node cluster..."

for i in 0 1 2; do
  "$SERVER" --id "$i" --peers "$PEERS" --data "$DATA_DIR/node${i}" \
    > "$LOG_DIR/node${i}.log" 2>&1 &
  echo $! > "$PID_DIR/node${i}.pid"
  echo "  Node $i started (PID $!) -> $LOG_DIR/node${i}.log"
done

echo "Cluster started. Use 'scripts/stop_cluster.sh' to stop."
