#!/usr/bin/env bash
# Stop the local Raft KV cluster started by start_cluster.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$PROJECT_DIR/logs"

stopped=0
for i in 0 1 2; do
  pid_file="$PID_DIR/node${i}.pid"
  if [[ -f "$pid_file" ]]; then
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      echo "Node $i stopped (PID $pid)"
      stopped=$((stopped + 1))
    else
      echo "Node $i not running (stale PID $pid)"
    fi
    rm -f "$pid_file"
  else
    echo "Node $i: no PID file"
  fi
done

if [[ $stopped -eq 0 ]]; then
  echo "No nodes were running."
else
  echo "$stopped node(s) stopped."
fi
