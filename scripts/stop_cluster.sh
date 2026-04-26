#!/usr/bin/env bash
# Stop the local Raft KV cluster started by start_cluster.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT_CONFIG="$PROJECT_DIR/config/script_settings.sh"

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

stopped=0
for node in "${CLUSTER_NODES[@]}"; do
  IFS=':' read -r node_id _ <<< "$node"
  pid_file="$PID_DIR/node${node_id}.pid"
  if [[ -f "$pid_file" ]]; then
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      echo "Node $node_id stopped (PID $pid)"
      stopped=$((stopped + 1))
    else
      echo "Node $node_id not running (stale PID $pid)"
    fi
    rm -f "$pid_file"
  else
    echo "Node $node_id: no PID file"
  fi
done

if [[ $stopped -eq 0 ]]; then
  echo "No nodes were running."
else
  echo "$stopped node(s) stopped."
fi
