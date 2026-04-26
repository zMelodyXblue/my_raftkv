#!/usr/bin/env bash
# Start the local Raft KV cluster defined in config/script_settings.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT_CONFIG="$PROJECT_DIR/config/script_settings.sh"
SERVER="$PROJECT_DIR/bin/raftkv_server"

if [[ ! -x "$SERVER" ]]; then
  echo "ERROR: $SERVER not found. Build the project first." >&2
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

LOG_DIR="$(resolve_path "$CLUSTER_LOG_DIR")"
PID_DIR="$(resolve_path "$CLUSTER_PID_DIR")"

mkdir -p "$LOG_DIR" "$PID_DIR"

for node in "${CLUSTER_NODES[@]}"; do
  IFS=':' read -r node_id config_rel <<< "$node"
  pid_file="$PID_DIR/node${node_id}.pid"
  if [[ -f "$pid_file" ]]; then
    old_pid=$(cat "$pid_file")
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "Node $node_id already running (PID $old_pid). Stop first."
      exit 1
    fi
    rm -f "$pid_file"
  fi
done

echo "Starting cluster..."

for node in "${CLUSTER_NODES[@]}"; do
  IFS=':' read -r node_id config_rel <<< "$node"
  config_path="$(resolve_path "$config_rel")"
  log_file="$LOG_DIR/node${node_id}.log"
  pid_file="$PID_DIR/node${node_id}.pid"

  (
    cd "$PROJECT_DIR"
    exec "$SERVER" --config "$config_path" > "$log_file" 2>&1
  ) &
  echo $! > "$pid_file"
  echo "  Node $node_id started (PID $!) -> $log_file"
done

echo "Cluster started. Use 'scripts/stop_cluster.sh' to stop."
