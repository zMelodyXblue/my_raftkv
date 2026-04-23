#pragma once

#include <chrono>
#include <string>
#include <vector>

namespace raftkv {

// ── Timing Configuration ────────────────────────────────────────
// All durations in milliseconds.
struct RaftConfig {
  // Election timeout range (randomized per node)
  int election_timeout_min_ms = 300;
  int election_timeout_max_ms = 500;

  // Leader heartbeat interval (must be << election timeout)
  int heartbeat_interval_ms = 30;

  // How often the applier checks for new committed entries
  int apply_interval_ms = 10;

  // RPC call timeout
  int rpc_timeout_ms = 500;

  // Snapshot threshold: trigger snapshot when log exceeds this size
  int max_raft_state_bytes = 8 * 1024 * 1024;  // 8 MB
};

// ── Server Configuration ────────────────────────────────────────
struct ServerConfig {
  int         node_id = 0;
  std::string listen_addr = "0.0.0.0:50051";  // This node's gRPC listen address
  std::vector<std::string> peer_addrs;         // All peer addresses (including self)
  std::string data_dir = "./data";             // Persistence directory
  std::string engine_type = "hashmap";         // "hashmap" or "skiplist"
  RaftConfig  raft;
};

}  // namespace raftkv
