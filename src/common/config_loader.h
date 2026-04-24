#pragma once

#include <fstream>
#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/nlohmann/json.hpp"

namespace raftkv {

// Load ServerConfig from a JSON file.
// Missing fields keep their default values from ServerConfig / RaftConfig.
inline ServerConfig load_config(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    throw std::runtime_error("cannot open config file: " + path);
  }

  nlohmann::json j;
  ifs >> j;

  ServerConfig cfg;

  if (j.count("node_id"))     cfg.node_id     = j["node_id"].get<int>();
  if (j.count("data_dir"))    cfg.data_dir    = j["data_dir"].get<std::string>();
  if (j.count("engine_type")) cfg.engine_type = j["engine_type"].get<std::string>();

  if (j.count("peers")) {
    cfg.peer_addrs.clear();
    for (const auto& p : j["peers"]) {
      cfg.peer_addrs.push_back(p.get<std::string>());
    }
  }

  if (j.count("raft")) {
    const auto& r = j["raft"];
    if (r.count("election_timeout_min_ms")) cfg.raft.election_timeout_min_ms = r["election_timeout_min_ms"].get<int>();
    if (r.count("election_timeout_max_ms")) cfg.raft.election_timeout_max_ms = r["election_timeout_max_ms"].get<int>();
    if (r.count("heartbeat_interval_ms"))   cfg.raft.heartbeat_interval_ms   = r["heartbeat_interval_ms"].get<int>();
    if (r.count("apply_interval_ms"))       cfg.raft.apply_interval_ms       = r["apply_interval_ms"].get<int>();
    if (r.count("rpc_timeout_ms"))          cfg.raft.rpc_timeout_ms          = r["rpc_timeout_ms"].get<int>();
    if (r.count("max_raft_state_bytes"))    cfg.raft.max_raft_state_bytes    = r["max_raft_state_bytes"].get<int>();
  }

  // Derive listen_addr from peer_addrs if available
  if (cfg.node_id >= 0 &&
      cfg.node_id < static_cast<int>(cfg.peer_addrs.size())) {
    cfg.listen_addr = cfg.peer_addrs[cfg.node_id];
  }

  return cfg;
}

}  // namespace raftkv
