#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "common/config.h"
#include "common/config_loader.h"
#include "common/thread_safe_queue.h"
#include "common/types.h"
#include "raft/persister.h"
#include "raft/raft.h"
#include "kv_store/kv_store.h"
#include "rpc/kv_service.h"
#include "rpc/raft_service.h"

// ── Command-line parsing ──────────────────────────────────────────
// Usage:
//   raftkv_server --config <config.json>
//   raftkv_server --config <config.json> --id 1      (CLI overrides file)
//   raftkv_server --id 0 --peers <addr0,addr1,...>   (legacy, no config file)

static std::vector<std::string> split_by_comma(const std::string& s) {
  std::vector<std::string> result;
  size_t start = 0;
  for (size_t i = 0; i <= s.size(); ++i) {
    if (i == s.size() || s[i] == ',') {
      result.push_back(s.substr(start, i - start));
      start = i + 1;
    }
  }
  return result;
}

static raftkv::ServerConfig parse_args(int argc, char* argv[]) {
  raftkv::ServerConfig cfg;
  cfg.node_id = -1;  // mark as unset

  // First pass: find --config and load it as base
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if ((flag == "--config" || flag == "-config") && i + 1 < argc) {
      cfg = raftkv::load_config(argv[++i]);
      break;
    }
  }

  // Second pass: CLI flags override config file values
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if ((flag == "--config" || flag == "-config") && i + 1 < argc) {
      ++i;  // already handled
    } else if ((flag == "--id" || flag == "-id") && i + 1 < argc) {
      cfg.node_id = std::stoi(argv[++i]);
    } else if ((flag == "--peers" || flag == "-peers") && i + 1 < argc) {
      cfg.peer_addrs = split_by_comma(argv[++i]);
    } else if ((flag == "--data" || flag == "-data") && i + 1 < argc) {
      cfg.data_dir = argv[++i];
    }
  }

  // Re-derive listen_addr after CLI overrides
  if (cfg.node_id >= 0 &&
      cfg.node_id < static_cast<int>(cfg.peer_addrs.size())) {
    cfg.listen_addr = cfg.peer_addrs[cfg.node_id];
  }

  return cfg;
}

// ── main ──────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [%l] %v");

  raftkv::ServerConfig config = parse_args(argc, argv);

  if (config.node_id < 0 || config.peer_addrs.empty()) {
    spdlog::error("Usage: raftkv_server --config <file> | --id <n> --peers <addr0,addr1,...> [--data <dir>]");
    return 1;
  }
  if (config.node_id >= static_cast<int>(config.peer_addrs.size())) {
    spdlog::error("node_id {} out of range (peers count={})",
                  config.node_id, config.peer_addrs.size());
    return 1;
  }

  spdlog::info("[Node {}] starting on {}", config.node_id, config.listen_addr);

  spdlog::info("[Node {}] config: peers={}, data_dir={}, election_timeout=[{},{}]ms, heartbeat={}ms, max_state={}B",
               config.node_id, config.peer_addrs.size(), config.data_dir,
               config.raft.election_timeout_min_ms, config.raft.election_timeout_max_ms,
               config.raft.heartbeat_interval_ms, config.raft.max_raft_state_bytes);

  // ── Create Persister ───────────────────────────────────────────
  auto persister = std::make_shared<raftkv::Persister>(
      config.node_id, config.data_dir);

  // ── Create apply channel ───────────────────────────────────────
  auto apply_channel =
      std::make_shared<raftkv::ThreadSafeQueue<raftkv::ApplyMsg>>();

  // ── Create peer clients ────────────────────────────────────────
  std::vector<std::shared_ptr<raftkv::RaftPeerClient>> peers;
  for (int i = 0; i < static_cast<int>(config.peer_addrs.size()); ++i) {
    if (i == config.node_id) {
      // Placeholder for self — Raft never sends RPCs to itself,
      // but the vector must be indexed by node_id.
      peers.push_back(std::shared_ptr<raftkv::RaftPeerClient>());
    } else {
      peers.push_back(std::make_shared<raftkv::GrpcRaftPeerClient>(
          config.peer_addrs[i]));
    }
  }

  // ── Create Raft node ───────────────────────────────────────────
  auto raft_node = std::make_shared<raftkv::Raft>(
      config, peers, persister, apply_channel);
  raft_node->start_threads();

  // ── Create KV store ─────────────────────────────────────────────
  auto kv_store = std::make_shared<raftkv::KvStore>(
      config, raft_node, apply_channel);

  // ── Create gRPC server ─────────────────────────────────────────
  raftkv::RaftServiceImpl raft_service(raft_node);
  raftkv::KvServiceImpl kv_service(kv_store);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(config.listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&raft_service);
  builder.RegisterService(&kv_service);

  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    spdlog::error("[Node {}] failed to start gRPC server on {}",
                  config.node_id, config.listen_addr);
    return 1;
  }
  spdlog::info("[Node {}] gRPC server listening on {}", config.node_id, config.listen_addr);

  // Block until Ctrl+C or the server is shut down externally.
  server->Wait();

  spdlog::info("[Node {}] shutting down", config.node_id);
  return 0;
}
