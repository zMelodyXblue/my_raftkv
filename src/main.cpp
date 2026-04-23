#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "common/config.h"
#include "common/thread_safe_queue.h"
#include "common/types.h"
#include "raft/persister.h"
#include "raft/raft.h"
#include "kv_store/kv_store.h"
#include "rpc/kv_service.h"
#include "rpc/raft_service.h"

// ── Command-line parsing ──────────────────────────────────────────
// Usage:
//   raftkv_server --id <node_id> \
//                 --peers <addr0>,<addr1>,<addr2> \
//                 --data <data_dir>
//
// Example (3-node local cluster):
//   raftkv_server --id 0 --peers 127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052 --data ./data/node0
//   raftkv_server --id 1 --peers 127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052 --data ./data/node1
//   raftkv_server --id 2 --peers 127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052 --data ./data/node2
//
// peer_addrs[node_id] is also used as the listen address for this node.

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

struct Args {
  int         node_id  = -1;
  std::string data_dir = "./data";
  std::vector<std::string> peer_addrs;
};

static Args parse_args(int argc, char* argv[]) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if ((flag == "--id" || flag == "-id") && i + 1 < argc) {
      a.node_id = std::stoi(argv[++i]);
    } else if ((flag == "--peers" || flag == "-peers") && i + 1 < argc) {
      a.peer_addrs = split_by_comma(argv[++i]);
    } else if ((flag == "--data" || flag == "-data") && i + 1 < argc) {
      a.data_dir = argv[++i];
    }
  }
  return a;
}

// ── main ──────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [%l] %v");

  Args args = parse_args(argc, argv);

  if (args.node_id < 0 || args.peer_addrs.empty()) {
    spdlog::error("Usage: raftkv_server --id <n> --peers <addr0,addr1,...> [--data <dir>]");
    return 1;
  }
  if (args.node_id >= static_cast<int>(args.peer_addrs.size())) {
    spdlog::error("node_id {} out of range (peers count={})",
                  args.node_id, args.peer_addrs.size());
    return 1;
  }

  const std::string listen_addr = args.peer_addrs[args.node_id];
  spdlog::info("[Node {}] starting on {}", args.node_id, listen_addr);

  // ── Build ServerConfig ─────────────────────────────────────────
  raftkv::ServerConfig config;
  config.node_id     = args.node_id;
  config.listen_addr = listen_addr;
  config.peer_addrs  = args.peer_addrs;
  config.data_dir    = args.data_dir;

  // ── Create Persister ───────────────────────────────────────────
  auto persister = std::make_shared<raftkv::Persister>(
      config.node_id, config.data_dir);

  // ── Create apply channel ───────────────────────────────────────
  auto apply_channel =
      std::make_shared<raftkv::ThreadSafeQueue<raftkv::ApplyMsg>>();

  // ── Create peer clients ────────────────────────────────────────
  std::vector<std::shared_ptr<raftkv::RaftPeerClient>> peers;
  for (int i = 0; i < static_cast<int>(args.peer_addrs.size()); ++i) {
    if (i == args.node_id) {
      // Placeholder for self — Raft never sends RPCs to itself,
      // but the vector must be indexed by node_id.
      peers.push_back(std::shared_ptr<raftkv::RaftPeerClient>());
    } else {
      peers.push_back(std::make_shared<raftkv::GrpcRaftPeerClient>(
          args.peer_addrs[i]));
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
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&raft_service);
  builder.RegisterService(&kv_service);

  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    spdlog::error("[Node {}] failed to start gRPC server on {}",
                  args.node_id, listen_addr);
    return 1;
  }
  spdlog::info("[Node {}] gRPC server listening on {}", args.node_id, listen_addr);

  // Block until Ctrl+C or the server is shut down externally.
  server->Wait();

  spdlog::info("[Node {}] shutting down", args.node_id);
  return 0;
}
