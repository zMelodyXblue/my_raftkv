#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "common/thread_safe_queue.h"
#include "common/types.h"
#include "raft/persister.h"
#include "raft/raft.h"
#include "kv_store/kv_store.h"
#include "rpc/grpc/grpc_raft_peer.h"
#include "rpc/grpc/grpc_raft_service.h"
#include "rpc/grpc/grpc_kv_service.h"
#include "rpc/grpc/grpc_kv_service_client.h"
#include "client/kv_client.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_skiplist_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_skiplist_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct SkipListNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<KvStore>                   kv_store;
  std::shared_ptr<RaftServiceImpl>           raft_service;
  std::shared_ptr<KvServiceImpl>             kv_service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
};

// ── SkipListCluster ──────────────────────────────────────────────
// Same as SnapshotCluster but uses engine_type = "skiplist".
class SkipListCluster {
 public:
  SkipListCluster(int n, int base_port, int max_raft_state = 200)
      : n_(n),
        base_port_(base_port),
        max_raft_state_(max_raft_state),
        tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));
    nodes_.resize(n_);
    for (int i = 0; i < n_; ++i) start_node(i);
  }

  ~SkipListCluster() {
    for (int i = 0; i < n_; ++i) kill_node(i);
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  void start_node(int i) {
    ServerConfig cfg;
    cfg.node_id     = i;
    cfg.listen_addr = addrs_[i];
    cfg.peer_addrs  = addrs_;
    cfg.data_dir    = tmpdir_;
    cfg.engine_type = "skiplist";
    cfg.raft.election_timeout_min_ms = 150;
    cfg.raft.election_timeout_max_ms = 300;
    cfg.raft.heartbeat_interval_ms   = 50;
    cfg.raft.rpc_timeout_ms          = 100;
    cfg.raft.apply_interval_ms       = 10;
    cfg.raft.max_raft_state_bytes    = max_raft_state_;

    auto& nd = nodes_[i];
    nd.queue = std::make_shared<ThreadSafeQueue<ApplyMsg>>();

    std::vector<std::shared_ptr<RaftPeerClient>> peers;
    for (int j = 0; j < n_; ++j)
      peers.push_back(std::make_shared<GrpcRaftPeerClient>(addrs_[j]));

    auto persister = std::make_shared<Persister>(i, tmpdir_);
    nd.raft = std::make_shared<Raft>(cfg, std::move(peers),
                                     persister, nd.queue);
    nd.raft->start_threads();

    nd.kv_store     = std::make_shared<KvStore>(cfg, nd.raft, nd.queue);
    nd.raft_service = std::make_shared<RaftServiceImpl>(nd.raft);
    nd.kv_service   = std::make_shared<KvServiceImpl>(nd.kv_store);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addrs_[i], grpc::InsecureServerCredentials());
    builder.RegisterService(nd.raft_service.get());
    builder.RegisterService(nd.kv_service.get());
    nd.server = builder.BuildAndStart();
  }

  void kill_node(int i) {
    auto& nd = nodes_[i];
    if (nd.server) { nd.server->Shutdown(); nd.server.reset(); }
    nd.kv_service.reset();
    nd.raft_service.reset();
    nd.kv_store.reset();
    nd.raft.reset();
    if (nd.queue) nd.queue->close();
    nd.queue.reset();
  }

  int wait_leader(int max_wait_ms = 5000) {
    for (int waited = 0; waited < max_wait_ms; waited += 50) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      int leader = -1;
      bool multi = false;
      for (int i = 0; i < n_; ++i) {
        if (!nodes_[i].raft) continue;
        if (nodes_[i].raft->is_leader()) {
          if (leader != -1) { multi = true; break; }
          leader = i;
        }
      }
      if (leader != -1 && !multi) return leader;
    }
    return -1;
  }

  KvClient make_client() {
    return KvClient(make_grpc_kv_service_clients(addrs_));
  }

  Raft* raft(int i) { return nodes_[i].raft.get(); }

  int n_;

 private:
  int         base_port_;
  int         max_raft_state_;
  std::string tmpdir_;
  std::vector<std::string>          addrs_;
  std::vector<SkipListNodeState>    nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

TEST(SkipListKvTest, BasicPutGet) {
  SkipListCluster c(3, 65000);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("x", "1");
  EXPECT_EQ("1", cli.get("x"));
}

TEST(SkipListKvTest, Append) {
  SkipListCluster c(3, 65100);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("k", "hello");
  cli.append("k", " world");
  EXPECT_EQ("hello world", cli.get("k"));
}

TEST(SkipListKvTest, MultipleKeys) {
  SkipListCluster c(3, 65200);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("a", "1");
  cli.put("b", "2");
  cli.put("c", "3");
  EXPECT_EQ("1", cli.get("a"));
  EXPECT_EQ("2", cli.get("b"));
  EXPECT_EQ("3", cli.get("c"));
}

// Full cycle: writes, snapshot, kill-all, restart, verify recovery.
TEST(SkipListKvTest, SnapshotAndRestart) {
  SkipListCluster c(3, 65300);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  for (int i = 0; i < 30; ++i) {
    cli.put("sk" + std::to_string(i), "sv" + std::to_string(i));
  }

  // Verify snapshot triggered on at least one node.
  bool any_snapshot = false;
  for (int i = 0; i < 3; ++i) {
    if (c.raft(i) && c.raft(i)->snapshot_index() > 0) {
      any_snapshot = true;
      break;
    }
  }
  EXPECT_TRUE(any_snapshot)
      << "snapshot should have triggered with skiplist engine";

  // Kill all and restart.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  for (int i = 0; i < 3; ++i) c.kill_node(i);
  for (int i = 0; i < 3; ++i) c.start_node(i);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli2 = c.make_client();
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ("sv" + std::to_string(i),
              cli2.get("sk" + std::to_string(i)))
        << "key sk" << i << " lost after restart with skiplist engine";
  }

  // New writes work after recovery.
  cli2.put("after", "restart");
  EXPECT_EQ("restart", cli2.get("after"));
}

}  // namespace
}  // namespace raftkv
