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
  char tmpl[] = "/tmp/raft_kv_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_kv_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct KvNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<KvStore>                   kv_store;
  std::shared_ptr<RaftServiceImpl>           raft_service;
  std::shared_ptr<KvServiceImpl>             kv_service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
};

// ── KvCluster ────────────────────────────────────────────────────
// In-process N-node cluster with Raft + KvStore + gRPC services.
class KvCluster {
 public:
  KvCluster(int n, int base_port)
      : n_(n), base_port_(base_port), tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));

    RaftConfig rcfg;
    rcfg.election_timeout_min_ms = 150;
    rcfg.election_timeout_max_ms = 300;
    rcfg.heartbeat_interval_ms   = 50;
    rcfg.rpc_timeout_ms          = 100;
    rcfg.apply_interval_ms       = 10;

    for (int i = 0; i < n_; ++i)
      nodes_.push_back(std::unique_ptr<KvNodeState>(new KvNodeState()));

    for (int i = 0; i < n_; ++i) {
      ServerConfig cfg;
      cfg.node_id     = i;
      cfg.listen_addr = addrs_[i];
      cfg.peer_addrs  = addrs_;
      cfg.data_dir    = tmpdir_;
      cfg.raft        = rcfg;

      auto* nd = nodes_[i].get();

      std::vector<std::shared_ptr<RaftPeerClient>> peers;
      for (int j = 0; j < n_; ++j)
        peers.push_back(std::make_shared<GrpcRaftPeerClient>(addrs_[j]));

      nd->queue = std::make_shared<ThreadSafeQueue<ApplyMsg>>();
      auto persister = std::make_shared<Persister>(i, tmpdir_);
      nd->raft = std::make_shared<Raft>(cfg, std::move(peers), persister, nd->queue);
      nd->raft->start_threads();

      nd->kv_store = std::make_shared<KvStore>(cfg, nd->raft, nd->queue);
      nd->raft_service = std::make_shared<RaftServiceImpl>(nd->raft);
      nd->kv_service = std::make_shared<KvServiceImpl>(nd->kv_store);

      grpc::ServerBuilder builder;
      builder.AddListeningPort(addrs_[i], grpc::InsecureServerCredentials());
      builder.RegisterService(nd->raft_service.get());
      builder.RegisterService(nd->kv_service.get());
      nd->server = builder.BuildAndStart();
    }
  }

  ~KvCluster() {
    for (int i = 0; i < n_; ++i) kill(i);
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  void kill(int i) {
    auto* nd = nodes_[i].get();
    if (nd->server) { nd->server->Shutdown(); nd->server.reset(); }
    nd->kv_service.reset();
    nd->raft_service.reset();
    nd->kv_store.reset();
    nd->raft.reset();
    if (nd->queue) nd->queue->close();
  }

  // Wait until a stable leader is elected. Returns node_id or -1.
  int wait_leader(int max_wait_ms = 5000) {
    for (int waited = 0; waited < max_wait_ms; waited += 50) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      int leader = -1;
      bool multi = false;
      for (int i = 0; i < n_; ++i) {
        if (!nodes_[i]->raft) continue;
        if (nodes_[i]->raft->is_leader()) {
          if (leader != -1) { multi = true; break; }
          leader = i;
        }
      }
      if (leader != -1 && !multi) return leader;
    }
    return -1;
  }

  // Create a KvClient connected to all servers.
  KvClient make_client() { return KvClient(make_grpc_kv_service_clients(addrs_)); }

  const std::vector<std::string>& addrs() const { return addrs_; }

  int n_;

 private:
  int         base_port_;
  std::string tmpdir_;
  std::vector<std::string>                    addrs_;
  std::vector<std::unique_ptr<KvNodeState>>   nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

// Basic Put then Get.
TEST(KvTest, BasicPutGet) {
  KvCluster c(3, 60100);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("x", "1");
  EXPECT_EQ("1", cli.get("x"));
}

// Append concatenates values.
TEST(KvTest, Append) {
  KvCluster c(3, 60200);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("k", "hello");
  cli.append("k", " world");
  EXPECT_EQ("hello world", cli.get("k"));
}

// Get on a missing key returns empty string.
TEST(KvTest, GetMissing) {
  KvCluster c(3, 60300);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  EXPECT_EQ("", cli.get("nonexistent"));
}

// Multiple keys are independent.
TEST(KvTest, MultipleKeys) {
  KvCluster c(3, 60400);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("a", "1");
  cli.put("b", "2");
  cli.put("c", "3");
  EXPECT_EQ("1", cli.get("a"));
  EXPECT_EQ("2", cli.get("b"));
  EXPECT_EQ("3", cli.get("c"));
}

// Overwrite: Put replaces existing value.
TEST(KvTest, Overwrite) {
  KvCluster c(3, 60500);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  cli.put("x", "old");
  cli.put("x", "new");
  EXPECT_EQ("new", cli.get("x"));
}

// Two independent clients see each other's writes (linearizability).
TEST(KvTest, TwoClients) {
  KvCluster c(3, 60600);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli1 = c.make_client();
  KvClient cli2 = c.make_client();

  cli1.put("shared", "from_cli1");
  EXPECT_EQ("from_cli1", cli2.get("shared"));

  cli2.put("shared", "from_cli2");
  EXPECT_EQ("from_cli2", cli1.get("shared"));
}

// After leader crash, new leader serves correct data.
TEST(KvTest, LeaderCrash) {
  KvCluster c(3, 60700);
  int leader1 = c.wait_leader();
  ASSERT_NE(-1, leader1);

  KvClient cli = c.make_client();
  cli.put("survive", "yes");
  EXPECT_EQ("yes", cli.get("survive"));

  // Kill the current leader.
  c.kill(leader1);

  // Wait for a new leader.
  int leader2 = c.wait_leader();
  ASSERT_NE(-1, leader2);
  ASSERT_NE(leader1, leader2);

  // Data committed before crash must survive.
  // Need a new client since the old one may be pointing at the dead node.
  // KvClient retries internally, but rebuild to be safe with port list.
  KvClient cli2 = c.make_client();
  EXPECT_EQ("yes", cli2.get("survive"));

  // New writes work on the new leader.
  cli2.put("after", "crash");
  EXPECT_EQ("crash", cli2.get("after"));
}

// Sequential appends produce the correct concatenation.
TEST(KvTest, AppendSequence) {
  KvCluster c(3, 60800);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  for (int i = 0; i < 10; ++i) {
    cli.append("seq", std::to_string(i));
  }
  EXPECT_EQ("0123456789", cli.get("seq"));
}

}  // namespace
}  // namespace raftkv
