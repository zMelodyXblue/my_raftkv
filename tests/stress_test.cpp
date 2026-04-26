#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <set>
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
  char tmpl[] = "/tmp/raft_stress_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_stress_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct StressNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<KvStore>                   kv_store;
  std::shared_ptr<RaftServiceImpl>           raft_service;
  std::shared_ptr<KvServiceImpl>             kv_service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
};

// ── StressCluster ────────────────────────────────────────────────
// Reuses the SnapshotCluster pattern with kill/start per node.
class StressCluster {
 public:
  StressCluster(int n, int base_port, int max_raft_state = 200)
      : n_(n),
        base_port_(base_port),
        max_raft_state_(max_raft_state),
        tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));
    nodes_.resize(n_);
    for (int i = 0; i < n_; ++i) start_node(i);
  }

  ~StressCluster() {
    for (int i = 0; i < n_; ++i) kill_node(i);
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  void start_node(int i) {
    ServerConfig cfg;
    cfg.node_id     = i;
    cfg.listen_addr = addrs_[i];
    cfg.peer_addrs  = addrs_;
    cfg.data_dir    = tmpdir_;
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
    nd.raft = std::make_shared<Raft>(cfg, std::move(peers), persister, nd.queue);
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

  int find_leader() {
    for (int i = 0; i < n_; ++i) {
      if (nodes_[i].raft && nodes_[i].raft->is_leader()) return i;
    }
    return -1;
  }

  KvClient make_client() { return KvClient(make_grpc_kv_service_clients(addrs_)); }

  int n_;

 private:
  int         base_port_;
  int         max_raft_state_;
  std::string tmpdir_;
  std::vector<std::string>        addrs_;
  std::vector<StressNodeState>    nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

// 500 sequential Put+Get: verify each value.
TEST(StressTest, ManyPutGet) {
  StressCluster c(3, 62000);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();
  for (int i = 0; i < 500; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string val = "v" + std::to_string(i);
    cli.put(key, val);
  }
  for (int i = 0; i < 500; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string expected = "v" + std::to_string(i);
    EXPECT_EQ(expected, cli.get(key)) << "mismatch at key " << key;
  }
}

// 4 concurrent clients, each writing 100 unique keys.
TEST(StressTest, ConcurrentClients) {
  StressCluster c(3, 62100);
  ASSERT_NE(-1, c.wait_leader());

  const int num_clients = 4;
  const int keys_per_client = 100;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_clients; ++t) {
    threads.emplace_back([&c, t, keys_per_client]() {
      KvClient cli = c.make_client();
      for (int i = 0; i < keys_per_client; ++i) {
        std::string key = "c" + std::to_string(t) + "_k" + std::to_string(i);
        std::string val = "c" + std::to_string(t) + "_v" + std::to_string(i);
        cli.put(key, val);
      }
    });
  }
  for (auto& th : threads) th.join();

  // Verify all 400 keys.
  KvClient verifier = c.make_client();
  for (int t = 0; t < num_clients; ++t) {
    for (int i = 0; i < keys_per_client; ++i) {
      std::string key = "c" + std::to_string(t) + "_k" + std::to_string(i);
      std::string expected = "c" + std::to_string(t) + "_v" + std::to_string(i);
      EXPECT_EQ(expected, verifier.get(key))
          << "mismatch at client " << t << " key " << i;
    }
  }
}

// Write continuously while killing the leader mid-way.
TEST(StressTest, LeaderSwitchDuringLoad) {
  StressCluster c(3, 62200);
  ASSERT_NE(-1, c.wait_leader());

  // Phase 1: write some keys before the crash.
  KvClient cli = c.make_client();
  for (int i = 0; i < 50; ++i) {
    cli.put("pre" + std::to_string(i), "val" + std::to_string(i));
  }

  // Kill the current leader.
  int leader = c.find_leader();
  ASSERT_NE(-1, leader);
  c.kill_node(leader);

  // Wait for new leader.
  int new_leader = c.wait_leader();
  ASSERT_NE(-1, new_leader);

  // Phase 2: write more keys after leader switch.
  KvClient cli2 = c.make_client();
  for (int i = 0; i < 50; ++i) {
    cli2.put("post" + std::to_string(i), "val" + std::to_string(i));
  }

  // Verify all data.
  KvClient verifier = c.make_client();
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ("val" + std::to_string(i), verifier.get("pre" + std::to_string(i)))
        << "pre-crash key " << i << " lost";
    EXPECT_EQ("val" + std::to_string(i), verifier.get("post" + std::to_string(i)))
        << "post-crash key " << i << " lost";
  }
}

// Multiple clients concurrently Append to the same key.
// Total length must equal sum of all appended values.
TEST(StressTest, AppendUnderStress) {
  StressCluster c(3, 62300);
  ASSERT_NE(-1, c.wait_leader());

  const int num_clients = 4;
  const int appends_per_client = 50;
  // Each client appends a fixed-length token: "cX_YY," (always 6 chars).
  // Total expected length = num_clients * appends_per_client * 6.

  std::vector<std::thread> threads;
  for (int t = 0; t < num_clients; ++t) {
    threads.emplace_back([&c, t, appends_per_client]() {
      KvClient cli = c.make_client();
      for (int i = 0; i < appends_per_client; ++i) {
        // Fixed 6-char token: "cT_II," where T is client, II is zero-padded index.
        char buf[8];
        snprintf(buf, sizeof(buf), "c%d_%02d,", t, i);
        cli.append("shared", std::string(buf));
      }
    });
  }
  for (auto& th : threads) th.join();

  KvClient verifier = c.make_client();
  std::string result = verifier.get("shared");

  // Each token is 6 chars, total = 4 * 50 * 6 = 1200.
  size_t expected_len = static_cast<size_t>(num_clients) * appends_per_client * 6;
  ASSERT_EQ(expected_len, result.size())
      << "total append length mismatch; got: " << result.size();

  // Parse 6-char tokens and verify every expected token appears exactly once.
  std::set<std::string> seen;
  for (size_t i = 0; i + 6 <= result.size(); i += 6) {
    std::string token = result.substr(i, 6);
    EXPECT_EQ(0u, seen.count(token)) << "duplicate token: " << token;
    seen.insert(token);
  }
  for (int t = 0; t < num_clients; ++t) {
    for (int i = 0; i < appends_per_client; ++i) {
      char buf[8];
      snprintf(buf, sizeof(buf), "c%d_%02d,", t, i);
      EXPECT_EQ(1u, seen.count(std::string(buf)))
          << "missing token: " << buf;
    }
  }
}

// Multiple clients concurrently Append unique tokens to the SAME key.
// Verifies linearizability: all tokens must appear exactly once.
TEST(StressTest, ConcurrentSameKeyLinearizable) {
  StressCluster c(3, 62400);
  ASSERT_NE(-1, c.wait_leader());

  const int num_clients = 4;
  const int appends_per_client = 30;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_clients; ++t) {
    threads.emplace_back([&c, t, appends_per_client]() {
      KvClient cli = c.make_client();
      for (int i = 0; i < appends_per_client; ++i) {
        char buf[8];
        snprintf(buf, sizeof(buf), "c%d_%02d,", t, i);
        cli.append("linear", std::string(buf));
      }
    });
  }
  for (auto& th : threads) th.join();

  KvClient verifier = c.make_client();
  std::string result = verifier.get("linear");

  size_t expected_len = static_cast<size_t>(num_clients) * appends_per_client * 6;
  ASSERT_EQ(expected_len, result.size())
      << "total append length mismatch";

  // Parse 6-char tokens and verify each appears exactly once.
  std::set<std::string> seen;
  for (size_t i = 0; i + 6 <= result.size(); i += 6) {
    std::string token = result.substr(i, 6);
    EXPECT_EQ(0u, seen.count(token)) << "duplicate token: " << token;
    seen.insert(token);
  }
  for (int t = 0; t < num_clients; ++t) {
    for (int i = 0; i < appends_per_client; ++i) {
      char buf[8];
      snprintf(buf, sizeof(buf), "c%d_%02d,", t, i);
      EXPECT_EQ(1u, seen.count(std::string(buf)))
          << "missing token: " << buf;
    }
  }
}

}  // namespace
}  // namespace raftkv
/*
[2026-04-24 00:19:31.243]

[----------] Global test environment tear-down
[==========] 4 tests from 1 test suite ran. (95687 ms total)
[  PASSED  ] 4 tests.
*/