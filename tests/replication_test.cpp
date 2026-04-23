#include <gtest/gtest.h>

#include <chrono>
#include <map>
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
#include "rpc/raft_service.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_repl_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_repl_test");
}

// ── Per-node state ───────────────────────────────────────────────
// Stored via unique_ptr because mutex and thread are non-movable.
struct NodeState {
  std::shared_ptr<Raft>                     raft;
  std::shared_ptr<RaftServiceImpl>          service;
  std::unique_ptr<grpc::Server>             server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;

  // Consumer thread drains queue and stores applied commands.
  std::thread consumer;
  std::mutex  applied_mu;
  std::map<int, std::string> applied;  // command_index -> command
};

// ── ReplicationCluster ───────────────────────────────────────────
// In-process N-node Raft cluster with apply-channel consumers.

class ReplicationCluster {
 public:
  ReplicationCluster(int n, int base_port)
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
      nodes_.push_back(std::unique_ptr<NodeState>(new NodeState()));

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
      nd->service = std::make_shared<RaftServiceImpl>(nd->raft);

      grpc::ServerBuilder builder;
      builder.AddListeningPort(addrs_[i], grpc::InsecureServerCredentials());
      builder.RegisterService(nd->service.get());
      nd->server = builder.BuildAndStart();

      nd->consumer = std::thread([nd]() {
        ApplyMsg msg;
        while (nd->queue->pop(msg)) {
          if (msg.command_valid) {
            std::lock_guard<std::mutex> lk(nd->applied_mu);
            nd->applied[msg.command_index] = msg.command;
          }
        }
      });
    }
  }

  ~ReplicationCluster() {
    for (int i = 0; i < n_; ++i) kill(i);
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  void kill(int i) {
    auto* nd = nodes_[i].get();
    if (nd->server) { nd->server->Shutdown(); nd->server.reset(); }
    nd->service.reset();
    nd->raft.reset();
    if (nd->queue) nd->queue->close();
    if (nd->consumer.joinable()) nd->consumer.join();
  }

  // Poll until exactly one leader is found. Returns node_id or -1.
  int check_one_leader(int max_wait_ms = 5000) {
    for (int waited = 0; waited < max_wait_ms; waited += 50) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      int leader = -1;
      bool bad = false;
      for (int i = 0; i < n_; ++i) {
        if (!nodes_[i]->raft) continue;
        if (nodes_[i]->raft->is_leader()) {
          if (leader != -1) { bad = true; break; }
          leader = i;
        }
      }
      if (leader != -1 && !bad) return leader;
    }
    return -1;
  }

  // Submit a command to whichever node is leader.
  // Returns {leader_id, log_index} or {-1, -1} if no leader.
  std::pair<int, int> submit(const std::string& cmd) {
    for (int i = 0; i < n_; ++i) {
      if (!nodes_[i]->raft) continue;
      StartResult r = nodes_[i]->raft->start(cmd);
      if (r.is_leader) return {i, r.index};
    }
    return {-1, -1};
  }

  // Wait for a specific node to apply the command at `index`.
  // Returns the command string, or "" on timeout.
  std::string wait_for_apply(int node_id, int index, int timeout_ms = 3000) {
    auto* nd = nodes_[node_id].get();
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      {
        std::lock_guard<std::mutex> lk(nd->applied_mu);
        auto it = nd->applied.find(index);
        if (it != nd->applied.end()) return it->second;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return "";
  }

  int n_;

 private:
  int         base_port_;
  std::string tmpdir_;
  std::vector<std::string>                addrs_;
  std::vector<std::unique_ptr<NodeState>> nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

// Submit one command, verify all 3 nodes apply it.
TEST(ReplicationTest, BasicAgreement) {
  ReplicationCluster c(3, 59600);
  int leader = c.check_one_leader();
  ASSERT_NE(-1, leader);

  auto result = c.submit("cmd1");
  ASSERT_EQ(leader, result.first);
  ASSERT_GT(result.second, 0);

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ("cmd1", c.wait_for_apply(i, result.second))
        << "node " << i << " did not apply cmd1";
  }
}

// Submit 5 commands sequentially, verify all applied in order on all nodes.
TEST(ReplicationTest, MultipleCommands) {
  ReplicationCluster c(3, 59700);
  ASSERT_NE(-1, c.check_one_leader());

  std::vector<std::pair<int, std::string>> committed;  // (index, command)
  for (int k = 0; k < 5; ++k) {
    std::string cmd = "cmd" + std::to_string(k);
    auto result = c.submit(cmd);
    ASSERT_NE(-1, result.first) << "no leader for cmd " << k;
    committed.push_back({result.second, cmd});
  }

  // Verify all nodes have all commands.
  for (auto& p : committed) {
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(p.second, c.wait_for_apply(i, p.first))
          << "node " << i << " missing index " << p.first;
    }
  }
}

// Kill one follower; the remaining 2/3 majority should still commit.
TEST(ReplicationTest, FailAgree) {
  ReplicationCluster c(3, 59800);
  int leader = c.check_one_leader();
  ASSERT_NE(-1, leader);

  int victim = (leader + 1) % 3;
  c.kill(victim);

  auto result = c.submit("cmd_after_kill");
  ASSERT_NE(-1, result.first);

  for (int i = 0; i < 3; ++i) {
    if (i == victim) continue;
    EXPECT_EQ("cmd_after_kill", c.wait_for_apply(i, result.second))
        << "live node " << i << " did not apply";
  }
}

// Kill 2 followers: no majority, command should NOT be committed.
TEST(ReplicationTest, FailNoAgree) {
  ReplicationCluster c(3, 59900);
  int leader = c.check_one_leader();
  ASSERT_NE(-1, leader);

  int f1 = (leader + 1) % 3;
  int f2 = (leader + 2) % 3;
  c.kill(f1);
  c.kill(f2);

  auto result = c.submit("no_commit");
  ASSERT_NE(-1, result.first);

  // Should NOT be applied after 500ms (no majority).
  EXPECT_EQ("", c.wait_for_apply(leader, result.second, 500));
}

// Submit before and after a leader crash.
// New leader should commit new commands; old committed commands survive.
TEST(ReplicationTest, LeaderCrashThenNewCommands) {
  ReplicationCluster c(3, 60000);
  int leader1 = c.check_one_leader();
  ASSERT_NE(-1, leader1);

  // Submit and wait for full commit before crash.
  auto r1 = c.submit("before_crash");
  ASSERT_NE(-1, r1.first);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ("before_crash", c.wait_for_apply(i, r1.second))
        << "pre-crash commit failed on node " << i;
  }

  c.kill(leader1);

  int leader2 = c.check_one_leader();
  ASSERT_NE(-1, leader2);
  ASSERT_NE(leader1, leader2);

  auto r2 = c.submit("after_crash");
  ASSERT_NE(-1, r2.first);
  for (int i = 0; i < 3; ++i) {
    if (i == leader1) continue;
    EXPECT_EQ("after_crash", c.wait_for_apply(i, r2.second))
        << "post-crash commit failed on node " << i;
  }
}

}  // namespace
}  // namespace raftkv
