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
#include "rpc/grpc/grpc_raft_peer.h"
#include "rpc/grpc/grpc_raft_service.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_safety_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_safety_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct SafetyNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<RaftServiceImpl>           service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
  std::thread                                consumer;
  std::mutex                                 applied_mu;
  std::map<int, std::string>                 applied;
};

// ── SafetyCluster ────────────────────────────────────────────────
// Raft-level cluster with per-node kill/restart and apply-channel
// consumers.  Like ReplicationCluster but supports restart for
// testing crash-recovery scenarios.
class SafetyCluster {
 public:
  SafetyCluster(int n, int base_port)
      : n_(n), base_port_(base_port), tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));
    for (int i = 0; i < n_; ++i)
      nodes_.push_back(
          std::unique_ptr<SafetyNodeState>(new SafetyNodeState()));
    for (int i = 0; i < n_; ++i) start_node(i);
  }

  ~SafetyCluster() {
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

    auto* nd = nodes_[i].get();
    {
      std::lock_guard<std::mutex> lk(nd->applied_mu);
      nd->applied.clear();
    }
    nd->queue = std::make_shared<ThreadSafeQueue<ApplyMsg>>();

    std::vector<std::shared_ptr<RaftPeerClient>> peers;
    for (int j = 0; j < n_; ++j)
      peers.push_back(std::make_shared<GrpcRaftPeerClient>(addrs_[j]));

    auto persister = std::make_shared<Persister>(i, tmpdir_);
    nd->raft = std::make_shared<Raft>(cfg, std::move(peers),
                                      persister, nd->queue);
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

  void kill_node(int i) {
    auto* nd = nodes_[i].get();
    if (nd->server) { nd->server->Shutdown(); nd->server.reset(); }
    nd->service.reset();
    nd->raft.reset();
    if (nd->queue) nd->queue->close();
    if (nd->consumer.joinable()) nd->consumer.join();
  }

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

  // Submit a command to whichever node is leader.
  std::pair<int, int> submit(const std::string& cmd) {
    for (int i = 0; i < n_; ++i) {
      if (!nodes_[i]->raft) continue;
      StartResult r = nodes_[i]->raft->start(cmd);
      if (r.is_leader) return {i, r.index};
    }
    return {-1, -1};
  }

  // Wait for a specific node to apply the command at `index`.
  std::string wait_for_apply(int node_id, int index,
                             int timeout_ms = 5000) {
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

  // Check if any applied entry on a node has a given command value.
  bool has_applied_value(int node_id, const std::string& cmd) {
    auto* nd = nodes_[node_id].get();
    std::lock_guard<std::mutex> lk(nd->applied_mu);
    for (const auto& p : nd->applied) {
      if (p.second == cmd) return true;
    }
    return false;
  }

  Raft* raft(int i) { return nodes_[i]->raft.get(); }

  int n_;

 private:
  int         base_port_;
  std::string tmpdir_;
  std::vector<std::string>                          addrs_;
  std::vector<std::unique_ptr<SafetyNodeState>>     nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

// Log conflict override: uncommitted entries on an old leader must be
// overwritten when a new leader establishes a different entry at the
// same log index.
//
// This directly tests the Raft safety property: only one value can be
// committed at a given log index, and uncommitted entries from stale
// leaders are discarded.
//
// Scenario (5 nodes):
//   1. Leader L writes "stale_cmd" but can't commit (all followers killed)
//   2. L is killed (stale entry persisted in WAL)
//   3. Followers restart, elect new leader, commit "new_cmd"
//   4. L restarts — its stale entry must be overwritten by "new_cmd"
TEST(RaftSafetyTest, LogConflictOverride) {
  SafetyCluster c(5, 64000);
  int L = c.wait_leader();
  ASSERT_NE(-1, L);

  // Step 1: Kill all followers so L has no majority.
  for (int i = 0; i < 5; ++i) {
    if (i != L) c.kill_node(i);
  }

  // Submit "stale_cmd" — accepted by L but cannot be committed (1/5).
  auto stale = c.submit("stale_cmd");
  ASSERT_EQ(L, stale.first);

  // Ensure entry is persisted in L's WAL before killing.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Step 2: Kill L.  It has "stale_cmd" uncommitted in its log.
  c.kill_node(L);

  // Step 3: Restart all followers (their logs are empty).
  for (int i = 0; i < 5; ++i) {
    if (i != L) c.start_node(i);
  }
  int L2 = c.wait_leader();
  ASSERT_NE(-1, L2);
  ASSERT_NE(L, L2);

  // Submit "new_cmd" on the new leader — committed by 4-node majority.
  auto fresh = c.submit("new_cmd");
  ASSERT_NE(-1, fresh.first);
  int new_index = fresh.second;

  // Wait for all alive nodes to apply "new_cmd".
  for (int i = 0; i < 5; ++i) {
    if (i == L) continue;
    ASSERT_EQ("new_cmd", c.wait_for_apply(i, new_index))
        << "node " << i << " did not apply new_cmd";
  }

  // Step 4: Restart old leader — it must overwrite "stale_cmd".
  c.start_node(L);

  // Old leader catches up: AppendEntries from new leader overwrites its
  // conflicting entry and advances commit_index.
  EXPECT_EQ("new_cmd", c.wait_for_apply(L, new_index))
      << "old leader should have overwritten stale entry with new_cmd";

  // Verify "stale_cmd" was never applied on any node.
  for (int i = 0; i < 5; ++i) {
    if (!c.raft(i)) continue;
    EXPECT_FALSE(c.has_applied_value(i, "stale_cmd"))
        << "node " << i << " should not have applied stale_cmd";
  }
}

// Term increases monotonically across successive leader elections.
TEST(RaftSafetyTest, TermMonotonicAcrossElections) {
  SafetyCluster c(5, 64100);
  int L1 = c.wait_leader();
  ASSERT_NE(-1, L1);
  int term1 = c.raft(L1)->current_term();

  // Kill leader, trigger new election (4/5 alive).
  c.kill_node(L1);
  int L2 = c.wait_leader();
  ASSERT_NE(-1, L2);
  int term2 = c.raft(L2)->current_term();
  EXPECT_GT(term2, term1)
      << "new leader's term should be higher than old leader's term";

  // Kill again (3/5 alive — still has majority).
  c.kill_node(L2);
  int L3 = c.wait_leader();
  ASSERT_NE(-1, L3);
  int term3 = c.raft(L3)->current_term();
  EXPECT_GT(term3, term2);
}

// A committed entry must survive arbitrary leader changes.
TEST(RaftSafetyTest, CommittedEntryPersistsThroughLeaderChanges) {
  SafetyCluster c(5, 64200);
  int L1 = c.wait_leader();
  ASSERT_NE(-1, L1);

  // Submit and wait for all 5 nodes to apply.
  auto r = c.submit("persistent_cmd");
  ASSERT_NE(-1, r.first);
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ("persistent_cmd", c.wait_for_apply(i, r.second))
        << "node " << i << " did not apply persistent_cmd";
  }

  // Kill leader + 1 follower (3/5 alive).
  c.kill_node(L1);
  int other = (L1 + 1) % 5;
  c.kill_node(other);

  int L2 = c.wait_leader();
  ASSERT_NE(-1, L2);

  // Submit new command to prove cluster is operational.
  auto r2 = c.submit("after_leader_change");
  ASSERT_NE(-1, r2.first);
  for (int i = 0; i < 5; ++i) {
    if (i == L1 || i == other) continue;
    EXPECT_EQ("after_leader_change", c.wait_for_apply(i, r2.second))
        << "node " << i;
  }

  // Restart killed nodes — they must also eventually have the committed entry.
  c.start_node(L1);
  c.start_node(other);
  EXPECT_EQ("persistent_cmd", c.wait_for_apply(L1, r.second))
      << "restarted old leader should recover committed entry";
  EXPECT_EQ("persistent_cmd", c.wait_for_apply(other, r.second))
      << "restarted follower should recover committed entry";
}

}  // namespace
}  // namespace raftkv
