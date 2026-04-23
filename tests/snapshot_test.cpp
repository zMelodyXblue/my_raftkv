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
#include "rpc/raft_service.h"
#include "rpc/kv_service.h"
#include "client/kv_client.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_snap_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_snap_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct SnapNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<KvStore>                   kv_store;
  std::shared_ptr<RaftServiceImpl>           raft_service;
  std::shared_ptr<KvServiceImpl>             kv_service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
};

// ── SnapshotCluster ──────────────────────────────────────────────
// Like KvCluster but with:
//   - Configurable max_raft_state_bytes (low threshold to trigger snapshots)
//   - Per-node start/kill/restart for crash-recovery tests
class SnapshotCluster {
 public:
  SnapshotCluster(int n, int base_port, int max_raft_state = 200)
      : n_(n),
        base_port_(base_port),
        max_raft_state_(max_raft_state),
        tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));
    nodes_.resize(n_);
    for (int i = 0; i < n_; ++i) start_node(i);
  }

  ~SnapshotCluster() {
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
      peers.push_back(std::make_shared<RaftPeerClient>(addrs_[j]));

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

  void restart_node(int i) {
    kill_node(i);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    start_node(i);
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

  KvClient make_client() { return KvClient(addrs_); }

  Raft* raft(int i) { return nodes_[i].raft.get(); }

  int n_;

 private:
  int         base_port_;
  int         max_raft_state_;
  std::string tmpdir_;
  std::vector<std::string>    addrs_;
  std::vector<SnapNodeState>  nodes_;
};

// ── Tests ─────────────────────────────────────────────────────────

// Write enough data to trigger snapshot(s), verify all data is still correct.
// With max_raft_state_bytes=200, a snapshot should fire after ~5 commands.
// Writing 30 commands exercises multiple snapshot cycles.
TEST(SnapshotTest, BasicSnapshot) {
  SnapshotCluster c(3, 61000);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();

  // Write 30 keys — should trigger multiple snapshots.
  for (int i = 0; i < 30; ++i) {
    cli.put("k" + std::to_string(i), "v" + std::to_string(i));
  }

  // Verify all values survived the snapshot cycles.
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ("v" + std::to_string(i), cli.get("k" + std::to_string(i)))
        << "key k" << i << " incorrect after snapshot";
  }
}

// After snapshot, new writes and reads still work correctly.
// Exercises log offset calculations (to_physical) after truncation.
TEST(SnapshotTest, ContinueAfterSnapshot) {
  SnapshotCluster c(3, 61100);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();

  // Phase 1: write enough to trigger snapshot.
  for (int i = 0; i < 20; ++i) {
    cli.put("old" + std::to_string(i), "val" + std::to_string(i));
  }

  // Phase 2: write more data after snapshot has been taken.
  for (int i = 0; i < 20; ++i) {
    cli.put("new" + std::to_string(i), "fresh" + std::to_string(i));
  }

  // Verify both old and new data.
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ("val" + std::to_string(i), cli.get("old" + std::to_string(i)));
    EXPECT_EQ("fresh" + std::to_string(i), cli.get("new" + std::to_string(i)));
  }
}

// Append after snapshot: tests that the dedup table (last_request_id_)
// survives inside the snapshot.
TEST(SnapshotTest, AppendAfterSnapshot) {
  SnapshotCluster c(3, 61200);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();

  // Build up a value via repeated Append — triggers snapshot mid-way.
  for (int i = 0; i < 30; ++i) {
    cli.append("accum", std::to_string(i) + ",");
  }

  std::string expected;
  for (int i = 0; i < 30; ++i) expected += std::to_string(i) + ",";
  EXPECT_EQ(expected, cli.get("accum"));
}

// Kill all 3 nodes, restart all 3, verify all data persisted via snapshot.
TEST(SnapshotTest, FullClusterRestart) {
  SnapshotCluster c(3, 61300);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli = c.make_client();

  // Write enough to trigger snapshot.
  for (int i = 0; i < 30; ++i) {
    cli.put("persist" + std::to_string(i), "data" + std::to_string(i));
  }
  // Wait a moment for snapshot to flush to disk.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Kill all nodes.
  for (int i = 0; i < 3; ++i) c.kill_node(i);

  // Restart all nodes — they should recover from persisted raft state + snapshot.
  for (int i = 0; i < 3; ++i) c.start_node(i);
  ASSERT_NE(-1, c.wait_leader());

  KvClient cli2 = c.make_client();

  // All data must survive.
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ("data" + std::to_string(i),
              cli2.get("persist" + std::to_string(i)))
        << "key persist" << i << " lost after full restart";
  }

  // New writes must work after recovery.
  cli2.put("after_restart", "works");
  EXPECT_EQ("works", cli2.get("after_restart"));
}

// A stale follower that missed writes (and snapshots) must catch up
// via InstallSnapshot when it restarts.
//
// Scenario:
//   1. Kill node 2
//   2. Write enough data to trigger snapshot on nodes 0,1
//      → log is truncated, node 2's next_index points into the snapshot
//   3. Restart node 2
//      → Leader detects next_index[2] <= last_snapshot_index
//      → Sends InstallSnapshot instead of AppendEntries
//   4. Kill current leader to force election among node 2 + the other follower
//   5. Verify all data is accessible through the new leader
TEST(SnapshotTest, InstallSnapshot) {
  SnapshotCluster c(3, 61400);
  int leader1 = c.wait_leader();
  ASSERT_NE(-1, leader1);

  // Step 1: kill one follower.
  int victim = (leader1 == 2) ? 1 : 2;
  c.kill_node(victim);

  // Step 2: write enough to trigger snapshot on the remaining nodes.
  KvClient cli = c.make_client();
  for (int i = 0; i < 30; ++i) {
    cli.put("snap" + std::to_string(i), "val" + std::to_string(i));
  }
  // Give time for snapshot to complete.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Step 3: restart the stale follower.
  c.start_node(victim);
  // Give time for InstallSnapshot + catch-up.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  // Step 4: kill the current leader to force a new election.
  // The restarted node (victim) is now a valid candidate.
  int leader2 = c.wait_leader();
  ASSERT_NE(-1, leader2);
  c.kill_node(leader2);

  int leader3 = c.wait_leader();
  ASSERT_NE(-1, leader3);

  // Step 5: verify all data through the new leader.
  KvClient cli2 = c.make_client();
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ("val" + std::to_string(i),
              cli2.get("snap" + std::to_string(i)))
        << "key snap" << i << " missing after InstallSnapshot catch-up";
  }

  // New writes also work.
  cli2.put("post_install", "ok");
  EXPECT_EQ("ok", cli2.get("post_install"));
}

// Leader crash after snapshot: the new leader must have the snapshot data
// and be able to serve reads immediately.
TEST(SnapshotTest, LeaderCrashAfterSnapshot) {
  SnapshotCluster c(3, 61500);
  int leader1 = c.wait_leader();
  ASSERT_NE(-1, leader1);

  KvClient cli = c.make_client();
  for (int i = 0; i < 30; ++i) {
    cli.put("lc" + std::to_string(i), "v" + std::to_string(i));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Kill leader.
  c.kill_node(leader1);

  int leader2 = c.wait_leader();
  ASSERT_NE(-1, leader2);
  ASSERT_NE(leader1, leader2);

  KvClient cli2 = c.make_client();
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ("v" + std::to_string(i), cli2.get("lc" + std::to_string(i)))
        << "key lc" << i << " lost after leader crash";
  }
}

}  // namespace
}  // namespace raftkv
