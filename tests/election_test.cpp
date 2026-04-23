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
#include "rpc/raft_service.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_election_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_election_test");
}

// ── TestCluster ──────────────────────────────────────────────────────
// Manages N in-process Raft nodes connected via real gRPC on loopback.
// Uses fast timeouts so tests complete quickly.

class TestCluster {
 public:
  TestCluster(int n, int base_port)
      : n_(n), base_port_(base_port), tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));

    // Fast timing for tests
    RaftConfig rcfg;
    rcfg.election_timeout_min_ms = 150;
    rcfg.election_timeout_max_ms = 300;
    rcfg.heartbeat_interval_ms   = 50;
    rcfg.rpc_timeout_ms          = 100;

    rafts_.resize(n_);
    services_.resize(n_);
    servers_.resize(n_);

    for (int i = 0; i < n_; ++i) {
      ServerConfig cfg;
      cfg.node_id     = i;
      cfg.listen_addr = addrs_[i];
      cfg.peer_addrs  = addrs_;
      cfg.data_dir    = tmpdir_;
      cfg.raft        = rcfg;

      std::vector<std::shared_ptr<RaftPeerClient>> peers;
      for (int j = 0; j < n_; ++j)
        peers.push_back(std::make_shared<GrpcRaftPeerClient>(addrs_[j]));

      auto persister = std::make_shared<Persister>(i, tmpdir_);
      auto queue = std::make_shared<ThreadSafeQueue<ApplyMsg>>();
      auto raft  = std::make_shared<Raft>(cfg, std::move(peers), persister, queue);
      raft->start_threads();
      auto svc   = std::make_shared<RaftServiceImpl>(raft);

      grpc::ServerBuilder builder;
      builder.AddListeningPort(addrs_[i], grpc::InsecureServerCredentials());
      builder.RegisterService(svc.get());

      rafts_[i]    = std::move(raft);
      services_[i] = std::move(svc);
      servers_[i]  = builder.BuildAndStart();
    }
  }

  ~TestCluster() {
    for (int i = 0; i < n_; ++i) kill(i);
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  // Simulate a node crash: shut down its gRPC server and destroy the Raft object.
  void kill(int i) {
    if (servers_[i]) {
      servers_[i]->Shutdown();
      servers_[i].reset();
    }
    services_[i].reset();
    rafts_[i].reset();
  }

  // Poll until exactly one leader is found, or max_wait_ms is exceeded.
  // Returns the leader's node_id, or -1.
  int check_one_leader(int max_wait_ms = 5000) {
    for (int waited = 0; waited < max_wait_ms; waited += 50) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      int  leader   = -1;
      bool multiple = false;
      for (int i = 0; i < n_; ++i) {
        if (!rafts_[i]) continue;
        if (rafts_[i]->is_leader()) {
          if (leader != -1) { multiple = true; break; }
          leader = i;
        }
      }
      if (leader != -1 && !multiple) return leader;
    }
    return -1;
  }

  int current_term(int i) const {
    return rafts_[i] ? rafts_[i]->current_term() : -1;
  }

  int n_;

 private:
  int         base_port_;
  std::string tmpdir_;
  std::vector<std::string>                      addrs_;
  std::vector<std::shared_ptr<Raft>>            rafts_;
  std::vector<std::shared_ptr<RaftServiceImpl>> services_;
  std::vector<std::unique_ptr<grpc::Server>>    servers_;
};

// ── Tests ─────────────────────────────────────────────────────────────

// 1-node cluster: the single node votes for itself and wins immediately.
TEST(ElectionTest, SingleNodeBecomesLeader) {
  TestCluster c(1, 59100);
  EXPECT_NE(-1, c.check_one_leader());
}

// 3-node cluster: a majority can be reached, so one leader should emerge.
TEST(ElectionTest, ThreeNodesElectOneLeader) {
  TestCluster c(3, 59200);
  EXPECT_NE(-1, c.check_one_leader());
}

// After a leader is elected, heartbeats keep followers from timing out.
// The same leader should remain after 2 seconds.
TEST(ElectionTest, LeaderIsStable) {
  TestCluster c(3, 59300);
  int leader1 = c.check_one_leader();
  ASSERT_NE(-1, leader1);
  int term1 = c.current_term(leader1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  int leader2 = c.check_one_leader(1000);
  EXPECT_EQ(leader1, leader2);
  EXPECT_EQ(term1, c.current_term(leader2));
}

// When the leader crashes, the two remaining nodes have a majority (2/3)
// and elect a new leader.
TEST(ElectionTest, ReElectionAfterLeaderCrash) {
  TestCluster c(3, 59400);
  int leader1 = c.check_one_leader();
  ASSERT_NE(-1, leader1);

  c.kill(leader1);

  int leader2 = c.check_one_leader();
  EXPECT_NE(-1, leader2);
  EXPECT_NE(leader1, leader2);
}

// 5-node cluster: majority is 3; one leader should be elected.
TEST(ElectionTest, FiveNodesElectOneLeader) {
  TestCluster c(5, 59500);
  EXPECT_NE(-1, c.check_one_leader());
}

}  // namespace
}  // namespace raftkv
