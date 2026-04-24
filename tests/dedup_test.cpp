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
#include "kv.grpc.pb.h"

namespace raftkv {
namespace {

static std::string make_tmpdir() {
  char tmpl[] = "/tmp/raft_dedup_XXXXXX";
  char* p = ::mkdtemp(tmpl);
  return p ? std::string(p) : std::string("/tmp/raft_dedup_test");
}

// ── Per-node state ───────────────────────────────────────────────
struct DedupNodeState {
  std::shared_ptr<Raft>                      raft;
  std::shared_ptr<KvStore>                   kv_store;
  std::shared_ptr<RaftServiceImpl>           raft_service;
  std::shared_ptr<KvServiceImpl>             kv_service;
  std::unique_ptr<grpc::Server>              server;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> queue;
};

// ── DedupCluster ─────────────────────────────────────────────────
// 3-node cluster with raw gRPC helpers for controlling client_id
// and request_id (bypassing KvClient's auto-increment).
class DedupCluster {
 public:
  explicit DedupCluster(int base_port)
      : n_(3), base_port_(base_port), tmpdir_(make_tmpdir()) {
    for (int i = 0; i < n_; ++i)
      addrs_.push_back("127.0.0.1:" + std::to_string(base_port_ + i));

    RaftConfig rcfg;
    rcfg.election_timeout_min_ms = 150;
    rcfg.election_timeout_max_ms = 300;
    rcfg.heartbeat_interval_ms   = 50;
    rcfg.rpc_timeout_ms          = 100;
    rcfg.apply_interval_ms       = 10;

    nodes_.resize(n_);
    for (int i = 0; i < n_; ++i) {
      ServerConfig cfg;
      cfg.node_id     = i;
      cfg.listen_addr = addrs_[i];
      cfg.peer_addrs  = addrs_;
      cfg.data_dir    = tmpdir_;
      cfg.raft        = rcfg;

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

    // Create a shared stub for raw RPC calls.
    // Point at all servers; we'll retry on WrongLeader.
    for (int i = 0; i < n_; ++i) {
      stubs_.push_back(kv::KvService::NewStub(
          grpc::CreateChannel(addrs_[i], grpc::InsecureChannelCredentials())));
    }
  }

  ~DedupCluster() {
    for (int i = 0; i < n_; ++i) {
      auto& nd = nodes_[i];
      if (nd.server) { nd.server->Shutdown(); nd.server.reset(); }
      nd.kv_service.reset();
      nd.raft_service.reset();
      nd.kv_store.reset();
      nd.raft.reset();
      if (nd.queue) nd.queue->close();
    }
    ::system(("rm -rf " + tmpdir_).c_str());
  }

  int wait_leader(int max_wait_ms = 5000) {
    for (int waited = 0; waited < max_wait_ms; waited += 50) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      int leader = -1;
      bool multi = false;
      for (int i = 0; i < n_; ++i) {
        if (nodes_[i].raft && nodes_[i].raft->is_leader()) {
          if (leader != -1) { multi = true; break; }
          leader = i;
        }
      }
      if (leader != -1 && !multi) return leader;
    }
    return -1;
  }

  // ── Raw gRPC helpers with explicit client_id/request_id ──────
  // Retry across servers on WrongLeader, just like KvClient.

  std::string raw_put(const std::string& key, const std::string& value,
                      const std::string& client_id, int64_t request_id) {
    for (int attempt = 0; attempt < 30; ++attempt) {
      kv::PutAppendRequest req;
      req.set_key(key);
      req.set_value(value);
      req.set_op("Put");
      req.set_client_id(client_id);
      req.set_request_id(request_id);

      kv::PutAppendReply reply;
      grpc::ClientContext ctx;
      grpc::Status status = stubs_[leader_hint_]->PutAppend(&ctx, req, &reply);

      if (!status.ok() || reply.error() == ErrCode::ErrWrongLeader
                       || reply.error() == ErrCode::ErrTimeout) {
        leader_hint_ = (leader_hint_ + 1) % n_;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      return reply.error();
    }
    return ErrCode::ErrTimeout;
  }

  std::pair<std::string, std::string> raw_get(
      const std::string& key,
      const std::string& client_id, int64_t request_id) {
    for (int attempt = 0; attempt < 30; ++attempt) {
      kv::GetRequest req;
      req.set_key(key);
      req.set_client_id(client_id);
      req.set_request_id(request_id);

      kv::GetReply reply;
      grpc::ClientContext ctx;
      grpc::Status status = stubs_[leader_hint_]->Get(&ctx, req, &reply);

      if (!status.ok() || reply.error() == ErrCode::ErrWrongLeader
                       || reply.error() == ErrCode::ErrTimeout) {
        leader_hint_ = (leader_hint_ + 1) % n_;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      return {reply.value(), reply.error()};
    }
    return {"", ErrCode::ErrTimeout};
  }

 private:
  int n_;
  int base_port_;
  int leader_hint_ = 0;
  std::string tmpdir_;
  std::vector<std::string>                              addrs_;
  std::vector<DedupNodeState>                           nodes_;
  std::vector<std::unique_ptr<kv::KvService::Stub>>     stubs_;
};

// ── Tests ─────────────────────────────────────────────────────────

// Same client_id + request_id sent twice: second Put should be ignored.
TEST(DedupTest, DuplicatePutIgnored) {
  DedupCluster c(63000);
  ASSERT_NE(-1, c.wait_leader());

  // First put: x = "first"
  std::string err1 = c.raw_put("x", "first", "clientA", 1);
  EXPECT_EQ("", err1);

  // Different key to advance state, ensuring request_id=1 is recorded.
  c.raw_put("y", "other", "clientA", 2);

  // Now put x = "second" with a NEW request_id — this should succeed.
  c.raw_put("x", "second", "clientA", 3);
  auto r1 = c.raw_get("x", "clientA", 4);
  EXPECT_EQ("second", r1.first);

  // Replay: put x = "replayed" with the SAME request_id=3 — should be ignored.
  c.raw_put("x", "replayed", "clientA", 3);
  auto r2 = c.raw_get("x", "clientA", 5);
  EXPECT_EQ("second", r2.first) << "duplicate put should not change value";
}

// Duplicate Get returns the cached result from first execution,
// even if the underlying value has changed since then.
TEST(DedupTest, DuplicateGetReturnsCachedResult) {
  DedupCluster c(63100);
  ASSERT_NE(-1, c.wait_leader());

  c.raw_put("key", "old_val", "clientB", 1);

  // First Get: reads "old_val", result is cached in dedup table.
  auto r1 = c.raw_get("key", "clientB", 2);
  EXPECT_EQ("old_val", r1.first);

  // Overwrite the value via a different client.
  c.raw_put("key", "new_val", "clientC", 1);

  // Replay same Get (clientB, request_id=2): should return the cached
  // "old_val", NOT the current "new_val".
  auto r2 = c.raw_get("key", "clientB", 2);
  EXPECT_EQ("old_val", r2.first)
      << "duplicate Get should return cached result, not current value";
}

// Two different client_ids with the same request_id: NOT duplicates.
TEST(DedupTest, DifferentClientsSameRequestId) {
  DedupCluster c(63200);
  ASSERT_NE(-1, c.wait_leader());

  c.raw_put("k", "fromA", "clientA", 1);
  // ClientB with same request_id=1 should NOT be treated as dup.
  c.raw_put("k", "fromB", "clientB", 1);

  auto r = c.raw_get("k", "clientC", 1);
  EXPECT_EQ("fromB", r.first) << "different clients should not dedup";
}

// After request_id=5 is applied, request_id=3 from the same client
// should be treated as duplicate (monotonic dedup).
TEST(DedupTest, MonotonicRequestId) {
  DedupCluster c(63300);
  ASSERT_NE(-1, c.wait_leader());

  // Apply request_id=5 first.
  c.raw_put("k", "rid5", "clientM", 5);

  // Now send request_id=3 (lower) — should be treated as duplicate, value unchanged.
  c.raw_put("k", "rid3", "clientM", 3);
  auto r = c.raw_get("k", "clientM", 6);
  EXPECT_EQ("rid5", r.first) << "old request_id should be treated as duplicate";
}

}  // namespace
}  // namespace raftkv
