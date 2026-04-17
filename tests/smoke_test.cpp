#include <gtest/gtest.h>
#include "common/types.h"
#include "common/thread_safe_queue.h"
#include "common/config.h"

// ── types.h smoke tests ───────────────────────────────────────────

TEST(Types, LogEntryDefaultInit) {
  raftkv::LogEntry e;
  EXPECT_EQ(e.term,  0);
  EXPECT_EQ(e.index, 0);
  EXPECT_TRUE(e.command.empty());
}

TEST(Types, ApplyMsgBothFlagsDefaultFalse) {
  raftkv::ApplyMsg msg;
  EXPECT_FALSE(msg.command_valid);
  EXPECT_FALSE(msg.snapshot_valid);
}

TEST(Types, StartResultDefaultNotLeader) {
  raftkv::StartResult r;
  EXPECT_FALSE(r.is_leader);
}

TEST(Types, RoleName) {
  EXPECT_STREQ(raftkv::to_string(raftkv::Role::Leader),    "Leader");
  EXPECT_STREQ(raftkv::to_string(raftkv::Role::Follower),  "Follower");
  EXPECT_STREQ(raftkv::to_string(raftkv::Role::Candidate), "Candidate");
}

// ── ThreadSafeQueue smoke tests ───────────────────────────────────

TEST(TSQueue, PushPop) {
  raftkv::ThreadSafeQueue<int> q;
  q.push(42);
  int v = 0;
  bool ok = q.pop(v);
  ASSERT_TRUE(ok);
  EXPECT_EQ(v, 42);
}

TEST(TSQueue, TryPopTimeout) {
  raftkv::ThreadSafeQueue<int> q;
  int v = 0;
  bool ok = q.try_pop(v, std::chrono::milliseconds(10));
  EXPECT_FALSE(ok);
}

TEST(TSQueue, CloseUnblocksWaiter) {
  raftkv::ThreadSafeQueue<int> q;
  // Close immediately; pop() should return false without blocking.
  q.close();
  int v = 0;
  bool ok = q.pop(v);
  EXPECT_FALSE(ok);
}

TEST(TSQueue, PushAfterCloseIsNoOp) {
  raftkv::ThreadSafeQueue<int> q;
  q.close();
  q.push(99);    // should not crash or block
  int v = 0;
  bool ok = q.pop(v);
  EXPECT_FALSE(ok);
}

// ── RaftConfig defaults ───────────────────────────────────────────

TEST(Config, RaftConfigDefaults) {
  raftkv::RaftConfig cfg;
  EXPECT_LT(cfg.election_timeout_min_ms, cfg.election_timeout_max_ms);
  EXPECT_LT(cfg.heartbeat_interval_ms,   cfg.election_timeout_min_ms);
}
