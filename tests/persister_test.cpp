#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

#include "raft/persister.h"

// ── test fixture ──────────────────────────────────────────────────
class PersisterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/persister_test_XXXXXX";
    char* result = mkdtemp(tmpl);
    ASSERT_NE(result, static_cast<char*>(0)) << "Failed to create temp dir";
    dir_ = result;
  }

  void TearDown() override {
    std::string cmd = "rm -rf " + dir_;
    ::system(cmd.c_str());
  }

  // Returns a heap-allocated Persister to avoid triggering move construction
  // (std::mutex is not movable, so Persister cannot be returned by value).
  std::unique_ptr<raftkv::Persister> make_persister(int node_id = 0) {
    return std::unique_ptr<raftkv::Persister>(
        new raftkv::Persister(node_id, dir_));
  }

  std::string dir_;
};

// ── basic round-trip tests ────────────────────────────────────────

TEST_F(PersisterTest, EmptyOnFirstLoad) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  EXPECT_TRUE(p->load_raft_state().empty());
  EXPECT_TRUE(p->load_snapshot().empty());
}

TEST_F(PersisterTest, SaveAndLoadRaftState) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  const std::string state = "term=3,votedFor=1,logSize=42";
  p->save_raft_state(state);
  EXPECT_EQ(p->load_raft_state(), state);
}

TEST_F(PersisterTest, SaveAndLoadSnapshot) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  const std::string snap = "\x01\x02\x03\x04snapshot_payload";
  p->save_snapshot(snap);
  EXPECT_EQ(p->load_snapshot(), snap);
}

TEST_F(PersisterTest, SaveBothAtomically) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->save("raft_data", "snap_data");
  EXPECT_EQ(p->load_raft_state(), "raft_data");
  EXPECT_EQ(p->load_snapshot(),   "snap_data");
}

TEST_F(PersisterTest, RaftStateSizeMatchesBytes) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  const std::string state = "some_bytes_123";
  p->save_raft_state(state);
  EXPECT_EQ(p->raft_state_size(), static_cast<int>(state.size()));
}

TEST_F(PersisterTest, RaftStateSizeZeroWhenEmpty) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  EXPECT_EQ(p->raft_state_size(), 0);
}

// ── persistence across restart ────────────────────────────────────

TEST_F(PersisterTest, PersistsAcrossRestart) {
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->save("persisted_state", "persisted_snapshot");
  }  // destroyed here, simulating process exit

  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  EXPECT_EQ(p2->load_raft_state(), "persisted_state");
  EXPECT_EQ(p2->load_snapshot(),   "persisted_snapshot");
}

TEST_F(PersisterTest, OverwriteUpdatesState) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->save_raft_state("first");
  p->save_raft_state("second");
  EXPECT_EQ(p->load_raft_state(), "second");

  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  EXPECT_EQ(p2->load_raft_state(), "second");
}

// ── multi-node isolation ──────────────────────────────────────────

TEST_F(PersisterTest, DifferentNodesDontShareFiles) {
  raftkv::Persister p0(0, dir_);
  raftkv::Persister p1(1, dir_);

  p0.save_raft_state("node0_state");
  p1.save_raft_state("node1_state");

  EXPECT_EQ(p0.load_raft_state(), "node0_state");
  EXPECT_EQ(p1.load_raft_state(), "node1_state");
}

// ── binary safety ─────────────────────────────────────────────────

TEST_F(PersisterTest, BinaryDataRoundTrip) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  std::string binary;
  for (int i = 0; i < 256; ++i) {
    binary += static_cast<char>(i);
  }
  p->save_raft_state(binary);
  EXPECT_EQ(p->load_raft_state(), binary);
}

// ── manifest-based crash recovery ─────────────────────────────────

TEST_F(PersisterTest, ManifestCreatedAfterSave) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->save("raft_data", "snap_data");

  // Manifest file should exist
  std::string manifest = dir_ + "/manifest_0.dat";
  std::ifstream f(manifest.c_str());
  ASSERT_TRUE(f.good()) << "manifest file should exist after save()";
}

TEST_F(PersisterTest, CrashBeforeManifestRollsBack) {
  // Step 1: write version 1 via normal save
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->save("raft_v1", "snap_v1");
  }

  // Step 2: simulate a crash after writing new data files but BEFORE
  // updating the manifest.  Write v2 data files manually, but leave
  // the manifest pointing at v1.
  {
    std::ofstream rf((dir_ + "/raft_state_0_v2.dat").c_str(),
                     std::ios::binary);
    rf << "raft_v2_ORPHAN";
  }
  {
    std::ofstream sf((dir_ + "/snapshot_0_v2.dat").c_str(),
                     std::ios::binary);
    sf << "snap_v2_ORPHAN";
  }

  // Step 3: recover — should load v1 (manifest still says v1), and
  // GC should clean up the orphaned v2 files.
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  EXPECT_EQ(p2->load_raft_state(), "raft_v1");
  EXPECT_EQ(p2->load_snapshot(),   "snap_v1");

  // Verify orphan files were cleaned up
  std::ifstream rf2((dir_ + "/raft_state_0_v2.dat").c_str());
  EXPECT_FALSE(rf2.good()) << "orphaned raft v2 file should be cleaned up";
  std::ifstream sf2((dir_ + "/snapshot_0_v2.dat").c_str());
  EXPECT_FALSE(sf2.good()) << "orphaned snap v2 file should be cleaned up";
}

TEST_F(PersisterTest, IndependentVersionCounters) {
  // raft_state and snapshot versions advance independently
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->save_raft_state("r1");
  p->save_raft_state("r2");
  p->save_raft_state("r3");
  p->save_snapshot("s1");

  // Reload from disk — should recover correctly
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  EXPECT_EQ(p2->load_raft_state(), "r3");
  EXPECT_EQ(p2->load_snapshot(),   "s1");
}
