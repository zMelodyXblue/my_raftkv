#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
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

TEST_F(PersisterTest, RaftStateSizeMatchesWalSize) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  EXPECT_EQ(p->raft_state_size(), 0);
  raftkv::LogEntry e;
  e.index = 1; e.term = 1; e.command = "some_bytes_123";
  p->append_logs({e});
  EXPECT_EQ(p->raft_state_size(), p->wal_size());
  EXPECT_GT(p->raft_state_size(), 0);
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

// ── WAL interface tests ──────────────────────────────────────────

static raftkv::LogEntry make_entry(int index, int term,
                                   const std::string& cmd = "") {
  raftkv::LogEntry e;
  e.index = index;
  e.term = term;
  e.command = cmd.empty() ? ("cmd_" + std::to_string(index)) : cmd;
  return e;
}

TEST_F(PersisterTest, WalSaveAndLoadMeta) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->save_meta(5, 2, 10, 3);

  int term, voted_for, snap_idx, snap_term;
  ASSERT_TRUE(p->load_meta(&term, &voted_for, &snap_idx, &snap_term));
  EXPECT_EQ(term, 5);
  EXPECT_EQ(voted_for, 2);
  EXPECT_EQ(snap_idx, 10);
  EXPECT_EQ(snap_term, 3);
}

TEST_F(PersisterTest, WalLoadMetaEmptyReturnsFalse) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  int a, b, c, d;
  EXPECT_FALSE(p->load_meta(&a, &b, &c, &d));
}

TEST_F(PersisterTest, WalMetaPersistsAcrossRestart) {
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->save_meta(7, -1, 0, 0);
  }
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  int term, voted_for, snap_idx, snap_term;
  ASSERT_TRUE(p2->load_meta(&term, &voted_for, &snap_idx, &snap_term));
  EXPECT_EQ(term, 7);
  EXPECT_EQ(voted_for, -1);
}

TEST_F(PersisterTest, WalAppendAndLoadLogs) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  std::vector<raftkv::LogEntry> entries = {
    make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)
  };
  p->append_logs(entries);

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 3u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[0].term, 1);
  EXPECT_EQ(loaded[0].command, "cmd_1");
  EXPECT_EQ(loaded[2].index, 3);
  EXPECT_EQ(loaded[2].term, 2);
}

TEST_F(PersisterTest, WalAppendIncrementally) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->append_logs({make_entry(1, 1)});
  p->append_logs({make_entry(2, 1), make_entry(3, 1)});

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 3u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[2].index, 3);
}

TEST_F(PersisterTest, WalLogsPersistAcrossRestart) {
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->append_logs({make_entry(1, 1), make_entry(2, 1)});
  }
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  std::vector<raftkv::LogEntry> loaded = p2->load_logs();
  ASSERT_EQ(loaded.size(), 2u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[1].index, 2);
}

TEST_F(PersisterTest, WalTruncateSuffix) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->append_logs({make_entry(1, 1), make_entry(2, 1), make_entry(3, 2),
                  make_entry(4, 2)});
  p->truncate_suffix(3);  // Remove index 3 and 4

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 2u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[1].index, 2);
}

TEST_F(PersisterTest, WalTruncateSuffixPersists) {
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->append_logs({make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)});
    p->truncate_suffix(2);
  }
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  std::vector<raftkv::LogEntry> loaded = p2->load_logs();
  ASSERT_EQ(loaded.size(), 1u);
  EXPECT_EQ(loaded[0].index, 1);
}

TEST_F(PersisterTest, WalTruncateSuffixThenAppend) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->append_logs({make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)});
  p->truncate_suffix(2);
  p->append_logs({make_entry(2, 3), make_entry(3, 3)});

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 3u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[0].term, 1);
  EXPECT_EQ(loaded[1].index, 2);
  EXPECT_EQ(loaded[1].term, 3);
  EXPECT_EQ(loaded[2].index, 3);
  EXPECT_EQ(loaded[2].term, 3);
}

TEST_F(PersisterTest, WalTruncatePrefix) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->append_logs({make_entry(1, 1), make_entry(2, 1), make_entry(3, 2),
                  make_entry(4, 2)});
  p->truncate_prefix(3);  // Remove entries with index < 3

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 2u);
  EXPECT_EQ(loaded[0].index, 3);
  EXPECT_EQ(loaded[1].index, 4);
}

TEST_F(PersisterTest, WalTruncatePrefixPersists) {
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->append_logs({make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)});
    p->truncate_prefix(2);
  }
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  std::vector<raftkv::LogEntry> loaded = p2->load_logs();
  ASSERT_EQ(loaded.size(), 2u);
  EXPECT_EQ(loaded[0].index, 2);
  EXPECT_EQ(loaded[1].index, 3);
}

TEST_F(PersisterTest, WalTruncatePrefixAll) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  p->append_logs({make_entry(1, 1), make_entry(2, 1)});
  p->truncate_prefix(5);  // Remove everything

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  EXPECT_TRUE(loaded.empty());
}

TEST_F(PersisterTest, WalSizeTracking) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  EXPECT_EQ(p->wal_size(), 0);

  p->append_logs({make_entry(1, 1)});
  EXPECT_GT(p->wal_size(), 0);

  int size_after_one = p->wal_size();
  p->append_logs({make_entry(2, 1)});
  EXPECT_GT(p->wal_size(), size_after_one);
}

TEST_F(PersisterTest, WalIncompleteRecordTruncated) {
  // Write valid entries, then append garbage (simulating crash mid-write)
  {
    std::unique_ptr<raftkv::Persister> p = make_persister();
    p->append_logs({make_entry(1, 1), make_entry(2, 1)});
  }
  // Append incomplete record to WAL file
  std::string wal = dir_ + "/raft_wal_0.log";
  {
    std::ofstream f(wal.c_str(), std::ios::binary | std::ios::app);
    char garbage[] = {0x10, 0x00, 0x00, 0x00};  // length=16 but no payload
    f.write(garbage, 4);
  }
  // Recover — should have only the 2 valid entries
  std::unique_ptr<raftkv::Persister> p2 = make_persister();
  std::vector<raftkv::LogEntry> loaded = p2->load_logs();
  ASSERT_EQ(loaded.size(), 2u);
  EXPECT_EQ(loaded[0].index, 1);
  EXPECT_EQ(loaded[1].index, 2);
}

TEST_F(PersisterTest, WalEmptyLoadReturnsEmpty) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  EXPECT_TRUE(loaded.empty());
}

TEST_F(PersisterTest, WalLargePayload) {
  std::unique_ptr<raftkv::Persister> p = make_persister();
  std::string big_cmd(4096, 'X');
  p->append_logs({make_entry(1, 1, big_cmd)});

  std::vector<raftkv::LogEntry> loaded = p->load_logs();
  ASSERT_EQ(loaded.size(), 1u);
  EXPECT_EQ(loaded[0].command, big_cmd);
}
