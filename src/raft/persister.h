#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "common/types.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// Persister — Raft state persistence abstraction
// ═════════════════════════════════════════════════════════════════
// Two persistence modes coexist:
//
// 1. WAL mode (primary, used by Raft):
//    - Metadata (term, votedFor, snapshot info): full overwrite via save_meta()
//    - Log entries: append-only WAL via append_logs() / truncate_*()
//    - Snapshot: versioned file via save_snapshot()
//
// 2. Legacy blob mode (retained for snapshot's atomic dual-write):
//    - save() writes both raft_state and snapshot atomically
//    - save_raft_state() / load_raft_state() are no longer called
//      by Raft but remain available for compatibility.
//
// File layout under data_dir:
//   {data_dir}/raft_meta_{node_id}.dat       (WAL metadata)
//   {data_dir}/raft_wal_{node_id}.log        (WAL log entries)
//   {data_dir}/snapshot_{node_id}_v{N}.dat   (versioned snapshot)
//   {data_dir}/manifest_{node_id}.dat        (snapshot version tracking)
//
class Persister {
 public:
  explicit Persister(int node_id, const std::string& data_dir);
  virtual ~Persister();

  // ── Legacy blob API (retained for snapshot writes) ────────────
  virtual void save(const std::string& raft_state, const std::string& snapshot);
  virtual void save_raft_state(const std::string& raft_state);
  virtual void save_snapshot(const std::string& snapshot);
  virtual std::string load_raft_state();
  virtual std::string load_snapshot();
  virtual int raft_state_size();  // Delegates to wal_size()

  // ── WAL interface (incremental persistence) ──────────────────
  // Metadata: term + votedFor + snapshot metadata.  Small, full overwrite.
  virtual void save_meta(int term, int voted_for,
                         int last_snapshot_index, int last_snapshot_term);
  virtual bool load_meta(int* term, int* voted_for,
                         int* last_snapshot_index, int* last_snapshot_term);

  // WAL log operations: append-only with truncation support.
  virtual void append_logs(const std::vector<LogEntry>& entries);
  virtual void truncate_suffix(int from_index);   // Remove entries with index >= from_index
  virtual void truncate_prefix(int to_index);     // Remove entries with index < to_index (snapshot compaction)
  virtual std::vector<LogEntry> load_logs();

  // Size of WAL file in bytes (replaces raft_state_size for threshold).
  virtual int wal_size();

 private:
  void write_manifest();
  void gc_old_files(int old_raft_ver, int old_snap_ver);

  // WAL helpers
  std::string meta_path() const;
  std::string wal_path() const;
  void recover_wal();
  void rewrite_wal(const std::vector<LogEntry>& entries);
  std::vector<LogEntry> load_logs_internal();  // Requires mu_ held

  int node_id_;
  std::string data_dir_;
  mutable std::mutex mu_;

  // Monotonic version counters for each data file.
  // Version 0 means "no file written yet".
  int raft_version_;
  int snap_version_;

  // In-memory cache of latest persisted data
  std::string raft_state_;
  std::string snapshot_;

  // WAL state
  std::vector<int64_t> wal_offsets_;   // File offset of each WAL record
  int wal_start_index_;                // Logical index of first WAL entry
  int64_t wal_file_size_;              // Current WAL file size in bytes
};

}  // namespace raftkv
