#pragma once

#include <mutex>
#include <string>

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// Persister — Raft state persistence abstraction
// ═════════════════════════════════════════════════════════════════
// Saves and loads Raft's persistent state (term, votedFor, log)
// and KV snapshot data to/from disk.
//
// Atomicity guarantee:
//   Each data file (raft_state / snapshot) is written to a versioned
//   path via write-tmp + rename. A manifest file records which
//   versions are current. The manifest is the sole commit point —
//   a crash before the manifest rename leaves the previous version
//   intact, so raft_state and snapshot are always consistent.
//
// File layout under data_dir:
//   {data_dir}/raft_state_{node_id}_v{N}.dat
//   {data_dir}/snapshot_{node_id}_v{M}.dat
//   {data_dir}/manifest_{node_id}.dat
//
class Persister {
 public:
  explicit Persister(int node_id, const std::string& data_dir);

  // Save Raft state (term + votedFor + log) and snapshot together.
  // Both files are committed via a single manifest update.
  void save(const std::string& raft_state, const std::string& snapshot);

  // Save only Raft state.
  void save_raft_state(const std::string& raft_state);

  // Save only snapshot.
  void save_snapshot(const std::string& snapshot);

  // Load from disk. Returns empty string if file doesn't exist.
  std::string load_raft_state();
  std::string load_snapshot();

  // Size of persisted raft state (for snapshot threshold check).
  int raft_state_size();

 private:
  void write_manifest();
  void gc_old_files(int old_raft_ver, int old_snap_ver);

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
};

}  // namespace raftkv
