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
// File layout under data_dir:
//   {data_dir}/raft_state_{node_id}.dat
//   {data_dir}/snapshot_{node_id}.dat
//
class Persister {
 public:
  explicit Persister(int node_id, const std::string& data_dir);

  // Save Raft state (term + votedFor + log) and snapshot atomically.
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
  int node_id_;
  std::string data_dir_;
  mutable std::mutex mu_;

  // In-memory cache of latest persisted data
  std::string raft_state_;
  std::string snapshot_;
};

}  // namespace raftkv
