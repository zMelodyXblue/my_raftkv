#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/thread_safe_queue.h"
#include "common/types.h"

namespace raftkv {

class Persister;        // Forward declaration
class RaftPeerClient;   // Forward declaration

// ═════════════════════════════════════════════════════════════════
// Raft — Core consensus engine
// ═════════════════════════════════════════════════════════════════
// Manages leader election, log replication, and snapshot.
// Communicates with the KV state machine via apply_channel.
//
// Dependency boundary:
//   Raft knows nothing about gRPC or protobuf. All network I/O is
//   delegated to RaftPeerClient (outgoing) and handled via the
//   handle_*() methods below (incoming). The gRPC adapter
//   (RaftServiceImpl) owns the translation between proto types and
//   the internal DTOs defined in types.h.
//
// Lifecycle:
//   1. Construct with config + peers + persister + apply_channel
//   2. Call start() to submit commands (only succeeds on leader)
//   3. Applied commands arrive on apply_channel as ApplyMsg
//   4. Call snapshot() when KV layer wants to compact the log
//
class Raft : public std::enable_shared_from_this<Raft> {
 public:
  // ── Construction & Lifecycle ─────────────────────────────────
  Raft(const ServerConfig& config,
       std::vector<std::shared_ptr<RaftPeerClient>> peers,
       std::shared_ptr<Persister> persister,
       std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel);
  ~Raft();

  // Must be called after make_shared<Raft>() so that shared_from_this()
  // is valid.  Starts the background threads.
  void start_threads();

  Raft(const Raft&) = delete;
  Raft& operator=(const Raft&) = delete;

  // ── Client API ───────────────────────────────────────────────
  // Submit a command to the Raft log.  Returns immediately.
  // If this node is not the leader, is_leader=false and index/term
  // are meaningless — the caller must not wait on any channel.
  StartResult start(const std::string& command);

  // Tell Raft to compact logs up to (and including) `index`.
  // snapshot_data is the serialised KvStore state at that index.
  void snapshot(int index, const std::string& snapshot_data);

  // ── ReadIndex ─────────────────────────────────────────────────
  // Confirms leadership by sending a round of heartbeats, then returns
  // the current commit_index as a safe read point.
  // Returns >= 0 on success (the read index), or -1 if not leader or
  // leadership confirmation failed.
  int read_index();

  // ── State Queries ────────────────────────────────────────────
  int  current_term() const;
  bool is_leader() const;
  int  node_id() const { return config_.node_id; }
  int  raft_state_size() const;
  int  snapshot_index() const;
  std::string read_persisted_snapshot() const;

  // Aggregated status for REST gateway.
  struct RaftStatus {
    int  node_id;
    int  term;
    Role role;
    int  last_log_index;
    int  commit_index;
    int  last_applied;
    int  last_snapshot_index;
  };
  RaftStatus get_status() const;

  // ── Inbound RPC Handlers ─────────────────────────────────────
  // Called by RaftServiceImpl (the gRPC adapter) after it has
  // translated the protobuf request into the internal DTO.
  // These methods acquire mu_ internally; the caller must NOT hold
  // any lock when calling them.
  void handle_append_entries(const AppendEntriesArgs& args,
                             AppendEntriesReply*      reply);

  void handle_request_vote(const RequestVoteArgs& args,
                           RequestVoteReply*      reply);

  void handle_install_snapshot(const InstallSnapshotArgs& args,
                               InstallSnapshotReply*      reply);

 private:
  // ── Background Loops ─────────────────────────────────────────
  void election_ticker();   // Detects election timeout; starts election
  void replicator(int peer_id);  // Per-peer: sends AppendEntries/InstallSnapshot
  void applier();           // Pushes committed entries to apply_channel

  // ── Election ─────────────────────────────────────────────────
  void start_election();
  void sendRequestVote(int server, std::shared_ptr<RequestVoteArgs> args,
                       std::shared_ptr<int> voted_cnt);
  void become_follower(int term);   // Requires mu_ held
  void become_leader();             // Requires mu_ held

  // ── Log Replication ──────────────────────────────────────────
  void replicate_to_peer(int peer_id);     // Send AppendEntries or InstallSnapshot
  void send_append_entries(int peer_id);
  void send_install_snapshot(int peer_id);

  // ── Commit Index Advancement ─────────────────────────────────
  // Recompute commit_index_ after a successful AppendEntries ACK.
  void maybe_advance_commit_index();  // Requires mu_ held

  // ── Persistence ──────────────────────────────────────────────
  void persist();   // Save metadata only (term, votedFor, snapshot info); requires mu_ held
  void restore();   // Load metadata + WAL; call during construction

  // ── Log Helpers (all require mu_ held) ──────────────────────
  // Logical index: the index as seen by the rest of the system,
  //   counting from 1 (0 is reserved as the "before any log" sentinel).
  // Physical index: position in logs_[] vector.
  //   physical = logical - last_snapshot_index_
  int  last_log_index() const;
  int  last_log_term()  const;
  int  term_at(int logical_index) const;  // -1 if index is in snapshot
  int  to_physical(int logical_index) const;

  void reset_election_timer();  // Requires mu_ held

  // ── State ────────────────────────────────────────────────────
  ServerConfig config_;
  std::vector<std::shared_ptr<RaftPeerClient>> peers_;
  std::shared_ptr<Persister> persister_;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel_;

  mutable std::mutex mu_;

  // ── Persistent state (must survive crashes) ──────────────────
  int                   current_term_ = 0;
  int                   voted_for_    = -1;
  // logs_[0] is a sentinel: {command="", term=last_snapshot_term_,
  //   index=last_snapshot_index_}. Real entries start at logs_[1].
  // All access via the log helpers above to hide the physical offset.
  std::vector<LogEntry> logs_;

  // ── Volatile state ────────────────────────────────────────────
  Role role_         = Role::Follower;
  int  commit_index_ = 0;
  int  last_applied_ = 0;

  // ── Leader-only volatile state ────────────────────────────────
  std::vector<int> next_index_;   // Initialised to last_log_index+1 on election
  std::vector<int> match_index_;  // Initialised to 0 on election

  // ── Snapshot state ────────────────────────────────────────────
  int last_snapshot_index_ = 0;
  int last_snapshot_term_  = 0;

  // ── Timing ────────────────────────────────────────────────────
  std::chrono::steady_clock::time_point last_heartbeat_recv_;
  int election_timeout_ms_ = 0;   // Randomized on each reset

  // ── Condition variables ────────────────────────────────────────
  std::condition_variable commit_cv_;      // notify when commit_index_ advances
  std::condition_variable replicate_cv_;   // notify replicators on new entry or leader election
  std::condition_variable election_cv_;    // notify on shutdown or timer reset

  // ── Background threads ────────────────────────────────────────
  std::atomic<bool> running_{false};
  std::thread election_thread_;
  std::vector<std::thread> replicator_threads_;  // One per peer (except self)
  std::thread applier_thread_;
};

}  // namespace raftkv
