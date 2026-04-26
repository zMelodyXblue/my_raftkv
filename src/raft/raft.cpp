#include "raft/raft.h"

#include <algorithm>
#include <random>
#include <spdlog/spdlog.h>

#include "common/raft_peer.h"
#include "raft/persister.h"

// Raft no longer touches protobuf directly — serialization is handled
// by the Persister (WAL incremental persistence).

namespace raftkv {

// ── Constructor ───────────────────────────────────────────────────

Raft::Raft(const ServerConfig& config,
           std::vector<std::shared_ptr<RaftPeerClient>> peers,
           std::shared_ptr<Persister> persister,
           std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel)
    : config_(config),
      peers_(std::move(peers)),
      persister_(std::move(persister)),
      apply_channel_(std::move(apply_channel)) {
}

// Must be called after make_shared<Raft>() so that shared_from_this() is valid.
// Initializes log sentinel, restores persisted state, starts background threads.
void Raft::start_threads() {
  LogEntry temp_log0;
  temp_log0.index = temp_log0.term = 0;
  temp_log0.command = "";
  logs_.push_back(temp_log0);
  restore();
  reset_election_timer();
  running_ = true;
  election_thread_ = std::thread(&Raft::election_ticker, this);
  for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
    if (i == config_.node_id) continue;
    replicator_threads_.emplace_back(&Raft::replicator, this, i);
  }
  applier_thread_ = std::thread(&Raft::applier, this);
}

// ── Destructor ────────────────────────────────────────────────────

Raft::~Raft() {
  running_ = false;
  // Wake all background threads so they exit without waiting for sleep timeout.
  commit_cv_.notify_all();
  replicate_cv_.notify_all();
  election_cv_.notify_all();
  if (election_thread_.joinable()) election_thread_.join();
  for (auto& t : replicator_threads_) {
    if (t.joinable()) t.join();
  }
  if (applier_thread_.joinable()) applier_thread_.join();
}

// ── State Queries ─────────────────────────────────────────────────

int Raft::current_term() const {
  std::lock_guard<std::mutex> lock(mu_);
  return current_term_;
}

bool Raft::is_leader() const {
  std::lock_guard<std::mutex> lock(mu_);
  return role_ == Role::Leader;
}

int Raft::raft_state_size() const {
  return persister_->wal_size();
}

int Raft::snapshot_index() const {
  std::lock_guard<std::mutex> lock(mu_);
  return last_snapshot_index_;
}

std::string Raft::read_persisted_snapshot() const {
  return persister_->load_snapshot();
}

Raft::RaftStatus Raft::get_status() const {
  std::lock_guard<std::mutex> lock(mu_);
  RaftStatus s;
  s.node_id            = config_.node_id;
  s.term               = current_term_;
  s.role               = role_;
  s.last_log_index     = last_log_index();
  s.commit_index       = commit_index_;
  s.last_applied       = last_applied_;
  s.last_snapshot_index = last_snapshot_index_;
  return s;
}

// ── Log Helpers ───────────────────────────────────────────────────
// All helpers below require mu_ to be held by the caller.

// Physical index = position in logs_[].
// logs_[0] is the sentinel at last_snapshot_index_.
int Raft::to_physical(int logical_index) const {
  return logical_index - last_snapshot_index_;
}

// Logical index of the last entry.
int Raft::last_log_index() const {
    return last_snapshot_index_ + (logs_.size() - 1);
}

// Term of the last log entry.
int Raft::last_log_term() const {
    return logs_.back().term;
}

// Term of the entry at logical_index. If the entry is in the snapshot,
// returns last_snapshot_term_.
int Raft::term_at(int logical_index) const {
    if (logical_index > last_snapshot_index_) {
        return logs_[to_physical(logical_index)].term;
    }
    return last_snapshot_term_;
}

// Pick a random timeout in [min, max] and reset the clock.
void Raft::reset_election_timer() {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(config_.raft.election_timeout_min_ms, config_.raft.election_timeout_max_ms);

    election_timeout_ms_ = dist(rng);
    last_heartbeat_recv_ = std::chrono::steady_clock::now();
    election_cv_.notify_one();  // wake election_ticker to recalculate remaining time
}

// ── Role Transitions ──────────────────────────────────────────────
// Requires mu_ held.

// Called whenever we see a term higher than ours.
// Updates term, resets vote, persists, resets election timer.
void Raft::become_follower(int term) {
  current_term_ = term;
  role_ = Role::Follower;
  voted_for_ = -1;
  persist();
  reset_election_timer();
  spdlog::info("[Node {}] [term={}] [Follower] stepped down", config_.node_id, current_term_);
}

// Called when this node wins an election.
// Initializes leader-only volatile state (nextIndex, matchIndex).
// Does NOT persist — role is volatile state.
void Raft::become_leader() {
  role_ = Role::Leader;
  int n = static_cast<int>(peers_.size());
  next_index_.assign(n, last_log_index() + 1);
  match_index_.assign(n, 0);
  spdlog::info("[Node {}] [term={}] [Leader] became leader", config_.node_id, current_term_);
}

// ── Election ──────────────────────────────────────────────────────

// Increment term, vote for self, send RequestVote to all peers in parallel.
// Each peer's reply is handled in a detached thread (sendRequestVote).
void Raft::start_election() {
  auto args = std::make_shared<RequestVoteArgs>();
  {
    std::lock_guard<std::mutex> lock(mu_);
    current_term_++;
    role_ = Role::Candidate;
    voted_for_ = config_.node_id;
    persist();
    reset_election_timer();
    args->term           = current_term_;
    args->candidate_id   = config_.node_id;
    args->last_log_index = last_log_index();
    args->last_log_term  = last_log_term();
    spdlog::info("[Node {}] [term={}] [Candidate] starting election", config_.node_id, current_term_);
  }

  auto voted_cnt = std::make_shared<int>(1);
  if (static_cast<int>(peers_.size()) == 1) {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ == Role::Candidate && current_term_ == args->term) {
      become_leader();
    }
    return;
  }
  std::shared_ptr<Raft> self;
  try { self = shared_from_this(); } catch (const std::bad_weak_ptr&) { return; }
  for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
    if (i == config_.node_id) continue;
    std::thread([self, i, args, voted_cnt]() {
      self->sendRequestVote(i, args, voted_cnt);
    }).detach();
  }
}

// Process a single RequestVote reply. Re-acquires mu_ and checks for staleness.
void Raft::sendRequestVote(int server,
                           std::shared_ptr<RequestVoteArgs> args,
                           std::shared_ptr<int> voted_cnt) {
  RequestVoteReply reply;
  bool ok = peers_[server]->request_vote(*args, &reply,
                                         config_.raft.rpc_timeout_ms);
  if (!ok) return;

  std::lock_guard<std::mutex> lock(mu_);
  if (role_ != Role::Candidate || current_term_ != args->term) return;

  if (reply.term > current_term_) {
    become_follower(reply.term);
    return;
  }
  if (reply.vote_granted) {
    ++(*voted_cnt);
    if (*voted_cnt > static_cast<int>(peers_.size()) / 2) {
      become_leader();
      replicate_cv_.notify_all();
    }
  }
}

// Handle an incoming RequestVote RPC.
// Grants vote if: (1) haven't voted or already voted for this candidate,
// AND (2) candidate's log is at least as up-to-date as ours (§5.4.1).
void Raft::handle_request_vote(const RequestVoteArgs& args,
                               RequestVoteReply*      reply) {
  std::lock_guard<std::mutex> lock(mu_);

  if (args.term > current_term_) {
    become_follower(args.term);
  } else if (args.term < current_term_) {
    reply->term = current_term_;
    reply->vote_granted = false;
    return;
  }
  reply->term = current_term_;
  if ((voted_for_ != -1 && voted_for_ != args.candidate_id) ||
      (args.last_log_term < last_log_term()) ||
      (args.last_log_term == last_log_term() && args.last_log_index < last_log_index())) {
    reply->vote_granted = false;
    spdlog::debug("[Node {}] [term={}] [{}] rejected vote for Node {}",
                  config_.node_id, current_term_, to_string(role_), args.candidate_id);
    return;
  }

  voted_for_ = args.candidate_id;
  persist();
  reset_election_timer();
  reply->vote_granted = true;
  spdlog::debug("[Node {}] [term={}] [Follower] voted for Node {}",
                config_.node_id, current_term_, args.candidate_id);
}

// Background thread: waits until election timeout expires, then starts election.
// Calculates remaining time to sleep precisely instead of polling every 10ms.
void Raft::election_ticker() {
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      // Calculate how long until the election timeout fires.
      auto remaining = [this] {
        auto elapsed = std::chrono::steady_clock::now() - last_heartbeat_recv_;
        auto timeout = std::chrono::milliseconds(election_timeout_ms_);
        auto left = timeout - std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        return (left.count() > 0) ? left : std::chrono::milliseconds(0);
      };
      // Wait exactly the remaining time. Wakes early on shutdown or
      // when reset_election_timer() is called (heartbeat received).
      election_cv_.wait_for(lock, remaining(),
                            [this] { return !running_; });
      if (!running_) break;
      if (role_ == Role::Leader) continue;
      // Re-check after wakeup: timer may have been reset while we waited.
      if (remaining().count() > 0) continue;
    }
    start_election();
  }
}

// ── Replication ───────────────────────────────────────────────────

// Per-peer background thread: waits on replicate_cv_ for new entries or
// heartbeat timeout, then sends AppendEntries/InstallSnapshot.
// Replaces the old heartbeat_ticker + detached-thread-per-RPC model.
void Raft::replicator(int peer_id) {
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      // Wait for: (1) new entries to replicate, (2) heartbeat timeout, or (3) shutdown.
      // Spurious wakeups are harmless — an extra replicate attempt is a no-op.
      replicate_cv_.wait_for(lock,
          std::chrono::milliseconds(config_.raft.heartbeat_interval_ms));
      if (!running_) break;
      if (role_ != Role::Leader) continue;
    }
    replicate_to_peer(peer_id);
  }
}

// Send AppendEntries (with log entries) to one peer.
// On success: advance next_index_ / match_index_, try to advance commit_index_.
// On failure: backtrack next_index_ using the fast-backoff info from the reply.
void Raft::send_append_entries(int peer_id) {
  AppendEntriesArgs args;
  args.leader_id = config_.node_id;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) return;
    args.term = current_term_;
    args.prev_log_index = next_index_[peer_id] - 1;
    args.prev_log_term = term_at(args.prev_log_index);
    args.leader_commit = commit_index_;
    for (int i = next_index_[peer_id], I = last_log_index(); i <= I; ++i) {
      args.entries.push_back(logs_[to_physical(i)]);
    }
  }

  AppendEntriesReply reply;
  bool ok = peers_[peer_id]->append_entries(args, &reply, config_.raft.rpc_timeout_ms);
  if (!ok) return;

  std::lock_guard<std::mutex> lock(mu_);
  if (reply.term > current_term_) {
    become_follower(reply.term);
    return;
  }
  if (role_ != Role::Leader || current_term_ != args.term) return;

  if (reply.success) {
    int new_match = args.prev_log_index + static_cast<int>(args.entries.size());
    match_index_[peer_id] = std::max(match_index_[peer_id], new_match);
    next_index_[peer_id] = match_index_[peer_id] + 1;
    spdlog::debug("[Node {}] [term={}] [Leader] replicated to Node {}, matchIndex={}",
                  config_.node_id, current_term_, peer_id, match_index_[peer_id]);
    maybe_advance_commit_index();
  } else {
    // Fast backoff: use conflict_term / conflict_index to skip entire terms.
    if (reply.conflict_term == -1) {
      // Follower's log is too short — jump to its end.
      next_index_[peer_id] = reply.conflict_index;
    } else {
      // Search our log for the last entry with conflict_term.
      int found = -1;
      for (int i = last_log_index(); i > last_snapshot_index_; --i) {
        if (term_at(i) == reply.conflict_term) { found = i; break; }
      }
      next_index_[peer_id] = (found != -1) ? found + 1 : reply.conflict_index;
    }
    spdlog::debug("[Node {}] [term={}] [Leader] backoff for Node {}, nextIndex={}",
                  config_.node_id, current_term_, peer_id, next_index_[peer_id]);
  }
}

// Handle an incoming AppendEntries RPC.
// Performs term check, log consistency check, entry appending, and commitIndex update.
void Raft::handle_append_entries(const AppendEntriesArgs& args,
                                 AppendEntriesReply*      reply) {
  std::lock_guard<std::mutex> lock(mu_);
  reply->term = current_term_;
  reply->success = false;

  // ── Step 1: Term check ────────────────────────────────────────
  if (args.term < current_term_) {
    return;
  }
  if (args.term > current_term_) {
    become_follower(args.term);
  }
  if (role_ == Role::Candidate) {
    role_ = Role::Follower;
  }
  reset_election_timer();
  reply->term = current_term_;

  // ── Step 2: Log consistency check ─────────────────────────────
  if (args.prev_log_index > last_log_index()) {
    reply->conflict_index = last_log_index() + 1;
    reply->conflict_term  = -1;
    return;
  }
  int actual_term = term_at(args.prev_log_index);
  if (actual_term != args.prev_log_term) {
    reply->conflict_term = actual_term;
    // Walk backwards to find the first index with this conflicting term.
    int idx = args.prev_log_index;
    while (idx > last_snapshot_index_ && term_at(idx) == actual_term) { --idx; }
    reply->conflict_index = idx + 1;
    return;
  }

  // ── Step 3: Append / overwrite entries ────────────────────────
  // Only truncate on actual conflict (term mismatch at a given index).
  // Do NOT blindly truncate — an out-of-order RPC with fewer entries
  // must not erase entries already confirmed by a later RPC.
  {
    int truncate_from = -1;
    std::vector<LogEntry> new_entries;
    for (size_t i = 0; i < args.entries.size(); ++i) {
      int idx = args.entries[i].index;
      if (idx > last_log_index()) {
        new_entries.push_back(args.entries[i]);
        logs_.push_back(args.entries[i]);
      } else if (args.entries[i].term != term_at(idx)) {
        truncate_from = idx;
        logs_.erase(logs_.begin() + to_physical(idx), logs_.end());
        for (size_t j = i; j < args.entries.size(); ++j) {
          new_entries.push_back(args.entries[j]);
          logs_.push_back(args.entries[j]);
        }
        break;
      }
    }
    if (truncate_from > 0) {
      persister_->truncate_suffix(truncate_from);
    }
    if (!new_entries.empty()) {
      persister_->append_logs(new_entries);
      persist();
    }
  }

  // ── Step 4: Update commit_index_ ──────────────────────────────
  if (args.leader_commit > commit_index_) {
    commit_index_ = std::min(args.leader_commit, last_log_index());
    commit_cv_.notify_one();  // wake applier immediately
  }
  reply->success = true;
}

// ── Persistence ───────────────────────────────────────────────────

// Save only metadata (term, votedFor, snapshot info).  O(1).
// Must be called while holding mu_.
void Raft::persist() {
    persister_->save_meta(current_term_, voted_for_,
                          last_snapshot_index_, last_snapshot_term_);
}

// Load persisted state from disk. Called during initialization, before threads start.
void Raft::restore() {
    int term, voted_for, snap_idx, snap_term;
    if (!persister_->load_meta(&term, &voted_for, &snap_idx, &snap_term)) return;

    current_term_ = term;
    voted_for_ = voted_for;
    last_snapshot_index_ = snap_idx;
    last_snapshot_term_ = snap_term;

    // Rebuild log from WAL.  logs_[0] is the sentinel.
    std::vector<LogEntry> wal_entries = persister_->load_logs();
    logs_.clear();
    LogEntry sentinel;
    sentinel.index = last_snapshot_index_;
    sentinel.term  = last_snapshot_term_;
    logs_.push_back(sentinel);
    for (const LogEntry& e : wal_entries) {
      logs_.push_back(e);
    }

    // After restoring from a snapshot, entries before the snapshot point
    // are already applied.  Advance volatile state accordingly.
    if (last_snapshot_index_ > commit_index_) {
      commit_index_ = last_snapshot_index_;
    }
    if (last_snapshot_index_ > last_applied_) {
      last_applied_ = last_snapshot_index_;
    }
}

// ── Client API ────────────────────────────────────────────────────

// Submit a command to the Raft log. Only the Leader accepts commands.
// Returns immediately — does not wait for replication or commit.
StartResult Raft::start(const std::string& command) {
  std::lock_guard<std::mutex> lock(mu_);
  StartResult r;
  if (role_ != Role::Leader) {
    r.is_leader = false;
    return r;
  }

  LogEntry entry;
  entry.command = command;
  entry.term = current_term_;
  entry.index = last_log_index() + 1;
  logs_.push_back(entry);

  persister_->append_logs({entry});
  persist();

  replicate_cv_.notify_all();

  r.index = entry.index;
  r.term = entry.term;
  r.is_leader = true;
  return r;
}

// Decide whether to send AppendEntries or InstallSnapshot to a peer.
// If the peer is too far behind (next_index <= last_snapshot_index),
// we must send the full snapshot instead of log entries.
void Raft::replicate_to_peer(int peer_id) {
  bool need_snapshot = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) return;
    need_snapshot = (next_index_[peer_id] <= last_snapshot_index_);
  }
  if (need_snapshot) {
    send_install_snapshot(peer_id);
  } else {
    send_append_entries(peer_id);
  }
}

// Scan matchIndex to find the highest log index replicated to a majority.
// Advance commit_index_ if that index is from the current term (Figure 8 safety).
// Requires mu_ held.
void Raft::maybe_advance_commit_index() {
  for (int idx = last_log_index(); idx > commit_index_; --idx) {
    int count = 1;  // self
    for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
      if (i == config_.node_id) continue;
      count += (match_index_[i] >= idx);
    }
    if (count < 1 + static_cast<int>(peers_.size()) / 2) continue;
    // Figure 8: only commit current-term entries directly.
    if (term_at(idx) == current_term_) {
      int old_commit = commit_index_;
      commit_index_ = idx;
      if (commit_index_ > old_commit) {
        commit_cv_.notify_one();  // wake applier immediately
        spdlog::debug("[Node {}] [term={}] [Leader] commitIndex advanced to {}",
                      config_.node_id, current_term_, commit_index_);
      }
    }
    break;
  }
}

// Background thread: periodically pushes committed but not-yet-applied
// entries from the log to apply_channel_.
// IMPORTANT: Do NOT hold mu_ while pushing — KV layer may call snapshot() back.
void Raft::applier() {
  while (running_) {
    std::vector<ApplyMsg> msgs;
    {
      std::unique_lock<std::mutex> lock(mu_);
      // Wait until new entries are committed or shutdown.
      // Max wait = apply_interval_ms (same as old sleep_for behavior),
      // but wakes immediately when commit_cv_ is notified.
      commit_cv_.wait_for(lock,
          std::chrono::milliseconds(config_.raft.apply_interval_ms),
          [this] { return last_applied_ < commit_index_ || !running_; });
      while (last_applied_ < commit_index_) {
        last_applied_++;
        ApplyMsg msg;
        msg.command_valid = true;
        msg.command       = logs_[to_physical(last_applied_)].command;
        msg.command_index = last_applied_;
        msg.command_term  = logs_[to_physical(last_applied_)].term;
        msgs.push_back(msg);
      }
    }

    for (auto& msg : msgs) {
      apply_channel_->push(msg);
    }
  }
}

// ── Phase 5: Snapshot ─────────────────────────────────────────────

// Called by KvStore when it wants to compact the log.
// Truncates logs_ up to `index`, updates the sentinel, and persists
// both raft state and snapshot data atomically.
void Raft::snapshot(int index, const std::string& snapshot_data) {
  std::lock_guard<std::mutex> lock(mu_);

  if (index <= last_snapshot_index_ || index > commit_index_) return;

  last_snapshot_term_ = term_at(index);

  int phys = to_physical(index);
  logs_.erase(logs_.begin(), logs_.begin() + phys);
  logs_[0].command.clear();

  last_snapshot_index_ = index;

  // WAL: remove compacted entries, save meta + snapshot
  persister_->truncate_prefix(index);
  persist();
  persister_->save_snapshot(snapshot_data);
  spdlog::info("[Node {}] [term={}] [{}] log compacted up to index {}",
               config_.node_id, current_term_, to_string(role_), index);
}

// Leader sends the full snapshot to a peer that is too far behind.
// Same lock pattern as send_append_entries: build args under lock,
// release lock during RPC, re-acquire to process reply.
void Raft::send_install_snapshot(int peer_id) {
  InstallSnapshotArgs args;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) return;

    args.term = current_term_;
    args.leader_id = config_.node_id;
    args.last_included_index = last_snapshot_index_;
    args.last_included_term  = last_snapshot_term_;
    args.data = persister_->load_snapshot();
    spdlog::info("[Node {}] [term={}] [Leader] sending snapshot to Node {} (lastIndex={})",
                 config_.node_id, current_term_, peer_id, last_snapshot_index_);
  }

  InstallSnapshotReply reply;
  bool ok = peers_[peer_id]->install_snapshot(args, &reply,
                                               config_.raft.rpc_timeout_ms);
  if (!ok) return;

  std::lock_guard<std::mutex> lock(mu_);
  if (reply.term > current_term_) {
    become_follower(reply.term);
    return;
  }
  if (role_ != Role::Leader || current_term_ != args.term) return;

  match_index_[peer_id] = std::max(match_index_[peer_id], args.last_included_index);
  next_index_[peer_id]  = match_index_[peer_id] + 1;
}

// Follower receives a snapshot from the leader.
// Replaces (or trims) the local log, persists, and delivers the
// snapshot to KvStore via apply_channel_.
//
// IMPORTANT: must NOT hold mu_ while pushing to apply_channel_
// (same deadlock-avoidance rule as applier()).
void Raft::handle_install_snapshot(const InstallSnapshotArgs& args,
                                   InstallSnapshotReply*      reply) {
  ApplyMsg snap_msg;
  bool should_apply = false;

  {
    std::lock_guard<std::mutex> lock(mu_);
    reply->term = current_term_;

    // ── Step 1: Term check (same as handle_append_entries) ────────
    if (args.term < current_term_) return;
    if (args.term > current_term_) become_follower(args.term);
    if (role_ == Role::Candidate) role_ = Role::Follower;
    reset_election_timer();
    reply->term = current_term_;
    // Step 2: Reject stale snapshot
    if (args.last_included_index <= last_snapshot_index_) return;

    // Step 3: Truncate / replace log
    bool had_entries_beyond = last_log_index() > args.last_included_index;
    if (had_entries_beyond) {
      int phys = to_physical(args.last_included_index);
      logs_.erase(logs_.begin(), logs_.begin() + phys);
      logs_[0].command.clear();
    } else {
      logs_.clear();
      LogEntry sentinel;
      sentinel.index   = args.last_included_index;
      sentinel.term    = args.last_included_term;
      logs_.push_back(sentinel);
    }

    // Step 4: Update snapshot metadata
    last_snapshot_index_ = args.last_included_index;
    last_snapshot_term_  = args.last_included_term;

    // Step 5: Advance volatile state
    commit_index_ = std::max(commit_index_, args.last_included_index);
    last_applied_  = std::max(last_applied_,  args.last_included_index);

    // Step 6: Persist via WAL — truncate log, save meta + snapshot
    persister_->truncate_prefix(args.last_included_index);
    if (had_entries_beyond) {
      // WAL still has entries beyond the snapshot; keep them.
      // truncate_prefix already removed entries < index.
    }
    persist();
    persister_->save_snapshot(args.data);

    // Step 7: Prepare ApplyMsg to deliver snapshot to KvStore
    snap_msg.snapshot_valid = true;
    snap_msg.snapshot       = args.data;
    snap_msg.snapshot_index = args.last_included_index;
    snap_msg.snapshot_term  = args.last_included_term;
    should_apply = true;
    spdlog::info("[Node {}] [term={}] [Follower] installed snapshot (index={}, term={})",
                 config_.node_id, current_term_, args.last_included_index, args.last_included_term);
  }
  // Lock released — safe to push to channel without deadlock risk.
  if (should_apply) {
    apply_channel_->push(snap_msg);
  }
}

}  // namespace raftkv
