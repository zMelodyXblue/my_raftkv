#include "raft/raft.h"

#include <algorithm>
#include <random>
#include <spdlog/spdlog.h>

#include "raft/persister.h"
#include "rpc/raft_service.h"

// persist() / restore() are the only places that touch protobuf.
// They are explicit serialization boundaries: everything else in this
// file uses internal types only.
#include "proto/raft.pb.h"

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
  election_thread_  = std::thread(&Raft::election_ticker,  this);
  heartbeat_thread_ = std::thread(&Raft::heartbeat_ticker, this);
  applier_thread_   = std::thread(&Raft::applier,          this);
}

// ── Destructor ────────────────────────────────────────────────────

Raft::~Raft() {
  running_ = false;
  election_thread_.join();
  heartbeat_thread_.join();
  applier_thread_.join();
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
  return persister_->raft_state_size();
}

int Raft::snapshot_index() const {
  std::lock_guard<std::mutex> lock(mu_);
  return last_snapshot_index_;
}

std::string Raft::read_persisted_snapshot() const {
  return persister_->load_snapshot();
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
}

// Called when this node wins an election.
// Initializes leader-only volatile state (nextIndex, matchIndex).
// Does NOT persist — role is volatile state.
void Raft::become_leader() {
  role_ = Role::Leader;
  int n = static_cast<int>(peers_.size());
  next_index_.assign(n, last_log_index() + 1);
  match_index_.assign(n, 0);
  spdlog::info("[{}] became Leader, term={}", config_.node_id, current_term_);
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
      std::shared_ptr<Raft> self2;
      try { self2 = shared_from_this(); } catch (const std::bad_weak_ptr&) { return; }
      for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
        if (i == config_.node_id) continue;
        std::thread([self2, i]() { self2->replicate_to_peer(i); }).detach();
      }
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
    return;
  }

  voted_for_ = args.candidate_id;
  persist();
  reset_election_timer();
  reply->vote_granted = true;
}

// Background thread: polls every 10ms, starts an election if the
// election timeout has elapsed and this node is not the leader.
void Raft::election_ticker() {
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    bool timed_out = false;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (role_ == Role::Leader) continue;
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - last_heartbeat_recv_).count();
      timed_out = (elapsed >= election_timeout_ms_);
    }
    if (timed_out) start_election();
  }
}

// ── Replication ───────────────────────────────────────────────────

// Background thread: Leader sends a round of AppendEntries every heartbeat_interval_ms.
void Raft::heartbeat_ticker() {
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.raft.heartbeat_interval_ms));
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (role_ != Role::Leader) continue;
    }

    std::shared_ptr<Raft> self;
    try { self = shared_from_this(); } catch (const std::bad_weak_ptr&) { continue; }
    for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
      if (i == config_.node_id) continue;
      std::thread([self, i]() { self->replicate_to_peer(i); }).detach();
    }
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
  for (size_t i = 0; i < args.entries.size(); ++i) {
    int new_log_idx = args.entries[i].index;
    if (new_log_idx > last_log_index()) {
      logs_.push_back(args.entries[i]);
    } else if (args.entries[i].term != term_at(new_log_idx)) {
      logs_.erase(logs_.begin() + to_physical(new_log_idx), logs_.end());
      logs_.push_back(args.entries[i]);
    }
  }
  if (!args.entries.empty()) {
    persist();
  }

  // ── Step 4: Update commit_index_ ──────────────────────────────
  if (args.leader_commit > commit_index_) {
    commit_index_ = std::min(args.leader_commit, last_log_index());
  }
  reply->success = true;
}

// ── Persistence ───────────────────────────────────────────────────

// Serialize all persistent Raft state (including snapshot metadata) to protobuf bytes.
// Requires mu_ held.  Used by persist() and snapshot().
std::string Raft::encode_raft_state() const {
    raft::RaftPersistState state;
    state.set_current_term(current_term_);
    state.set_voted_for(voted_for_);
    state.set_last_snapshot_index(last_snapshot_index_);
    state.set_last_snapshot_term(last_snapshot_term_);
    for (const LogEntry& e : logs_) {
      raft::LogEntry* pe = state.add_logs();
      pe->set_command(e.command);
      pe->set_term(e.term);
      pe->set_index(e.index);
    }
    std::string bytes;
    state.SerializeToString(&bytes);
    return bytes;
}

// Save raft state only (no snapshot data).  Must be called while holding mu_.
void Raft::persist() {
    persister_->save_raft_state(encode_raft_state());
}

// Load persisted state from disk. Called during initialization, before threads start.
void Raft::restore() {
    std::string bytes = persister_->load_raft_state();
    if (bytes.empty()) return;
    raft::RaftPersistState state;
    if (!state.ParseFromString(bytes)) return;

    current_term_ = state.current_term();
    voted_for_ = state.voted_for();
    last_snapshot_index_ = state.last_snapshot_index();
    last_snapshot_term_ = state.last_snapshot_term();
    logs_.clear();
    for (int i = 0; i < state.logs_size(); ++i) {
      LogEntry e;
      e.command = state.logs(i).command();
      e.term = state.logs(i).term();
      e.index = state.logs(i).index();
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

  persist();
  r.index = entry.index;
  r.term = entry.term;
  r.is_leader = true;
  return r;
}

// Decide whether to send AppendEntries or InstallSnapshot to a peer.
// If the peer is too far behind (next_index <= last_snapshot_index),
// we must send the full snapshot instead of log entries.
void Raft::replicate_to_peer(int peer_id) {
  // ── YOUR CODE HERE ──────────────────────────────────────────────
  // Read next_index_[peer_id] and last_snapshot_index_ under mu_.
  // Branch:
  //   next_index <= last_snapshot_index  → send_install_snapshot(peer_id)
  //   otherwise                          → send_append_entries(peer_id)
  // ────────────────────────────────────────────────────────────────

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
      commit_index_ = idx;
    }
    break;
  }
}

// Background thread: periodically pushes committed but not-yet-applied
// entries from the log to apply_channel_.
// IMPORTANT: Do NOT hold mu_ while pushing — KV layer may call snapshot() back.
void Raft::applier() {
  while (running_) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(config_.raft.apply_interval_ms));

    std::vector<ApplyMsg> msgs;
    {
      std::lock_guard<std::mutex> lock(mu_);
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

  // ── YOUR CODE HERE ──────────────────────────────────────────────
  // 1. Validate: reject if index <= last_snapshot_index_ or index > commit_index_
  //
  // 2. Record last_snapshot_term_ = term_at(index)
  //
  // 3. Truncate logs_:
  //    - Erase entries from logs_.begin() up to (but not including) the
  //      entry at `index` in physical coordinates.
  //    - After erasure, logs_[0] IS the entry at `index` — clear its
  //      command to turn it into the new sentinel.
  //
  // 4. Update last_snapshot_index_ = index
  //
  // 5. Persist BOTH raft state and snapshot atomically:
  //      persister_->save(encode_raft_state(), snapshot_data);
  // ────────────────────────────────────────────────────────────────
}

// Leader sends the full snapshot to a peer that is too far behind.
// Same lock pattern as send_append_entries: build args under lock,
// release lock during RPC, re-acquire to process reply.
void Raft::send_install_snapshot(int peer_id) {
  InstallSnapshotArgs args;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) return;

    // ── YOUR CODE HERE ────────────────────────────────────────────
    // Fill args:
    //   args.term                = current_term_
    //   args.leader_id           = config_.node_id
    //   args.last_included_index = last_snapshot_index_
    //   args.last_included_term  = last_snapshot_term_
    //   args.data                = persister_->load_snapshot()
    // ──────────────────────────────────────────────────────────────
    args.term = current_term_;
    args.leader_id = config_.node_id;
    args.last_included_index = last_snapshot_index_;
    args.last_included_term  = last_snapshot_term_;
    args.data = persister_->load_snapshot();
  }

  InstallSnapshotReply reply;
  bool ok = peers_[peer_id]->install_snapshot(args, &reply,
                                               config_.raft.rpc_timeout_ms);
  if (!ok) return;

  std::lock_guard<std::mutex> lock(mu_);

  // ── YOUR CODE HERE ──────────────────────────────────────────────
  // 1. If reply.term > current_term_: become_follower(reply.term), return.
  //
  // 2. Staleness check: if role != Leader or term changed, return.
  //
  // 3. Update peer state:
  //      match_index_[peer_id] = max(match_index_[peer_id], args.last_included_index)
  //      next_index_[peer_id]  = match_index_[peer_id] + 1
  // ────────────────────────────────────────────────────────────────
  if (reply.term > current_term_) {
    become_follower(reply.term);
    return ;
  }
  if (role_ != Role::Leader || current_term_ != args.term) {
    return ;
  }
  for (size_t i = 0; i < peers_.size(); ++i) {
    match_index_[i] = std::max(match_index_[i], args.last_included_index);
    next_index_[i] = match_index_[i] + 1;
  }
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

    // ── YOUR CODE HERE ────────────────────────────────────────────
    // Step 2: Reject stale snapshot
    //   if args.last_included_index <= last_snapshot_index_, return.
    //
    if (args.last_included_index <= last_snapshot_index_) return ;

    // Step 3: Truncate / replace log
    //   Case A: our log extends beyond the snapshot point
    //     → erase entries up to the snapshot point, keep the rest
    //     → logs_[0] becomes the new sentinel (clear its command)
    //   Case B: snapshot covers our entire log
    //     → clear logs_, push a fresh sentinel entry
    //        (index = args.last_included_index, term = args.last_included_term)
    //
    

    // Step 4: Update snapshot metadata
    //   last_snapshot_index_ = args.last_included_index
    //   last_snapshot_term_  = args.last_included_term
    //
    // Step 5: Advance volatile state
    //   commit_index_ = max(commit_index_, args.last_included_index)
    //   last_applied_  = max(last_applied_,  args.last_included_index)
    //
    // Step 6: Persist both raft state and snapshot atomically
    //   persister_->save(encode_raft_state(), args.data);
    //
    // Step 7: Prepare ApplyMsg to deliver snapshot to KvStore
    //   snap_msg.snapshot_valid = true
    //   snap_msg.snapshot       = args.data
    //   snap_msg.snapshot_index = args.last_included_index
    //   snap_msg.snapshot_term  = args.last_included_term
    //   should_apply = true
    // ──────────────────────────────────────────────────────────────
  }
  // Lock released — safe to push to channel without deadlock risk.
  if (should_apply) {
    apply_channel_->push(snap_msg);
  }
}

}  // namespace raftkv
