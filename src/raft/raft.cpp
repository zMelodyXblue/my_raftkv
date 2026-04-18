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
  // Single-node cluster: no RPCs needed, become leader immediately.
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
      // Immediate heartbeat to suppress other elections.
      std::shared_ptr<Raft> self2;
      try { self2 = shared_from_this(); } catch (const std::bad_weak_ptr&) { return; }
      for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
        if (i == config_.node_id) continue;
        std::thread([self2, i]() { self2->send_append_entries(i); }).detach();
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
    return ;
  }
  reply->term = current_term_;
  if ((voted_for_ != -1 && voted_for_ != args.candidate_id) ||
      (args.last_log_term < last_log_term()) ||
      (args.last_log_term == last_log_term() && args.last_log_index < last_log_index())) {
    reply->vote_granted = false;
    return ;
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

// Background thread: Leader sends a round of heartbeats every heartbeat_interval_ms.
void Raft::heartbeat_ticker() {
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.raft.heartbeat_interval_ms));
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (role_ != Role::Leader) continue;
    }

    // During shutdown the weak_ptr may already be expired (refcount == 0
    // while ~Raft is blocked on join); in that case skip the heartbeat round.
    std::shared_ptr<Raft> self;
    try { self = shared_from_this(); } catch (const std::bad_weak_ptr&) { continue; }
    for (int i = 0; i < static_cast<int>(peers_.size()); ++i) {
      if (i == config_.node_id) continue;
      std::thread([self, i]() { self->send_append_entries(i); }).detach();
    }
  }
}

// Send a heartbeat (empty AppendEntries) to one peer.
// Phase 3 will extend this to include log entries.
void Raft::send_append_entries(int peer_id) {
  AppendEntriesArgs args;
  args.leader_id = config_.node_id;
  {
    std::lock_guard<std::mutex> lock(mu_);
    args.term = current_term_;
    args.prev_log_index = next_index_[peer_id] - 1;
    args.prev_log_term = term_at(args.prev_log_index);
    args.leader_commit = commit_index_;
  }
  AppendEntriesReply reply;
  bool ok = peers_[peer_id]->append_entries(args, &reply, config_.raft.rpc_timeout_ms);
  if (!ok) return ;
  std::lock_guard<std::mutex> lock(mu_);
  if (reply.term > current_term_) {
    become_follower(reply.term);
  }
  if (role_ != Role::Leader || reply.term != current_term_) return;
  // Phase 3: update next_index_ / match_index_ on success/failure
}

// Handle an incoming AppendEntries RPC (heartbeat in Phase 2).
// Rejects if sender's term is stale; otherwise resets election timer.
void Raft::handle_append_entries(const AppendEntriesArgs& args,
                                 AppendEntriesReply*      reply) {
  std::lock_guard<std::mutex> lock(mu_);
  if (args.term < current_term_) {
    reply->term = current_term_;
    reply->success = false;
    return ;
  } else if (args.term > current_term_) {
    become_follower(args.term);
  }
  reset_election_timer();

  // Phase 3: log consistency check

  reply->term = current_term_;
  reply->success = true;
}

// ── Persistence ───────────────────────────────────────────────────

// Serialize current_term_, voted_for_, logs_ to protobuf and save to disk.
// Must be called while holding mu_.
void Raft::persist() {
    raft::RaftPersistState state;
    state.set_current_term(current_term_);
    state.set_voted_for(voted_for_);
    for (const LogEntry& e : logs_) {
      raft::LogEntry* pe = state.add_logs();
      pe->set_command(e.command);
      pe->set_term(e.term);
      pe->set_index(e.index);
    }
    std::string bytes;
    state.SerializeToString(&bytes);
    persister_->save_raft_state(bytes);
}

// Load persisted state from disk. Called during initialization, before threads start.
void Raft::restore() {
    std::string bytes = persister_->load_raft_state();
    if (bytes.empty()) return;
    raft::RaftPersistState state;
    if (!state.ParseFromString(bytes)) return;

    current_term_ = state.current_term();
    voted_for_ = state.voted_for();
    logs_.clear();
    for (int i = 0; i < state.logs_size(); ++i) {
      LogEntry e;
      e.command = state.logs(i).command();
      e.term = state.logs(i).term();
      e.index = state.logs(i).index();
      logs_.push_back(e);
    }
}

// ── Phase 3 stubs ─────────────────────────────────────────────────

StartResult Raft::start(const std::string& /*command*/) {
  // Phase 3: append command to log, return (index, term, is_leader).
  StartResult r;
  return r;
}

void Raft::replicate_to_peer(int /*peer_id*/) {
  // Phase 3: send AppendEntries or InstallSnapshot depending on nextIndex.
}

void Raft::maybe_advance_commit_index() {
  // Phase 3: find the highest index replicated to a majority and
  // advance commit_index_ if it's in the current term.
}

void Raft::applier() {
  // Phase 3: loop, push committed entries to apply_channel_.
}

// ── Phase 5 stubs ─────────────────────────────────────────────────

void Raft::snapshot(int /*index*/, const std::string& /*snapshot_data*/) {
  // Phase 5: truncate logs_ up to index, update sentinel, persist.
}

void Raft::send_install_snapshot(int /*peer_id*/) {
  // Phase 5: send InstallSnapshot RPC when peer is too far behind.
}

void Raft::handle_install_snapshot(const InstallSnapshotArgs& /*args*/,
                                   InstallSnapshotReply*      reply) {
  // Phase 5: receive snapshot from leader, deliver via apply_channel_.
  std::lock_guard<std::mutex> lock(mu_);
  reply->term = current_term_;
}

}  // namespace raftkv
