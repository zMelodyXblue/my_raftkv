#pragma once

#include <string>
#include <vector>

namespace raftkv {

// ── Raft Node Role ──────────────────────────────────────────────
enum class Role {
  Follower,
  Candidate,
  Leader,
};

inline const char* to_string(Role r) {
  switch (r) {
    case Role::Follower:  return "Follower";
    case Role::Candidate: return "Candidate";
    case Role::Leader:    return "Leader";
  }
  return "Unknown";
}

// ── Log Entry ───────────────────────────────────────────────────
// In-memory representation of a Raft log entry.
// Serialization (to protobuf / disk) only happens at two boundaries:
//   - persist(): encoding for the Persister
//   - send_append_entries(): encoding for the wire
// Inside Raft, we always work with this struct directly so that
// last_log_term(), log_term_at(), etc. never need to deserialize.
struct LogEntry {
  std::string command;  // Serialized KvOp (payload, opaque to Raft)
  int         term  = 0;
  int         index = 0;
};

// ── KV Operation ────────────────────────────────────────────────
// The command payload stored inside LogEntry::command.
//
// Design decision — does Get go through the Raft log?
//
//   YES (chosen for my_raftkv): both reads and writes are submitted
//   via Raft::start() and wait for the entry to be committed before
//   returning. This guarantees linearizability with zero extra
//   complexity: the leader cannot serve stale reads because it will
//   only reply after a majority has confirmed the entry.
//
//   The alternative (leader lease / ReadIndex) avoids the log-write
//   overhead for reads but requires additional protocol machinery.
//   We defer that optimisation to a future phase.
//
// Consequence: op field values are exactly "Get", "Put", "Append".
// The KvStore state machine executes Get against its local map and
// returns the value through the same wait-channel mechanism used for
// writes. This also means Get is deduplicated the same way.
struct KvOp {
  std::string op;          // "Get" | "Put" | "Append"
  std::string key;
  std::string value;       // Unused for Get
  std::string client_id;
  int64_t     request_id = 0;
};

// ── Apply Message ───────────────────────────────────────────────
// Produced by Raft::applier() and consumed by KvStore::apply_loop().
// Exactly one of command_valid / snapshot_valid is true per message.
struct ApplyMsg {
  bool        command_valid = false;
  std::string command;       // Serialized KvOp
  int         command_index = 0;
  int         command_term  = 0;

  bool        snapshot_valid = false;
  std::string snapshot;
  int         snapshot_index = 0;
  int         snapshot_term  = 0;
};

// ── Start Result ────────────────────────────────────────────────
// Returned by Raft::start() to the KV layer.
struct StartResult {
  int  index     = -1;
  int  term      = -1;
  bool is_leader = false;
};

// ── Apply Result ─────────────────────────────────────────────────
// Sent through the per-index wait channel from KvStore::apply_loop()
// back to the waiting RPC handler (KvStore::get / put_append).
struct ApplyResult {
  std::string value;   // Non-empty only for Get
  std::string error;   // ErrCode constant (empty = OK)
};

// ── Error Codes ─────────────────────────────────────────────────
namespace ErrCode {
  constexpr const char* OK             = "";
  constexpr const char* ErrNoKey       = "ErrNoKey";
  constexpr const char* ErrWrongLeader = "ErrWrongLeader";
  constexpr const char* ErrTimeout     = "ErrTimeout";
}

// ── Internal Raft RPC DTOs ───────────────────────────────────────
// These are the types used by Raft's public handler API.
// The gRPC adapter layer (RaftServiceImpl / RaftPeerClient) is
// responsible for translating between these and the protobuf types.
// Keeping them separate means the Raft core has zero gRPC dependency
// and can be unit-tested without any network infrastructure.

struct AppendEntriesArgs {
  int                   term            = 0;
  int                   leader_id       = 0;
  int                   prev_log_index  = 0;
  int                   prev_log_term   = 0;
  std::vector<LogEntry> entries;
  int                   leader_commit   = 0;
};

struct AppendEntriesReply {
  int  term            = 0;
  bool success         = false;
  // Fast log-backup optimisation fields (see Raft extended paper §5.3):
  // On conflict, follower reports the conflicting term and the first
  // index for that term so the leader can skip a whole term at once.
  int  conflict_index  = 0;   // First index with conflicting term (or next index)
  int  conflict_term   = -1;  // -1 means "no entry at prevLogIndex"
};

struct RequestVoteArgs {
  int term            = 0;
  int candidate_id    = 0;
  int last_log_index  = 0;
  int last_log_term   = 0;
};

struct RequestVoteReply {
  int  term         = 0;
  bool vote_granted = false;
};

struct InstallSnapshotArgs {
  int         term                 = 0;
  int         leader_id            = 0;
  int         last_included_index  = 0;
  int         last_included_term   = 0;
  std::string data;
};

struct InstallSnapshotReply {
  int term = 0;
};

}  // namespace raftkv
