#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/config.h"
#include "common/kv_engine.h"
#include "common/thread_safe_queue.h"
#include "common/types.h"

namespace raftkv {

class Raft;  // Forward declaration

// ═════════════════════════════════════════════════════════════════
// KvStore — Raft-backed KV state machine
// ═════════════════════════════════════════════════════════════════
// Receives committed commands from Raft via apply_channel and
// applies them to an in-memory key-value store.
//
// Two client-facing paths:
//
//   Writes (Put, Append):
//     1. Serialize command, submit to Raft via raft_->start()
//     2. Wait for commit + apply via wait_channel
//     3. Return result
//
//   Reads (Get) — ReadIndex optimization:
//     1. Confirm leadership via raft_->read_index()
//     2. Wait for state machine to catch up to commit_index
//     3. Read directly from data_ (no log entry written)
//
// Features:
//   - ReadIndex for linearizable reads without log writes
//   - Request deduplication for writes via client_id + request_id
//   - Snapshot serialization for Raft log compaction
//
class KvStore {
 public:
  KvStore(const ServerConfig& config,
          std::shared_ptr<Raft> raft,
          std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel);
  ~KvStore();

  KvStore(const KvStore&) = delete;
  KvStore& operator=(const KvStore&) = delete;

  // ── Client API ───────────────────────────────────────────────
  // Called by KvServiceImpl (gRPC handler).
  struct GetResult {
    std::string value;
    std::string error;
  };

  GetResult get(const std::string& key,
                const std::string& client_id,
                int64_t request_id);

  std::string put_append(const std::string& key,
                         const std::string& value,
                         const std::string& op,  // "Put" or "Append"
                         const std::string& client_id,
                         int64_t request_id);

 private:
  // ── ReadIndex (read optimization) ─────────────────────────────
  GetResult read_index_get(const std::string& key);
  void wait_until_applied(int index);

  // ── Apply Loop ───────────────────────────────────────────────
  void apply_loop();           // Main loop: reads from apply_channel
  void apply_command(const ApplyMsg& msg);
  void apply_snapshot(const ApplyMsg& msg);

  // ── Snapshot ─────────────────────────────────────────────────
  void maybe_take_snapshot(int applied_index);
  std::string serialize_snapshot();
  void        restore_snapshot(const std::string& data);

  // ── Deduplication ────────────────────────────────────────────
  struct DedupEntry {
    int64_t request_id;
    std::string value;  // cached return value (Get result; empty for Put/Append)
  };

  bool is_duplicate(const std::string& client_id, int64_t request_id);

  // ── Waiting for apply ────────────────────────────────────────
  // Per-index channel: the RPC handler blocks here after submitting a
  // command to Raft, and is unblocked by apply_loop() when that log
  // index is committed and applied.
  //
  // Race-free creation rule (see also tutorial/01_architecture.md):
  //   The channel MUST be inserted into wait_channels_ while holding
  //   mu_ in the same critical section as the Raft::start() call.
  //   apply_loop() also holds mu_ before notifying, so either:
  //     (a) the handler creates the channel first → apply_loop finds
  //         it and delivers the result, or
  //     (b) apply_loop runs first → it finds no channel and skips
  //         notification → handler's try_pop() times out and retries.
  //   Case (b) is harmless: the command was committed so the value is
  //   already in data_; the next retry on the client side will succeed.
  //
  // Shutdown: close() is called on every live channel in ~KvStore()
  // before joining apply_thread_, ensuring no handler blocks forever.
  using WaitChannel = ThreadSafeQueue<ApplyResult>;
  std::shared_ptr<WaitChannel> get_or_create_wait_channel(int index);
  void notify_wait_channel(int index, ApplyResult result);
  void close_and_remove_wait_channel(int index);

  // ── State ────────────────────────────────────────────────────
  ServerConfig config_;
  std::shared_ptr<Raft> raft_;
  std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel_;

  mutable std::mutex mu_;

  // The actual KV data store (pluggable via KvEngine interface)
  std::unique_ptr<KvEngine> data_;

  // Deduplication: client_id → last applied entry (request_id + cached result)
  std::unordered_map<std::string, DedupEntry> last_applied_;

  // Wait channels: log_index → channel
  // Lifetime: created by the RPC handler (while holding mu_),
  // notified by apply_loop() (while holding mu_), then erased.
  std::unordered_map<int, std::shared_ptr<WaitChannel>> wait_channels_;

  int last_snapshot_index_ = 0;

  // ReadIndex: tracks the highest applied command index for read optimization.
  int applied_index_ = 0;
  std::condition_variable applied_cv_;

  // Background thread
  std::atomic<bool> running_{false};
  std::thread apply_thread_;
};

}  // namespace raftkv
