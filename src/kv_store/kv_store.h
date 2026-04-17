#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/config.h"
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
// Client-facing RPCs (Get, PutAppend) go through:
//   1. KvStore submits command to Raft via raft_->start()
//   2. Waits for the command to appear on the apply channel
//   3. Returns result to client
//
// Features:
//   - Request deduplication via client_id + request_id
//   - Snapshot serialization for Raft log compaction
//   - Thread-safe access to KV data
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
  // ── Apply Loop ───────────────────────────────────────────────
  void apply_loop();           // Main loop: reads from apply_channel
  void apply_command(const ApplyMsg& msg);
  void apply_snapshot(const ApplyMsg& msg);

  // ── Snapshot ─────────────────────────────────────────────────
  void maybe_take_snapshot(int applied_index);
  std::string serialize_snapshot();
  void        restore_snapshot(const std::string& data);

  // ── Deduplication ────────────────────────────────────────────
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

  // The actual KV data store
  std::unordered_map<std::string, std::string> data_;

  // Deduplication: client_id → last applied request_id
  std::unordered_map<std::string, int64_t> last_request_id_;

  // Wait channels: log_index → channel
  // Lifetime: created by the RPC handler (while holding mu_),
  // notified by apply_loop() (while holding mu_), then erased.
  std::unordered_map<int, std::shared_ptr<WaitChannel>> wait_channels_;

  int last_snapshot_index_ = 0;

  // Background thread
  std::atomic<bool> running_{false};
  std::thread apply_thread_;
};

}  // namespace raftkv
