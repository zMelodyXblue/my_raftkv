#include "kv_store/kv_store.h"

#include <spdlog/spdlog.h>

#include "raft/raft.h"

// KvOp serialization uses protobuf.
// KvOpData is defined in kv.pb.h alongside the RPC messages.
#include "proto/kv.pb.h"

namespace raftkv {

// ── Constructor ──────────────────────────────────────────────────

KvStore::KvStore(const ServerConfig& config,
                 std::shared_ptr<Raft> raft,
                 std::shared_ptr<ThreadSafeQueue<ApplyMsg>> apply_channel)
    : config_(config),
      raft_(std::move(raft)),
      apply_channel_(std::move(apply_channel)) {
  running_ = true;
  apply_thread_ = std::thread(&KvStore::apply_loop, this);
}

// ── Destructor ───────────────────────────────────────────────────

KvStore::~KvStore() {
  running_ = false;
  apply_channel_->close();
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& pair : wait_channels_) {
      pair.second->close();
    }
  }
  if (apply_thread_.joinable()) {
    apply_thread_.join();
  }
}

// ── Client API ───────────────────────────────────────────────────

// Called by KvServiceImpl::Get().
// Submits a Get operation through the Raft log to guarantee
// linearizability, then waits for the result.
KvStore::GetResult KvStore::get(const std::string& key,
                                const std::string& client_id,
                                int64_t request_id) {
  // ── Step 1: Serialize KvOp into a string ──────────────────────
  // TODO: Construct a kv::KvOpData protobuf message with:
  //   op = "Get", key, client_id, request_id
  // Serialize it to a std::string (SerializeToString).
  std::string command;  // TODO: replace with serialized KvOpData

  // ── Step 2: Submit to Raft and create wait channel ────────────
  // CRITICAL: Both start() and wait channel creation MUST be in
  // the same critical section.  See kv_store.h for the race-free
  // creation rule explanation.
  std::shared_ptr<WaitChannel> ch;
  {
    std::lock_guard<std::mutex> lk(mu_);
    StartResult sr = raft_->start(command);
    if (!sr.is_leader) {
      return {/*value=*/"", /*error=*/ErrCode::ErrWrongLeader};
    }
    ch = get_or_create_wait_channel(sr.index);
  }
  // Lock released — now safe to block.

  // ── Step 3: Wait for apply ────────────────────────────────────
  // TODO: Use ch->try_pop() with a reasonable timeout (e.g., 5000ms).
  // If timed out, clean up the wait channel and return ErrTimeout.
  // If successful, return the ApplyResult's value and error.
  ApplyResult result;
  // TODO: try_pop with timeout, handle timeout case

  close_and_remove_wait_channel(0 /* TODO: use the correct index */);
  return {result.value, result.error};
}

// Called by KvServiceImpl::PutAppend().
// Same flow as get() but returns only an error string.
std::string KvStore::put_append(const std::string& key,
                                const std::string& value,
                                const std::string& op,
                                const std::string& client_id,
                                int64_t request_id) {
  // ── Step 1: Serialize KvOp ────────────────────────────────────
  // TODO: Construct a kv::KvOpData protobuf message with:
  //   op, key, value, client_id, request_id
  // Serialize it to a std::string.
  std::string command;  // TODO: replace with serialized KvOpData

  // ── Step 2: Submit to Raft and create wait channel ────────────
  std::shared_ptr<WaitChannel> ch;
  {
    std::lock_guard<std::mutex> lk(mu_);
    StartResult sr = raft_->start(command);
    if (!sr.is_leader) {
      return ErrCode::ErrWrongLeader;
    }
    ch = get_or_create_wait_channel(sr.index);
  }

  // ── Step 3: Wait for apply ────────────────────────────────────
  // TODO: Use ch->try_pop() with a reasonable timeout.
  // If timed out, clean up and return ErrTimeout.
  // If successful, return the ApplyResult's error field.
  ApplyResult result;
  // TODO: try_pop with timeout, handle timeout case

  close_and_remove_wait_channel(0 /* TODO: use the correct index */);
  return result.error;
}

// ── Apply Loop ───────────────────────────────────────────────────

// Background thread: reads committed entries from apply_channel and
// dispatches them to apply_command() or apply_snapshot().
void KvStore::apply_loop() {
  while (running_) {
    ApplyMsg msg;
    if (!apply_channel_->pop(msg)) {
      break;  // Channel closed — shutting down
    }

    if (msg.command_valid) {
      apply_command(msg);
    } else if (msg.snapshot_valid) {
      apply_snapshot(msg);
    }
  }
}

// Process a single committed command.
// Deserialize the KvOp, check for duplicates, execute, notify waiter.
void KvStore::apply_command(const ApplyMsg& msg) {
  // ── Step 1: Deserialize KvOp ──────────────────────────────────
  // TODO: Parse msg.command as a kv::KvOpData protobuf.
  // Extract op, key, value, client_id, request_id into local variables.
  KvOp op;  // TODO: populate from deserialized KvOpData

  // ── Step 2: Execute under lock ────────────────────────────────
  ApplyResult result;
  {
    std::lock_guard<std::mutex> lk(mu_);

    // TODO: Check is_duplicate(client_id, request_id).
    //   - If duplicate: skip execution.
    //     For Get, still read data_[key] so the result is correct.
    //     For Put/Append, do nothing (idempotent).
    //   - If not duplicate: execute the operation:
    //     - "Get":    result.value = data_[key] (or ErrNoKey if absent)
    //     - "Put":    data_[key] = value
    //     - "Append": data_[key] += value (create if absent)
    //     Then update last_request_id_[client_id] = request_id.

    // TODO: Fill result.value (for Get) and result.error.

    // Notify the RPC handler waiting on this log index.
    notify_wait_channel(msg.command_index, result);
  }

  // Phase 5: check if snapshot is needed.
  maybe_take_snapshot(msg.command_index);
}

// Phase 5: apply a snapshot received from Raft.
void KvStore::apply_snapshot(const ApplyMsg& /*msg*/) {
  // Phase 5: deserialize snapshot, replace data_ and last_request_id_.
}

// ── Snapshot (Phase 5 stubs) ─────────────────────────────────────

void KvStore::maybe_take_snapshot(int /*applied_index*/) {
  // Phase 5: if raft_->raft_state_size() > config_.raft.max_raft_state_bytes,
  //          call raft_->snapshot(applied_index, serialize_snapshot()).
}

std::string KvStore::serialize_snapshot() {
  // Phase 5: serialize data_ and last_request_id_ into a kv::KvSnapshot
  // protobuf string.
  return "";
}

void KvStore::restore_snapshot(const std::string& /*data*/) {
  // Phase 5: deserialize kv::KvSnapshot, replace data_ and last_request_id_.
}

// ── Deduplication ────────────────────────────────────────────────

// Returns true if the request has already been applied.
// Requires mu_ held.
bool KvStore::is_duplicate(const std::string& client_id, int64_t request_id) {
  // TODO: Look up client_id in last_request_id_.
  //   - If found and last_request_id >= request_id → duplicate (return true).
  //   - Otherwise → not duplicate (return false).
  return false;
}

// ── Wait Channel Helpers ─────────────────────────────────────────
// All require mu_ held.

std::shared_ptr<KvStore::WaitChannel>
KvStore::get_or_create_wait_channel(int index) {
  // TODO: If wait_channels_[index] exists, return it.
  //       Otherwise, create a new WaitChannel, insert it, and return it.
  auto it = wait_channels_.find(index);
  if (it != wait_channels_.end()) {
    return it->second;
  }
  auto ch = std::make_shared<WaitChannel>();
  wait_channels_[index] = ch;
  return ch;
}

void KvStore::notify_wait_channel(int index, ApplyResult result) {
  // TODO: Look up wait_channels_[index].
  //       If found, push the result into it.
  auto it = wait_channels_.find(index);
  if (it != wait_channels_.end()) {
    it->second->push(std::move(result));
  }
}

void KvStore::close_and_remove_wait_channel(int index) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = wait_channels_.find(index);
  if (it != wait_channels_.end()) {
    it->second->close();
    wait_channels_.erase(it);
  }
}

}  // namespace raftkv
