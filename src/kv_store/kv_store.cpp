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
  kv::KvOpData op_data;
  op_data.set_op("Get");
  op_data.set_key(key);
  op_data.set_client_id(client_id);
  op_data.set_request_id(request_id);
  std::string command;
  op_data.SerializeToString(&command);

  // ── Step 2: Submit to Raft and create wait channel ────────────
  // CRITICAL: Both start() and wait channel creation MUST be in
  // the same critical section.  See kv_store.h for the race-free
  // creation rule explanation.
  std::shared_ptr<WaitChannel> ch;
  int idx;
  {
    std::lock_guard<std::mutex> lk(mu_);
    StartResult sr = raft_->start(command);
    if (!sr.is_leader) {
      return {/*value=*/"", /*error=*/ErrCode::ErrWrongLeader};
    }
    ch = get_or_create_wait_channel(sr.index);
    idx = sr.index;
  }
  // Lock released — now safe to block.

  // ── Step 3: Wait for apply ────────────────────────────────────
  ApplyResult result;
  bool ok = ch->try_pop(result, std::chrono::milliseconds(5000));
  if (!ok) {
    result.error = ErrCode::ErrTimeout;
  }

  close_and_remove_wait_channel(idx);
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
  kv::KvOpData op_data;
  op_data.set_op(op);
  op_data.set_key(key);
  op_data.set_value(value);
  op_data.set_client_id(client_id);
  op_data.set_request_id(request_id);
  std::string command;
  op_data.SerializeToString(&command);

  // ── Step 2: Submit to Raft and create wait channel ────────────
  std::shared_ptr<WaitChannel> ch;
  int idx;
  {
    std::lock_guard<std::mutex> lk(mu_);
    StartResult sr = raft_->start(command);
    if (!sr.is_leader) {
      return ErrCode::ErrWrongLeader;
    }
    ch = get_or_create_wait_channel(sr.index);
    idx = sr.index;
  }

  // ── Step 3: Wait for apply ────────────────────────────────────
  ApplyResult result;
  bool ok = ch->try_pop(result, std::chrono::milliseconds(5000));
  if (!ok) {
    result.error = ErrCode::ErrTimeout;
  }
  close_and_remove_wait_channel(idx);
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
  kv::KvOpData op_data;
  op_data.ParseFromString(msg.command);
  KvOp op;
  op.op = op_data.op();
  op.key = op_data.key();
  op.value = op_data.value();
  op.client_id = op_data.client_id();
  op.request_id = op_data.request_id();

  // ── Step 2: Execute under lock ────────────────────────────────
  ApplyResult result;
  {
    std::lock_guard<std::mutex> lk(mu_);

    result.error = ErrCode::OK;
    if (is_duplicate(op.client_id, op.request_id)) {
      // Duplicate — skip mutation but still read for Get.
      if (op.op == "Get") {
        auto it = data_.find(op.key);
        result.value = (it != data_.end()) ? it->second : "";
      }
    } else {
      if (op.op == "Get") {
        auto it = data_.find(op.key);
        result.value = (it != data_.end()) ? it->second : "";
      } else if (op.op == "Put") {
        data_[op.key] = op.value;
      } else if (op.op == "Append") {
        data_[op.key] += op.value;
      } else {
        result.error = ErrCode::ErrNoKey;
      }
      last_request_id_[op.client_id] = op.request_id;
    }

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
  auto it = last_request_id_.find(client_id);
  return it != last_request_id_.end() && it->second >= request_id;
}

// ── Wait Channel Helpers ─────────────────────────────────────────
// All require mu_ held.

std::shared_ptr<KvStore::WaitChannel>
KvStore::get_or_create_wait_channel(int index) {
  auto it = wait_channels_.find(index);
  if (it != wait_channels_.end()) {
    return it->second;
  }
  auto ch = std::make_shared<WaitChannel>();
  wait_channels_[index] = ch;
  return ch;
}

void KvStore::notify_wait_channel(int index, ApplyResult result) {
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
