#include "kv_store/kv_store.h"

#include <spdlog/spdlog.h>

#include "common/hashmap_engine.h"
#include "common/skiplist_engine.h"
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
      apply_channel_(std::move(apply_channel)),
      data_(config.engine_type == "skiplist"
                ? static_cast<KvEngine*>(new SkipListEngine())
                : static_cast<KvEngine*>(new HashMapEngine())) {
  // Restore from persisted snapshot before starting the apply thread.
  std::string snap = raft_->read_persisted_snapshot();
  if (!snap.empty()) {
    restore_snapshot(snap);
    last_snapshot_index_ = raft_->snapshot_index();
    applied_index_ = last_snapshot_index_;
  }

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
// Uses ReadIndex optimization: confirms leadership via heartbeat,
// waits for apply to catch up, then reads directly from state machine.
// No log entry is written for GET requests.
KvStore::GetResult KvStore::get(const std::string& key,
                                const std::string& /* client_id */,
                                int64_t /* request_id */) {
  return read_index_get(key);
}

// Called by KvServiceImpl::PutAppend().
// Submits command to Raft log, waits for commit + apply, returns error status.
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

// ── ReadIndex (read optimization) ─────────────────────────────────

void KvStore::wait_until_applied(int index) {
  std::unique_lock<std::mutex> lk(mu_);
  applied_cv_.wait_for(lk, std::chrono::milliseconds(5000),
      [this, index] { return applied_index_ >= index || !running_; });
}

KvStore::GetResult KvStore::read_index_get(const std::string& key) {
  int ri = raft_->read_index();
  if (ri < 0) {
    return {"", ErrCode::ErrWrongLeader};
  }

  wait_until_applied(ri);

  std::lock_guard<std::mutex> lk(mu_);
  if (applied_index_ < ri) {
    return {"", ErrCode::ErrTimeout};
  }
  std::string val;
  data_->get(key, &val);
  return {val, ErrCode::OK};
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
  // No-op entry (empty command) — used by leader to commit its term.
  if (msg.command.empty()) {
    std::lock_guard<std::mutex> lk(mu_);
    applied_index_ = msg.command_index;
    applied_cv_.notify_all();
    return;
  }

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
      // Duplicate — return cached result from last applied entry.
      result.value = last_applied_[op.client_id].value;
    } else {
      if (op.op == "Get") {
        std::string val;
        if (data_->get(op.key, &val)) result.value = val;
      } else if (op.op == "Put") {
        data_->put(op.key, op.value);
      } else if (op.op == "Append") {
        data_->append(op.key, op.value);
      } else {
        result.error = ErrCode::ErrNoKey;
      }
      last_applied_[op.client_id] = {op.request_id, result.value};
    }

    // Notify the RPC handler waiting on this log index.
    notify_wait_channel(msg.command_index, result);
    applied_index_ = msg.command_index;
    spdlog::debug("[Node {}] [KvStore] applied {} key={} at index={}",
                  config_.node_id, op.op, op.key, msg.command_index);
  }
  applied_cv_.notify_all();

  // Check if snapshot is needed to compact the WAL.
  maybe_take_snapshot(msg.command_index);
}

// Apply a snapshot received from Raft (via InstallSnapshot RPC).
// Called by apply_loop() when it receives a snapshot ApplyMsg.
void KvStore::apply_snapshot(const ApplyMsg& msg) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (msg.snapshot_index <= last_snapshot_index_) return;
    restore_snapshot(msg.snapshot);
    last_snapshot_index_ = msg.snapshot_index;
    if (msg.snapshot_index > applied_index_) {
      applied_index_ = msg.snapshot_index;
    }
    spdlog::info("[Node {}] [KvStore] applied remote snapshot (index={})",
                 config_.node_id, msg.snapshot_index);
  }
  applied_cv_.notify_all();
}

// ── Snapshot ─────────────────────────────────────────────────────

// Check if raft state is too large; if so, take a snapshot to compact the log.
// Called after each command is applied (outside mu_ in apply_command).
void KvStore::maybe_take_snapshot(int applied_index) {
  if (config_.raft.max_raft_state_bytes < 0) return;
  if (raft_->raft_state_size() <= config_.raft.max_raft_state_bytes) return;

  spdlog::info("[Node {}] [KvStore] triggering snapshot at index {}",
               config_.node_id, applied_index);
  std::string snap;
  {
    std::lock_guard<std::mutex> lk(mu_);
    snap = serialize_snapshot();
    last_snapshot_index_ = applied_index;
  }
  raft_->snapshot(applied_index, snap);
}

// Serialize data_ and last_applied_ into a kv::KvSnapshot protobuf string.
// Requires mu_ held.
std::string KvStore::serialize_snapshot() {
  kv::KvSnapshot snapshot;
  auto* data_map = snapshot.mutable_data();
  data_->for_each([data_map](const std::string& k, const std::string& v) {
    (*data_map)[k] = v;
  });
  for (const auto& kv : last_applied_) {
    kv::DedupEntry* entry = snapshot.add_dedup();
    entry->set_client_id(kv.first);
    entry->set_request_id(kv.second.request_id);
    entry->set_value(kv.second.value);
  }
  std::string bytes;
  snapshot.SerializeToString(&bytes);
  return bytes;
}

// Deserialize a kv::KvSnapshot and replace the current KV state.
// Requires mu_ held (called from constructor and apply_snapshot).
void KvStore::restore_snapshot(const std::string& data) {
  kv::KvSnapshot snapshot;
  if (!snapshot.ParseFromString(data)) return;

  data_->clear();
  for (const auto& kv : snapshot.data()) {
    data_->put(kv.first, kv.second);
  }
  last_applied_.clear();
  for (int i = 0; i < snapshot.dedup_size(); ++i) {
    const kv::DedupEntry& e = snapshot.dedup(i);
    last_applied_[e.client_id()] = {e.request_id(), e.value()};
  }
}

// ── Deduplication ────────────────────────────────────────────────

// Returns true if the request has already been applied.
// Requires mu_ held.
bool KvStore::is_duplicate(const std::string& client_id, int64_t request_id) {
  auto it = last_applied_.find(client_id);
  return it != last_applied_.end() && it->second.request_id >= request_id;
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
