#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/kv_rpc_client.h"

namespace raftkv {

// ── Client retry options ─────────────────────────────────────────
struct ClientOptions {
  int max_retries       = 50;     // Max retry attempts per request (0 = unlimited)
  int total_timeout_ms  = 30000;  // Total timeout per request in ms (0 = unlimited)
  int retry_interval_ms = 100;    // Sleep between retries in ms
};

// ═════════════════════════════════════════════════════════════════
// KvClient — Client library for the Raft KV cluster
// ═════════════════════════════════════════════════════════════════
// Features:
//   - Automatic leader discovery (round-robin retry)
//   - Request deduplication (unique client_id + incrementing request_id)
//   - Transparent retry on ErrWrongLeader / timeout
//   - Configurable retry limit and total timeout
//
class KvClient {
 public:
  // Takes ownership of per-server RPC clients.
  explicit KvClient(std::vector<std::unique_ptr<KvRpcClient>> clients,
                    const ClientOptions& opts = ClientOptions());

  std::string get(const std::string& key);
  void put(const std::string& key, const std::string& value);
  void append(const std::string& key, const std::string& value);

 private:
  std::string put_append(const std::string& key,
                         const std::string& value,
                         const std::string& op);

  std::vector<std::unique_ptr<KvRpcClient>> clients_;
  ClientOptions opts_;
  int         leader_hint_ = 0;
  std::string client_id_;
  int64_t     next_request_id_ = 1;
};

}  // namespace raftkv
