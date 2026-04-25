#pragma once

#include <cstdint>
#include <string>

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// KvRpcClient — Abstract interface for a single KV server connection
// ═════════════════════════════════════════════════════════════════
// KvClient holds one KvRpcClient per server and uses it for
// round-robin leader discovery.  Implementations: GrpcKvRpcClient.
//
class KvRpcClient {
 public:
  virtual ~KvRpcClient() = default;

  // Returns true if the RPC succeeded at the network level.
  // On success, *out_value and *out_error are filled.
  virtual bool get(const std::string& key,
                   const std::string& client_id,
                   int64_t request_id,
                   std::string* out_value,
                   std::string* out_error) = 0;

  // Returns true if the RPC succeeded at the network level.
  // On success, *out_error is filled (empty = OK).
  virtual bool put_append(const std::string& key,
                          const std::string& value,
                          const std::string& op,
                          const std::string& client_id,
                          int64_t request_id,
                          std::string* out_error) = 0;
};

}  // namespace raftkv
