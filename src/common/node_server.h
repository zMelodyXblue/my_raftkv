#pragma once

#include <memory>
#include <string>

namespace raftkv {

class Raft;
class KvStore;

// ═════════════════════════════════════════════════════════════════
// NodeServer — Abstract interface for the combined Raft + KV server
// ═════════════════════════════════════════════════════════════════
// Encapsulates transport-specific server lifecycle.
// Implementations: GrpcNodeServer (src/rpc/grpc/).
//
class NodeServer {
 public:
  virtual ~NodeServer() = default;

  // Build and start the server on `listen_addr`.
  // Registers both Raft inter-node handlers and KV client handlers.
  virtual void start(const std::string& listen_addr,
                     std::shared_ptr<Raft> raft,
                     std::shared_ptr<KvStore> kv) = 0;

  // Initiate graceful shutdown (non-blocking).
  virtual void shutdown() = 0;

  // Block until the server finishes (e.g., after shutdown()).
  virtual void wait() = 0;
};

}  // namespace raftkv
