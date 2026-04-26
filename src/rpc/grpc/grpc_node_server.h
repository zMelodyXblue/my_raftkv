#pragma once

#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "common/node_server.h"
#include "rpc/grpc/grpc_raft_service.h"
#include "rpc/grpc/grpc_kv_service.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// GrpcNodeServer — gRPC implementation of NodeServer
// ═════════════════════════════════════════════════════════════════
class GrpcNodeServer : public NodeServer {
 public:
  GrpcNodeServer() = default;

  void start(const std::string& listen_addr,
             std::shared_ptr<Raft> raft,
             std::shared_ptr<KvStore> kv) override;

  void shutdown() override;
  void wait() override;

 private:
  std::unique_ptr<RaftServiceImpl> raft_service_;
  std::unique_ptr<KvServiceImpl>   kv_service_;
  std::unique_ptr<grpc::Server>    server_;
};

}  // namespace raftkv
