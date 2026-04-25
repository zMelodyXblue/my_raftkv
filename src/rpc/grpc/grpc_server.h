#pragma once

#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "common/rpc_server.h"
#include "rpc/grpc/grpc_raft_service.h"
#include "rpc/grpc/grpc_kv_service.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// GrpcRpcServer — gRPC implementation of RpcServer
// ═════════════════════════════════════════════════════════════════
class GrpcRpcServer : public RpcServer {
 public:
  GrpcRpcServer() = default;

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
