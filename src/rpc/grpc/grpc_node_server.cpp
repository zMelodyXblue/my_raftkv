#include "rpc/grpc/grpc_node_server.h"

#include <spdlog/spdlog.h>

namespace raftkv {

void GrpcNodeServer::start(const std::string& listen_addr,
                            std::shared_ptr<Raft> raft,
                            std::shared_ptr<KvStore> kv) {
  raft_service_.reset(new RaftServiceImpl(raft));
  kv_service_.reset(new KvServiceImpl(kv));

  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(raft_service_.get());
  builder.RegisterService(kv_service_.get());

  server_ = builder.BuildAndStart();
  if (!server_) {
    spdlog::error("GrpcNodeServer: failed to start on {}", listen_addr);
    throw std::runtime_error("failed to start gRPC server on " + listen_addr);
  }
  spdlog::info("gRPC server listening on {}", listen_addr);
}

void GrpcNodeServer::shutdown() {
  if (server_) server_->Shutdown();
}

void GrpcNodeServer::wait() {
  if (server_) server_->Wait();
}

}  // namespace raftkv
