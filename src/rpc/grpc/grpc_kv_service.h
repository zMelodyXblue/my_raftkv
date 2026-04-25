#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>
#include "proto/kv.grpc.pb.h"

namespace raftkv {

class KvStore;

// ═════════════════════════════════════════════════════════════════
// KvServiceImpl — gRPC server-side handler for KV client requests
// ═════════════════════════════════════════════════════════════════
class KvServiceImpl final : public kv::KvService::Service {
 public:
  explicit KvServiceImpl(std::shared_ptr<KvStore> kv_store);

  grpc::Status Get(
      grpc::ServerContext* ctx,
      const kv::GetRequest* request,
      kv::GetReply* reply) override;

  grpc::Status PutAppend(
      grpc::ServerContext* ctx,
      const kv::PutAppendRequest* request,
      kv::PutAppendReply* reply) override;

 private:
  std::shared_ptr<KvStore> kv_store_;
};

}  // namespace raftkv
