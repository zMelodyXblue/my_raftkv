#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "proto/kv.grpc.pb.h"
#include "common/kv_rpc_client.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// GrpcKvRpcClient — gRPC implementation of KvRpcClient
// ═════════════════════════════════════════════════════════════════
class GrpcKvRpcClient : public KvRpcClient {
 public:
  explicit GrpcKvRpcClient(const std::string& addr);

  bool get(const std::string& key,
           const std::string& client_id,
           int64_t request_id,
           std::string* out_value,
           std::string* out_error) override;

  bool put_append(const std::string& key,
                  const std::string& value,
                  const std::string& op,
                  const std::string& client_id,
                  int64_t request_id,
                  std::string* out_error) override;

 private:
  std::unique_ptr<kv::KvService::Stub> stub_;
};

// Convenience factory: create one GrpcKvRpcClient per address.
std::vector<std::unique_ptr<KvRpcClient>>
make_grpc_kv_clients(const std::vector<std::string>& addrs);

}  // namespace raftkv
