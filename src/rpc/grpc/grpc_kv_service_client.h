#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "proto/kv.grpc.pb.h"
#include "common/kv_service_client.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// GrpcKvServiceClient — gRPC implementation of KvServiceClient
// ═════════════════════════════════════════════════════════════════
class GrpcKvServiceClient : public KvServiceClient {
 public:
  explicit GrpcKvServiceClient(const std::string& addr);

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

// Convenience factory: create one GrpcKvServiceClient per address (owned).
std::vector<std::unique_ptr<KvServiceClient>>
make_grpc_kv_service_clients(const std::vector<std::string>& addrs);

// Shared factory: create shared GrpcKvServiceClients (for reuse across KvClients).
std::vector<std::shared_ptr<KvServiceClient>>
make_shared_grpc_kv_service_clients(const std::vector<std::string>& addrs);

}  // namespace raftkv
