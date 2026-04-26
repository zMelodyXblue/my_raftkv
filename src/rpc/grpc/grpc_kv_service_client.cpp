#include "rpc/grpc/grpc_kv_service_client.h"

namespace raftkv {

GrpcKvServiceClient::GrpcKvServiceClient(const std::string& addr)
    : stub_(kv::KvService::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

bool GrpcKvServiceClient::get(const std::string& key,
                               const std::string& client_id,
                               int64_t request_id,
                               std::string* out_value,
                               std::string* out_error) {
  kv::GetRequest request;
  request.set_key(key);
  request.set_client_id(client_id);
  request.set_request_id(request_id);

  grpc::ClientContext ctx;
  kv::GetReply reply;
  grpc::Status status = stub_->Get(&ctx, request, &reply);
  if (!status.ok()) return false;

  *out_value = reply.value();
  *out_error = reply.error();
  return true;
}

bool GrpcKvServiceClient::put_append(const std::string& key,
                                      const std::string& value,
                                      const std::string& op,
                                      const std::string& client_id,
                                      int64_t request_id,
                                      std::string* out_error) {
  kv::PutAppendRequest request;
  request.set_key(key);
  request.set_value(value);
  request.set_op(op);
  request.set_client_id(client_id);
  request.set_request_id(request_id);

  grpc::ClientContext ctx;
  kv::PutAppendReply reply;
  grpc::Status status = stub_->PutAppend(&ctx, request, &reply);
  if (!status.ok()) return false;

  *out_error = reply.error();
  return true;
}

std::vector<std::unique_ptr<KvServiceClient>>
make_grpc_kv_service_clients(const std::vector<std::string>& addrs) {
  std::vector<std::unique_ptr<KvServiceClient>> clients;
  clients.reserve(addrs.size());
  for (const auto& addr : addrs) {
    clients.push_back(std::unique_ptr<KvServiceClient>(new GrpcKvServiceClient(addr)));
  }
  return clients;
}

std::vector<std::shared_ptr<KvServiceClient>>
make_shared_grpc_kv_service_clients(const std::vector<std::string>& addrs) {
  std::vector<std::shared_ptr<KvServiceClient>> clients;
  clients.reserve(addrs.size());
  for (const auto& addr : addrs) {
    clients.push_back(std::make_shared<GrpcKvServiceClient>(addr));
  }
  return clients;
}

}  // namespace raftkv
