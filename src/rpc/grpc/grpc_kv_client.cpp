#include "rpc/grpc/grpc_kv_client.h"

namespace raftkv {

GrpcKvRpcClient::GrpcKvRpcClient(const std::string& addr)
    : stub_(kv::KvService::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

bool GrpcKvRpcClient::get(const std::string& key,
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

bool GrpcKvRpcClient::put_append(const std::string& key,
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

std::vector<std::unique_ptr<KvRpcClient>>
make_grpc_kv_clients(const std::vector<std::string>& addrs) {
  std::vector<std::unique_ptr<KvRpcClient>> clients;
  clients.reserve(addrs.size());
  for (const auto& addr : addrs) {
    clients.push_back(std::unique_ptr<KvRpcClient>(new GrpcKvRpcClient(addr)));
  }
  return clients;
}

}  // namespace raftkv
