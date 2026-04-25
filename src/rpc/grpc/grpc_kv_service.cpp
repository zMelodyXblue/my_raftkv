#include "rpc/grpc/grpc_kv_service.h"

#include "kv_store/kv_store.h"

namespace raftkv {

KvServiceImpl::KvServiceImpl(std::shared_ptr<KvStore> kv_store)
    : kv_store_(std::move(kv_store)) {}

grpc::Status KvServiceImpl::Get(
    grpc::ServerContext* /*ctx*/,
    const kv::GetRequest* request,
    kv::GetReply* reply) {
  KvStore::GetResult result = kv_store_->get(
      request->key(),
      request->client_id(),
      request->request_id());
  reply->set_value(result.value);
  reply->set_error(result.error);
  return grpc::Status::OK;
}

grpc::Status KvServiceImpl::PutAppend(
    grpc::ServerContext* /*ctx*/,
    const kv::PutAppendRequest* request,
    kv::PutAppendReply* reply) {
  std::string err = kv_store_->put_append(
      request->key(),
      request->value(),
      request->op(),
      request->client_id(),
      request->request_id());
  reply->set_error(err);
  return grpc::Status::OK;
}

}  // namespace raftkv
