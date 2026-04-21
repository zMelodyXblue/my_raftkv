#include "client/kv_client.h"

#include <random>
#include <sstream>
#include <spdlog/spdlog.h>

namespace raftkv {

// ── Constructor ──────────────────────────────────────────────────
// Creates gRPC stubs for all servers and generates a unique client_id.
KvClient::KvClient(const std::vector<std::string>& server_addrs) {
  // TODO: For each address in server_addrs:
  //   1. Create a gRPC channel: grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())
  //   2. Create a stub: kv::KvService::NewStub(channel)
  //   3. Push the stub into stubs_
  for (const auto& addr : server_addrs) {
    (void)addr;
    // TODO: create channel and stub
  }

  // TODO: Generate a unique client_id.
  // Use std::random_device + mt19937 to generate a random UUID-like string,
  // or use a simple combination of timestamp + random number.
  // Store it in client_id_.
  client_id_ = "";  // TODO: generate unique ID
}

// ── Get ──────────────────────────────────────────────────────────
// Sends a Get request, retrying on ErrWrongLeader / failure.
std::string KvClient::get(const std::string& key) {
  int64_t rid = next_request_id_++;
  while (true) {
    // TODO: 1. Construct a kv::GetRequest with key, client_id_, rid.
    //       2. Create a grpc::ClientContext (with deadline if desired).
    //       3. Call stubs_[leader_hint_]->Get(&ctx, request, &reply).
    //       4. Check the gRPC status and reply.error():
    //          - If error is "" (OK) or "ErrNoKey": return reply.value().
    //          - If error is "ErrWrongLeader" or gRPC failed:
    //            advance leader_hint_ = (leader_hint_ + 1) % stubs_.size()
    //            and retry.
    //          - If error is "ErrTimeout": retry with same leader_hint_.
    //       5. Between retries, sleep briefly (e.g., 100ms) to avoid
    //          busy-looping during a leader election.

    // TODO: implement retry loop body
    break;  // placeholder
  }
  return "";  // placeholder
}

// ── Put / Append ─────────────────────────────────────────────────

void KvClient::put(const std::string& key, const std::string& value) {
  put_append(key, value, "Put");
}

void KvClient::append(const std::string& key, const std::string& value) {
  put_append(key, value, "Append");
}

// Shared implementation for Put and Append.
// Same retry logic as get().
std::string KvClient::put_append(const std::string& key,
                                 const std::string& value,
                                 const std::string& op) {
  int64_t rid = next_request_id_++;
  while (true) {
    // TODO: 1. Construct a kv::PutAppendRequest with key, value, op,
    //          client_id_, rid.
    //       2. Create a grpc::ClientContext.
    //       3. Call stubs_[leader_hint_]->PutAppend(&ctx, request, &reply).
    //       4. Check the gRPC status and reply.error():
    //          - If error is "" (OK): return "".
    //          - If error is "ErrWrongLeader" or gRPC failed:
    //            advance leader_hint_ and retry.
    //          - If error is "ErrTimeout": retry with same leader_hint_.
    //       5. Brief sleep between retries.

    // TODO: implement retry loop body
    break;  // placeholder
  }
  return "";  // placeholder
}

}  // namespace raftkv
