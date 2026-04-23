#include "client/kv_client.h"

#include <chrono>
#include <random>
#include <sstream>
#include <thread>

#include <spdlog/spdlog.h>
#include "common/types.h"

namespace raftkv {

// ── Constructor ──────────────────────────────────────────────────
// Creates gRPC stubs for all servers and generates a unique client_id.
KvClient::KvClient(const std::vector<std::string>& server_addrs) {
  for (const auto& addr : server_addrs) {
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    stubs_.push_back(kv::KvService::NewStub(channel));
  }

  // Generate a unique client_id from random_device + mt19937.
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dist;
  std::ostringstream oss;
  oss << std::hex << dist(gen) << "-" << dist(gen);
  client_id_ = oss.str();
}

// ── Get ──────────────────────────────────────────────────────────
// Sends a Get request, retrying on ErrWrongLeader / failure.
std::string KvClient::get(const std::string& key) {
  int64_t rid = next_request_id_++;
  while (true) {
    kv::GetRequest request;
    request.set_key(key);
    request.set_client_id(client_id_);
    request.set_request_id(rid);

    grpc::ClientContext ctx;
    kv::GetReply reply;
    grpc::Status status = stubs_[leader_hint_]->Get(&ctx, request, &reply);

    if (!status.ok()) {
      // gRPC transport failure — try next server
      spdlog::debug("Get RPC to server {} failed: {}",
                    leader_hint_, status.error_message());
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    if (reply.error() == ErrCode::OK || reply.error() == ErrCode::ErrNoKey) {
      return reply.value();
    } else if (reply.error() == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    } else if (reply.error() == ErrCode::ErrTimeout) {
      continue;
    }

    // Unknown error — retry with next server
    leader_hint_ = (leader_hint_ + 1) % stubs_.size();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
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
    kv::PutAppendRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_op(op);
    request.set_client_id(client_id_);
    request.set_request_id(rid);

    grpc::ClientContext ctx;
    kv::PutAppendReply reply;
    grpc::Status status = stubs_[leader_hint_]->PutAppend(&ctx, request, &reply);

    if (!status.ok()) {
      spdlog::debug("PutAppend RPC to server {} failed: {}",
                    leader_hint_, status.error_message());
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    if (reply.error() == ErrCode::OK) {
      return "";
    } else if (reply.error() == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    } else if (reply.error() == ErrCode::ErrTimeout) {
      continue;
    }

    // Unknown error — retry with next server
    leader_hint_ = (leader_hint_ + 1) % stubs_.size();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

}  // namespace raftkv
