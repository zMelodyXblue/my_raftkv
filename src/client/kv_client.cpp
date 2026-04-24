#include "client/kv_client.h"

#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <spdlog/spdlog.h>
#include "common/types.h"

namespace raftkv {

// ── Constructor ──────────────────────────────────────────────────
// Creates gRPC stubs for all servers and generates a unique client_id.
KvClient::KvClient(const std::vector<std::string>& server_addrs,
                   const ClientOptions& opts)
    : opts_(opts) {
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
// Throws std::runtime_error if max_retries or total_timeout is exceeded.
std::string KvClient::get(const std::string& key) {
  int64_t rid = next_request_id_++;
  auto start = std::chrono::steady_clock::now();

  for (int attempt = 0;; ++attempt) {
    // Check retry limit.
    if (opts_.max_retries > 0 && attempt >= opts_.max_retries) {
      throw std::runtime_error(
          "KvClient::get: failed after " + std::to_string(attempt) + " retries");
    }
    // Check total timeout.
    if (opts_.total_timeout_ms > 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start).count();
      if (elapsed >= opts_.total_timeout_ms) {
        throw std::runtime_error(
            "KvClient::get: timed out after " + std::to_string(elapsed) + "ms");
      }
    }

    kv::GetRequest request;
    request.set_key(key);
    request.set_client_id(client_id_);
    request.set_request_id(rid);

    grpc::ClientContext ctx;
    kv::GetReply reply;
    grpc::Status status = stubs_[leader_hint_]->Get(&ctx, request, &reply);

    if (!status.ok()) {
      spdlog::debug("Get RPC to server {} failed: {}",
                    leader_hint_, status.error_message());
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    }

    if (reply.error() == ErrCode::OK || reply.error() == ErrCode::ErrNoKey) {
      return reply.value();
    } else if (reply.error() == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    } else if (reply.error() == ErrCode::ErrTimeout) {
      continue;
    }

    // Unknown error — retry with next server
    leader_hint_ = (leader_hint_ + 1) % stubs_.size();
    std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
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
// Throws std::runtime_error if max_retries or total_timeout is exceeded.
std::string KvClient::put_append(const std::string& key,
                                 const std::string& value,
                                 const std::string& op) {
  int64_t rid = next_request_id_++;
  auto start = std::chrono::steady_clock::now();

  for (int attempt = 0;; ++attempt) {
    if (opts_.max_retries > 0 && attempt >= opts_.max_retries) {
      throw std::runtime_error(
          "KvClient::" + op + ": failed after " + std::to_string(attempt) + " retries");
    }
    if (opts_.total_timeout_ms > 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start).count();
      if (elapsed >= opts_.total_timeout_ms) {
        throw std::runtime_error(
            "KvClient::" + op + ": timed out after " + std::to_string(elapsed) + "ms");
      }
    }

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
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    }

    if (reply.error() == ErrCode::OK) {
      return "";
    } else if (reply.error() == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    } else if (reply.error() == ErrCode::ErrTimeout) {
      continue;
    }

    // Unknown error — retry with next server
    leader_hint_ = (leader_hint_ + 1) % stubs_.size();
    std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
  }
}

}  // namespace raftkv
