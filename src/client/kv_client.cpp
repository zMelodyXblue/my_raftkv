#include "client/kv_client.h"

#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <spdlog/spdlog.h>
#include "common/types.h"

namespace raftkv {

static std::string generate_client_id() {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dist;
  std::ostringstream oss;
  oss << std::hex << dist(gen) << "-" << dist(gen);
  return oss.str();
}

KvClient::KvClient(std::vector<std::unique_ptr<KvServiceClient>> clients,
                   const ClientOptions& opts)
    : opts_(opts), client_id_(generate_client_id()) {
  // Convert unique_ptr → shared_ptr for uniform internal storage.
  clients_.reserve(clients.size());
  for (auto& c : clients) {
    clients_.push_back(std::shared_ptr<KvServiceClient>(c.release()));
  }
}

KvClient::KvClient(std::vector<std::shared_ptr<KvServiceClient>> clients,
                   const ClientOptions& opts)
    : clients_(std::move(clients)), opts_(opts),
      client_id_(generate_client_id()) {}

std::string KvClient::get(const std::string& key) {
  int64_t rid = next_request_id_++;
  auto start = std::chrono::steady_clock::now();

  for (int attempt = 0;; ++attempt) {
    if (opts_.max_retries > 0 && attempt >= opts_.max_retries) {
      throw std::runtime_error(
          "KvClient::get: failed after " + std::to_string(attempt) + " retries");
    }
    if (opts_.total_timeout_ms > 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start).count();
      if (elapsed >= opts_.total_timeout_ms) {
        throw std::runtime_error(
            "KvClient::get: timed out after " + std::to_string(elapsed) + "ms");
      }
    }

    std::string value, error;
    bool ok = clients_[leader_hint_]->get(key, client_id_, rid, &value, &error);

    if (!ok) {
      spdlog::debug("Get RPC to server {} failed", leader_hint_);
      leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    }

    if (error == ErrCode::OK || error == ErrCode::ErrNoKey) {
      return value;
    } else if (error == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    } else if (error == ErrCode::ErrTimeout) {
      continue;
    }

    leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
  }
}

void KvClient::put(const std::string& key, const std::string& value) {
  put_append(key, value, "Put");
}

void KvClient::append(const std::string& key, const std::string& value) {
  put_append(key, value, "Append");
}

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

    std::string error;
    bool ok = clients_[leader_hint_]->put_append(key, value, op, client_id_, rid, &error);

    if (!ok) {
      spdlog::debug("PutAppend RPC to server {} failed", leader_hint_);
      leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    }

    if (error == ErrCode::OK) {
      return "";
    } else if (error == ErrCode::ErrWrongLeader) {
      leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
      continue;
    } else if (error == ErrCode::ErrTimeout) {
      continue;
    }

    leader_hint_ = (leader_hint_ + 1) % static_cast<int>(clients_.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(opts_.retry_interval_ms));
  }
}

}  // namespace raftkv
