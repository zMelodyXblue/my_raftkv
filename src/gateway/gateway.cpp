#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "client/kv_client.h"
#include "common/config_loader.h"
#include "common/nlohmann/json.hpp"
#include "raft.grpc.pb.h"
#include "gateway/httplib.h"

// ── Command-line parsing ────────────────────────────────────────
struct GatewayArgs {
  std::vector<std::string> peers;
  int         port    = 8080;
  std::string web_dir = "./web";
};

static std::vector<std::string> split_by_comma(const std::string& s) {
  std::vector<std::string> result;
  size_t start = 0;
  for (size_t i = 0; i <= s.size(); ++i) {
    if (i == s.size() || s[i] == ',') {
      result.push_back(s.substr(start, i - start));
      start = i + 1;
    }
  }
  return result;
}

static GatewayArgs parse_args(int argc, char* argv[]) {
  GatewayArgs args;
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if ((flag == "--config") && i + 1 < argc) {
      raftkv::ServerConfig cfg = raftkv::load_config(argv[++i]);
      args.peers = cfg.peer_addrs;
    } else if ((flag == "--peers") && i + 1 < argc) {
      args.peers = split_by_comma(argv[++i]);
    } else if ((flag == "--port") && i + 1 < argc) {
      args.port = std::stoi(argv[++i]);
    } else if ((flag == "--web") && i + 1 < argc) {
      args.web_dir = argv[++i];
    }
  }
  return args;
}

// ── Fetch status from one node via gRPC ─────────────────────────
static nlohmann::json fetch_node_status(const std::string& addr) {
  auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  auto stub = raftkv::raft::RaftService::NewStub(channel);

  raftkv::raft::GetStatusRequest req;
  raftkv::raft::GetStatusReply reply;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() +
                   std::chrono::milliseconds(500));

  grpc::Status status = stub->GetStatus(&ctx, req, &reply);

  nlohmann::json j;
  j["addr"] = addr;
  if (status.ok()) {
    j["online"]             = true;
    j["node_id"]            = reply.node_id();
    j["term"]               = reply.term();
    j["role"]               = reply.role();
    j["last_log_index"]     = reply.last_log_index();
    j["commit_index"]       = reply.commit_index();
    j["last_applied"]       = reply.last_applied();
    j["last_snapshot_index"] = reply.last_snapshot_index();
  } else {
    j["online"] = false;
    spdlog::warn("GetStatus failed for {}: code={} msg={}",
                 addr, static_cast<int>(status.error_code()),
                 status.error_message());
  }
  return j;
}

// ── Main ────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::info);

  GatewayArgs args = parse_args(argc, argv);
  if (args.peers.empty()) {
    std::cerr << "Usage: raftkv_gateway --config <file> | --peers <addr0,...>\n"
              << "                      [--port 8080] [--web ./web]\n";
    return 1;
  }

  raftkv::KvClient kv_client(args.peers);
  // Keep a copy for status queries
  std::vector<std::string> peers = args.peers;

  httplib::Server svr;

  // ── KV API ──────────────────────────────────────────────────────

  // GET /api/kv/:key
  svr.Get(R"(/api/kv/(.+))", [&kv_client](const httplib::Request& req,
                                           httplib::Response& res) {
    std::string key = req.matches[1];
    try {
      std::string value = kv_client.get(key);
      nlohmann::json j;
      j["key"]   = key;
      j["value"] = value;
      res.set_content(j.dump(), "application/json");
    } catch (const std::exception& e) {
      res.status = 500;
      nlohmann::json j;
      j["error"] = e.what();
      res.set_content(j.dump(), "application/json");
    }
  });

  // PUT /api/kv/:key  body = raw value
  svr.Put(R"(/api/kv/(.+))", [&kv_client](const httplib::Request& req,
                                           httplib::Response& res) {
    std::string key   = req.matches[1];
    std::string value = req.body;
    try {
      kv_client.put(key, value);
      nlohmann::json j;
      j["status"] = "ok";
      j["key"]    = key;
      res.set_content(j.dump(), "application/json");
    } catch (const std::exception& e) {
      res.status = 500;
      nlohmann::json j;
      j["error"] = e.what();
      res.set_content(j.dump(), "application/json");
    }
  });

  // POST /api/kv/:key/append  body = raw value
  svr.Post(R"(/api/kv/(.+)/append)", [&kv_client](const httplib::Request& req,
                                                    httplib::Response& res) {
    std::string key   = req.matches[1];
    std::string value = req.body;
    try {
      kv_client.append(key, value);
      nlohmann::json j;
      j["status"] = "ok";
      j["key"]    = key;
      res.set_content(j.dump(), "application/json");
    } catch (const std::exception& e) {
      res.status = 500;
      nlohmann::json j;
      j["error"] = e.what();
      res.set_content(j.dump(), "application/json");
    }
  });

  // ── Cluster Status API ──────────────────────────────────────────

  // GET /api/cluster/status
  svr.Get("/api/cluster/status", [&peers](const httplib::Request& /*req*/,
                                          httplib::Response& res) {
    nlohmann::json nodes = nlohmann::json::array();
    for (const auto& addr : peers) {
      nodes.push_back(fetch_node_status(addr));
    }
    nlohmann::json j;
    j["nodes"] = nodes;
    res.set_content(j.dump(), "application/json");
  });

  // ── Static file serving ─────────────────────────────────────────
  svr.set_mount_point("/", args.web_dir.c_str());

  spdlog::info("Gateway listening on http://localhost:{}", args.port);
  spdlog::info("Web root: {}", args.web_dir);
  svr.listen("0.0.0.0", args.port);

  return 0;
}
