#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "client/kv_client.h"
#include "common/config_loader.h"

// ── Command-line parsing ─────────────────────────────────────────
// Usage:
//   raftkv_cli --config <file> get <key>
//   raftkv_cli --peers <addr0,addr1,...> get <key>

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

static void usage() {
  std::cerr << "Usage:\n"
            << "  raftkv_cli --config <file> | --peers <addr0,addr1,...>\n"
            << "             get <key>\n"
            << "             put <key> <value>\n"
            << "             append <key> <value>\n";
}

int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::warn);

  std::vector<std::string> peers;
  int cmd_start = -1;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "--config" || arg == "-config") && i + 1 < argc) {
      raftkv::ServerConfig cfg = raftkv::load_config(argv[++i]);
      peers = cfg.peer_addrs;
    } else if ((arg == "--peers" || arg == "-peers") && i + 1 < argc) {
      peers = split_by_comma(argv[++i]);
    } else if (cmd_start == -1) {
      cmd_start = i;
      break;
    }
  }

  if (peers.empty() || cmd_start == -1) {
    usage();
    return 1;
  }

  raftkv::KvClient client(peers);
  std::string cmd = argv[cmd_start];

  try {
    if (cmd == "get") {
      if (cmd_start + 1 >= argc) { usage(); return 1; }
      std::string key = argv[cmd_start + 1];
      std::string val = client.get(key);
      std::cout << val << "\n";
    } else if (cmd == "put") {
      if (cmd_start + 2 >= argc) { usage(); return 1; }
      std::string key = argv[cmd_start + 1];
      std::string val = argv[cmd_start + 2];
      client.put(key, val);
      std::cout << "OK\n";
    } else if (cmd == "append") {
      if (cmd_start + 2 >= argc) { usage(); return 1; }
      std::string key = argv[cmd_start + 1];
      std::string val = argv[cmd_start + 2];
      client.append(key, val);
      std::cout << "OK\n";
    } else {
      std::cerr << "Unknown command: " << cmd << "\n";
      usage();
      return 1;
    }
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
