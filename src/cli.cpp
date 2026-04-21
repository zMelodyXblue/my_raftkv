#include <iostream>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "client/kv_client.h"

// ── Command-line parsing ─────────────────────────────────────────
// Usage:
//   raftkv_cli --peers <addr0>,<addr1>,<addr2> get <key>
//   raftkv_cli --peers <addr0>,<addr1>,<addr2> put <key> <value>
//   raftkv_cli --peers <addr0>,<addr1>,<addr2> append <key> <value>

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
            << "  raftkv_cli --peers <addr0,addr1,...> get <key>\n"
            << "  raftkv_cli --peers <addr0,addr1,...> put <key> <value>\n"
            << "  raftkv_cli --peers <addr0,addr1,...> append <key> <value>\n";
}

int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::warn);

  std::vector<std::string> peers;
  int cmd_start = -1;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "--peers" || arg == "-peers") && i + 1 < argc) {
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

  return 0;
}
