// raftkv_redis_proxy — RESP protocol adapter for redis-benchmark / redis-cli
//
// Translates Redis GET/SET commands into KvClient calls, allowing
// standard Redis tooling to benchmark the raftkv cluster.
//
// Usage:
//   raftkv_redis_proxy --peers 127.0.0.1:50050,... --port 6379
//   redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -c 50 -n 10000

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "client/kv_client.h"
#include "common/config_loader.h"
#include "rpc/grpc/grpc_kv_service_client.h"

// ═════════════════════════════════════════════════════════════════
// RESP Protocol Helpers
// ═════════════════════════════════════════════════════════════════

static std::string resp_simple_string(const std::string& s) {
  return "+" + s + "\r\n";
}

static std::string resp_error(const std::string& msg) {
  return "-ERR " + msg + "\r\n";
}

static std::string resp_bulk_string(const std::string& s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

static std::string resp_null_bulk() {
  return "$-1\r\n";
}

static std::string resp_integer(int64_t n) {
  return ":" + std::to_string(n) + "\r\n";
}

static std::string resp_empty_array() {
  return "*0\r\n";
}

// ═════════════════════════════════════════════════════════════════
// RESP Parser — reads from a buffered TCP stream
// ═════════════════════════════════════════════════════════════════

class RespReader {
 public:
  explicit RespReader(int fd) : fd_(fd) {}

  // Read one RESP command (array of bulk strings).
  // Returns false on EOF or protocol error.
  bool read_command(std::vector<std::string>& out) {
    out.clear();

    // Read first byte to determine format.
    char ch;
    if (!peek_byte(ch)) return false;

    if (ch == '*') {
      return read_array_command(out);
    } else {
      // Inline command: read line, split by spaces.
      return read_inline_command(out);
    }
  }

 private:
  bool read_array_command(std::vector<std::string>& out) {
    std::string line;
    if (!read_line(line)) return false;       // "*N\r\n"
    if (line.empty() || line[0] != '*') return false;

    int count = std::atoi(line.c_str() + 1);
    if (count < 0) return false;

    out.reserve(count);
    for (int i = 0; i < count; ++i) {
      if (!read_line(line)) return false;     // "$len\r\n"
      if (line.empty() || line[0] != '$') return false;

      int len = std::atoi(line.c_str() + 1);
      if (len < 0) { out.push_back(""); continue; }

      std::string data(len, '\0');
      if (!read_exact(&data[0], len)) return false;

      // Consume trailing \r\n
      char crlf[2];
      if (!read_exact(crlf, 2)) return false;

      out.push_back(std::move(data));
    }
    return true;
  }

  bool read_inline_command(std::vector<std::string>& out) {
    std::string line;
    if (!read_line(line)) return false;

    // Split by spaces.
    size_t start = 0;
    while (start < line.size()) {
      size_t end = line.find(' ', start);
      if (end == std::string::npos) end = line.size();
      if (end > start) {
        out.push_back(line.substr(start, end - start));
      }
      start = end + 1;
    }
    return !out.empty();
  }

  // Read until \r\n, return content without \r\n.
  bool read_line(std::string& out) {
    out.clear();
    while (true) {
      char ch;
      if (!read_byte(ch)) return false;
      if (ch == '\r') {
        char lf;
        if (!read_byte(lf)) return false;
        return true;
      }
      out.push_back(ch);
    }
  }

  bool read_exact(char* buf, int n) {
    for (int i = 0; i < n; ++i) {
      char ch;
      if (!read_byte(ch)) return false;
      buf[i] = ch;
    }
    return true;
  }

  bool peek_byte(char& ch) {
    if (pos_ < len_) {
      ch = buf_[pos_];
      return true;
    }
    if (!fill()) return false;
    ch = buf_[pos_];
    return true;
  }

  bool read_byte(char& ch) {
    if (pos_ < len_) {
      ch = buf_[pos_++];
      return true;
    }
    if (!fill()) return false;
    ch = buf_[pos_++];
    return true;
  }

  bool fill() {
    ssize_t n = ::recv(fd_, buf_, sizeof(buf_), 0);
    if (n <= 0) return false;
    pos_ = 0;
    len_ = static_cast<int>(n);
    return true;
  }

  int  fd_;
  char buf_[8192];
  int  pos_ = 0;
  int  len_ = 0;
};

// ═════════════════════════════════════════════════════════════════
// Connection handler
// ═════════════════════════════════════════════════════════════════

static void handle_connection(
    int fd,
    const std::vector<std::shared_ptr<raftkv::KvServiceClient>>& shared_clients) {
  // Each connection gets its own KvClient (leader_hint, request_id),
  // but shares the underlying gRPC channels for efficiency.
  raftkv::ClientOptions copts;
  copts.max_retries      = 50;
  copts.total_timeout_ms = 10000;
  copts.retry_interval_ms = 50;
  raftkv::KvClient kv(shared_clients, copts);

  RespReader reader(fd);
  std::vector<std::string> cmd;

  while (reader.read_command(cmd)) {
    if (cmd.empty()) continue;

    // Uppercase the command name.
    std::string& name = cmd[0];
    for (size_t i = 0; i < name.size(); ++i)
      name[i] = static_cast<char>(toupper(static_cast<unsigned char>(name[i])));

    std::string reply;

    if (name == "PING") {
      if (cmd.size() > 1) {
        reply = resp_bulk_string(cmd[1]);
      } else {
        reply = resp_simple_string("PONG");
      }
    } else if (name == "SET" && cmd.size() >= 3) {
      try {
        kv.put(cmd[1], cmd[2]);
        reply = resp_simple_string("OK");
      } catch (const std::exception& e) {
        reply = resp_error(e.what());
      }
    } else if (name == "GET" && cmd.size() >= 2) {
      try {
        std::string val = kv.get(cmd[1]);
        if (val.empty()) {
          // Could be empty string or non-existent key.
          // KvClient returns "" for both; treat as bulk string.
          reply = resp_bulk_string(val);
        } else {
          reply = resp_bulk_string(val);
        }
      } catch (const std::exception& e) {
        reply = resp_error(e.what());
      }
    } else if (name == "APPEND" && cmd.size() >= 3) {
      try {
        kv.append(cmd[1], cmd[2]);
        // Redis APPEND returns the new length; we don't know it cheaply,
        // so return a placeholder.  redis-benchmark doesn't use APPEND.
        reply = resp_integer(0);
      } catch (const std::exception& e) {
        reply = resp_error(e.what());
      }
    } else if (name == "DEL") {
      // Not supported — return 0 keys deleted.
      reply = resp_integer(0);
    } else if (name == "DBSIZE") {
      reply = resp_integer(0);
    } else if (name == "COMMAND" || name == "CONFIG" || name == "INFO" ||
               name == "CLIENT" || name == "CLUSTER" || name == "DEBUG" ||
               name == "HELLO") {
      // Meta commands sent by redis-benchmark / redis-cli on connect.
      reply = resp_empty_array();
    } else if (name == "QUIT") {
      reply = resp_simple_string("OK");
      ::send(fd, reply.data(), reply.size(), MSG_NOSIGNAL);
      break;
    } else {
      reply = resp_error("unknown command '" + name + "'");
    }

    // Send response.
    ssize_t total = 0;
    while (total < static_cast<ssize_t>(reply.size())) {
      ssize_t n = ::send(fd, reply.data() + total,
                         reply.size() - total, MSG_NOSIGNAL);
      if (n <= 0) goto done;
      total += n;
    }
  }

done:
  ::close(fd);
}

// ═════════════════════════════════════════════════════════════════
// TCP Server
// ═════════════════════════════════════════════════════════════════

static volatile sig_atomic_t g_running = 1;
static int g_listen_fd = -1;

static void sig_handler(int) {
  g_running = 0;
  // Close the listening socket to unblock accept().
  if (g_listen_fd >= 0) ::close(g_listen_fd);
}

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

int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::info);

  std::vector<std::string> peers;
  int port = 6379;

  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if ((flag == "--config") && i + 1 < argc) {
      raftkv::ServerConfig cfg = raftkv::load_config(argv[++i]);
      peers = cfg.peer_addrs;
    } else if ((flag == "--peers") && i + 1 < argc) {
      peers = split_by_comma(argv[++i]);
    } else if ((flag == "--port") && i + 1 < argc) {
      port = std::stoi(argv[++i]);
    }
  }

  if (peers.empty()) {
    std::cerr << "Usage: raftkv_redis_proxy --config <file> | --peers <addr0,...>\n"
              << "                          [--port 6379]\n";
    return 1;
  }

  // ── Create listening socket ───────────────────────────────────
  int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    spdlog::error("socket(): {}", std::strerror(errno));
    return 1;
  }

  int opt = 1;
  ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port        = htons(static_cast<uint16_t>(port));

  if (::bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr),
             sizeof(addr)) < 0) {
    spdlog::error("bind(): {}", std::strerror(errno));
    ::close(listen_fd);
    return 1;
  }

  if (::listen(listen_fd, 128) < 0) {
    spdlog::error("listen(): {}", std::strerror(errno));
    ::close(listen_fd);
    return 1;
  }

  g_listen_fd = listen_fd;

  ::signal(SIGINT,  sig_handler);
  ::signal(SIGTERM, sig_handler);
  ::signal(SIGPIPE, SIG_IGN);

  spdlog::info("Redis proxy listening on :{}", port);
  spdlog::info("Backend peers: {}", [&]{
    std::string s;
    for (size_t i = 0; i < peers.size(); ++i) {
      if (i) s += ", ";
      s += peers[i];
    }
    return s;
  }());

  // ── Pre-create shared gRPC channels ───────────────────────────
  // All connections share the same gRPC channels to avoid per-connection
  // channel creation overhead (TCP handshake + HTTP/2 negotiation).
  auto shared_clients = raftkv::make_shared_grpc_kv_service_clients(peers);
  spdlog::info("gRPC channels established ({} servers)", shared_clients.size());

  // ── Accept loop ───────────────────────────────────────────────
  while (g_running) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd = ::accept(listen_fd,
                             reinterpret_cast<struct sockaddr*>(&client_addr),
                             &client_len);
    if (client_fd < 0) {
      if (!g_running) break;  // shutting down, listen_fd closed by signal
      if (errno == EINTR) continue;
      spdlog::error("accept(): {}", std::strerror(errno));
      break;
    }

    // Disable Nagle for lower latency.
    int one = 1;
    ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    // Detach thread — connection is self-contained.
    std::thread(handle_connection, client_fd, std::cref(shared_clients)).detach();
  }

  // listen_fd may already be closed by sig_handler; close is harmless if so.
  g_listen_fd = -1;
  ::close(listen_fd);
  spdlog::info("Redis proxy shut down");
  return 0;
}
