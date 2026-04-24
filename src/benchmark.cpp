#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include "client/kv_client.h"
#include "common/config_loader.h"

// ── Command-line options ────────────────────────────────────────
struct BenchOptions {
  std::vector<std::string> peers;
  int clients    = 4;
  int requests   = 10000;
  int value_size = 256;
  double read_ratio = 0.5;
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

static void usage() {
  std::cerr
    << "Usage:\n"
    << "  raftkv_bench --config <file> | --peers <addr0,addr1,...>\n"
    << "               [--clients 4]\n"
    << "               [--requests 10000]\n"
    << "               [--value-size 256]\n"
    << "               [--read-ratio 0.5]\n";
}

static bool parse_args(int argc, char* argv[], BenchOptions& opts) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "--config") && i + 1 < argc) {
      raftkv::ServerConfig cfg = raftkv::load_config(argv[++i]);
      opts.peers = cfg.peer_addrs;
    } else if ((arg == "--peers") && i + 1 < argc) {
      opts.peers = split_by_comma(argv[++i]);
    } else if (arg == "--clients" && i + 1 < argc) {
      opts.clients = std::stoi(argv[++i]);
    } else if (arg == "--requests" && i + 1 < argc) {
      opts.requests = std::stoi(argv[++i]);
    } else if (arg == "--value-size" && i + 1 < argc) {
      opts.value_size = std::stoi(argv[++i]);
    } else if (arg == "--read-ratio" && i + 1 < argc) {
      opts.read_ratio = std::stod(argv[++i]);
    } else {
      return false;
    }
  }
  return !opts.peers.empty();
}

// ── Per-thread result ───────────────────────────────────────────
struct ThreadResult {
  std::vector<double> latencies_us;  // microseconds per operation
  int errors = 0;
};

// ── Worker ──────────────────────────────────────────────────────
static void worker(const BenchOptions& opts,
                   int thread_id,
                   int ops_count,
                   ThreadResult& result) {
  raftkv::KvClient client(opts.peers);

  // Pre-generate a fixed value string
  std::string value(opts.value_size, 'x');

  std::mt19937 rng(thread_id * 1000 + 42);
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::uniform_int_distribution<int> key_dist(0, opts.requests - 1);

  result.latencies_us.reserve(ops_count);

  for (int i = 0; i < ops_count; ++i) {
    std::string key = "bench_key_" + std::to_string(key_dist(rng));
    bool is_read = (coin(rng) < opts.read_ratio);

    auto t0 = std::chrono::steady_clock::now();
    try {
      if (is_read) {
        client.get(key);
      } else {
        client.put(key, value);
      }
      auto t1 = std::chrono::steady_clock::now();
      double us = std::chrono::duration_cast<std::chrono::microseconds>(
                      t1 - t0).count();
      result.latencies_us.push_back(us);
    } catch (const std::exception& e) {
      ++result.errors;
    }
  }
}

// ── Percentile helper ───────────────────────────────────────────
static double percentile(std::vector<double>& sorted, double p) {
  if (sorted.empty()) return 0.0;
  size_t idx = static_cast<size_t>(p * sorted.size());
  if (idx >= sorted.size()) idx = sorted.size() - 1;
  return sorted[idx];
}

// ── Main ────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::warn);
  //spdlog::set_level(spdlog::level::debug);

  BenchOptions opts;
  if (!parse_args(argc, argv, opts)) {
    usage();
    return 1;
  }

  int ops_per_thread = opts.requests / opts.clients;
  int remainder = opts.requests % opts.clients;

  std::cout << "=== raftkv_bench ===" << std::endl;
  std::cout << "Peers:       ";
  for (size_t i = 0; i < opts.peers.size(); ++i) {
    if (i > 0) std::cout << ", ";
    std::cout << opts.peers[i];
  }
  std::cout << std::endl;
  std::cout << "Clients:     " << opts.clients << std::endl;
  std::cout << "Requests:    " << opts.requests << std::endl;
  std::cout << "Value size:  " << opts.value_size << " bytes" << std::endl;
  std::cout << "Read ratio:  " << opts.read_ratio << std::endl;
  std::cout << std::endl;

  std::vector<ThreadResult> results(opts.clients);
  std::vector<std::thread> threads;

  auto wall_start = std::chrono::steady_clock::now();

  for (int t = 0; t < opts.clients; ++t) {
    int n = ops_per_thread + (t < remainder ? 1 : 0);
    threads.emplace_back(worker, std::cref(opts), t, n, std::ref(results[t]));
  }

  for (auto& th : threads) {
    th.join();
  }

  auto wall_end = std::chrono::steady_clock::now();
  double wall_sec = std::chrono::duration_cast<std::chrono::microseconds>(
                        wall_end - wall_start).count() / 1e6;

  // Merge all latencies
  std::vector<double> all_latencies;
  int total_errors = 0;
  for (auto& r : results) {
    all_latencies.insert(all_latencies.end(),
                         r.latencies_us.begin(), r.latencies_us.end());
    total_errors += r.errors;
  }
  std::sort(all_latencies.begin(), all_latencies.end());

  int total_ok = static_cast<int>(all_latencies.size());
  double qps = (wall_sec > 0) ? total_ok / wall_sec : 0;

  std::cout << "--- Results ---" << std::endl;
  std::cout << "Completed:   " << total_ok << " ops" << std::endl;
  std::cout << "Errors:      " << total_errors << std::endl;
  std::cout << "Wall time:   " << wall_sec << " s" << std::endl;
  std::cout << "QPS:         " << static_cast<int>(qps) << std::endl;
  std::cout << std::endl;

  if (!all_latencies.empty()) {
    std::cout << "--- Latency (us) ---" << std::endl;
    std::cout << "  P50:  " << percentile(all_latencies, 0.50) << std::endl;
    std::cout << "  P95:  " << percentile(all_latencies, 0.95) << std::endl;
    std::cout << "  P99:  " << percentile(all_latencies, 0.99) << std::endl;
    std::cout << "  Max:  " << all_latencies.back() << std::endl;
  }

  return 0;
}
