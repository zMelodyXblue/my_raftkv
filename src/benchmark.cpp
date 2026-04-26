#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include "client/kv_client.h"
#include "common/config_loader.h"
#include "rpc/grpc/grpc_kv_service_client.h"

// ── Command-line options ────────────────────────────────────────
struct BenchOptions {
  std::vector<std::string> peers;
  int clients       = 4;
  int requests      = 10000;
  int value_size    = 256;
  double read_ratio = 0.5;
  int key_space     = 0;       // 0 = same as requests
  int warmup        = 100;     // warm-up ops per client before measurement
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
    << "               [--read-ratio 0.5]\n"
    << "               [--key-space 1000]     (default: same as --requests)\n"
    << "               [--warmup 100]         (warm-up ops per client)\n";
}

static bool parse_args(int argc, char* argv[], BenchOptions& opts) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "--config") && i + 1 < argc) {
      try {
        raftkv::ServerConfig cfg = raftkv::load_config(argv[++i]);
        opts.peers = cfg.peer_addrs;
      } catch (const std::exception& e) {
        std::cerr << "Error loading config: " << e.what() << "\n";
        return false;
      }
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
    } else if (arg == "--key-space" && i + 1 < argc) {
      opts.key_space = std::stoi(argv[++i]);
    } else if (arg == "--warmup" && i + 1 < argc) {
      opts.warmup = std::stoi(argv[++i]);
    } else {
      return false;
    }
  }

  // Default key_space to requests if not specified.
  if (opts.key_space <= 0) opts.key_space = opts.requests;

  return !opts.peers.empty();
}

static bool validate_opts(const BenchOptions& opts) {
  bool ok = true;
  if (opts.clients <= 0) {
    std::cerr << "Error: --clients must be > 0\n"; ok = false;
  }
  if (opts.requests <= 0) {
    std::cerr << "Error: --requests must be > 0\n"; ok = false;
  }
  if (opts.value_size < 0) {
    std::cerr << "Error: --value-size must be >= 0\n"; ok = false;
  }
  if (opts.read_ratio < 0.0 || opts.read_ratio > 1.0) {
    std::cerr << "Error: --read-ratio must be in [0.0, 1.0]\n"; ok = false;
  }
  if (opts.warmup < 0) {
    std::cerr << "Error: --warmup must be >= 0\n"; ok = false;
  }
  return ok;
}

// ── Per-thread result ───────────────────────────────────────────
struct ThreadResult {
  std::vector<double> read_latencies_us;
  std::vector<double> write_latencies_us;
  int read_errors  = 0;
  int write_errors = 0;
  std::string first_error;  // first error message for diagnosis
};

// ── Worker ──────────────────────────────────────────────────────
static void worker(const BenchOptions& opts,
                   int thread_id,
                   int ops_count,
                   ThreadResult& result) {
  raftkv::ClientOptions copts;
  copts.max_retries      = 100;
  copts.total_timeout_ms = 60000;
  raftkv::KvClient client(raftkv::make_grpc_kv_service_clients(opts.peers), copts);

  std::string value(opts.value_size, 'x');

  std::mt19937 rng(thread_id * 1000 + 42);
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::uniform_int_distribution<int> key_dist(0, opts.key_space - 1);

  // ── Warm-up phase (not measured) ───────────────────────────────
  for (int i = 0; i < opts.warmup; ++i) {
    std::string key = "bench_key_" + std::to_string(key_dist(rng));
    try {
      if (coin(rng) < opts.read_ratio) {
        client.get(key);
      } else {
        client.put(key, value);
      }
    } catch (...) {
      // warm-up errors are ignored
    }
  }

  // ── Measurement phase ──────────────────────────────────────────
  result.read_latencies_us.reserve(ops_count);
  result.write_latencies_us.reserve(ops_count);

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
      if (is_read) {
        result.read_latencies_us.push_back(us);
      } else {
        result.write_latencies_us.push_back(us);
      }
    } catch (const std::exception& e) {
      if (is_read) {
        ++result.read_errors;
      } else {
        ++result.write_errors;
      }
      if (result.first_error.empty()) {
        result.first_error = e.what();
      }
    }
  }
}

// ── Percentile helper ───────────────────────────────────────────
static double percentile(const std::vector<double>& sorted, double p) {
  if (sorted.empty()) return 0.0;
  double idx = p * (sorted.size() - 1);
  size_t lo = static_cast<size_t>(idx);
  size_t hi = lo + 1;
  if (hi >= sorted.size()) return sorted.back();
  double frac = idx - lo;
  return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
}

static double average(const std::vector<double>& v) {
  if (v.empty()) return 0.0;
  return std::accumulate(v.begin(), v.end(), 0.0) / v.size();
}

// ── Print latency stats for a category ──────────────────────────
static void print_latency_stats(const char* label,
                                std::vector<double>& latencies,
                                int errors) {
  std::sort(latencies.begin(), latencies.end());
  int ok = static_cast<int>(latencies.size());
  int total = ok + errors;

  std::cout << "  " << label << ":" << std::endl;
  std::cout << "    Count:   " << ok << " ok / " << errors << " err";
  if (total > 0) {
    double rate = 100.0 * ok / total;
    std::cout << "  (" << static_cast<int>(rate) << "% success)";
  }
  std::cout << std::endl;

  if (!latencies.empty()) {
    std::cout << "    Avg:     " << static_cast<int>(average(latencies)) << " us" << std::endl;
    std::cout << "    P50:     " << static_cast<int>(percentile(latencies, 0.50)) << " us" << std::endl;
    std::cout << "    P95:     " << static_cast<int>(percentile(latencies, 0.95)) << " us" << std::endl;
    std::cout << "    P99:     " << static_cast<int>(percentile(latencies, 0.99)) << " us" << std::endl;
    std::cout << "    Max:     " << static_cast<int>(latencies.back()) << " us" << std::endl;
  }
}

// ── Main ────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
  spdlog::set_level(spdlog::level::warn);

  BenchOptions opts;
  if (!parse_args(argc, argv, opts)) {
    usage();
    return 1;
  }
  if (!validate_opts(opts)) {
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
  std::cout << "Key space:   " << opts.key_space << std::endl;
  std::cout << "Warm-up:     " << opts.warmup << " ops/client" << std::endl;
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

  // Merge results
  std::vector<double> all_latencies;
  std::vector<double> read_latencies;
  std::vector<double> write_latencies;
  int read_errors = 0, write_errors = 0;
  std::string first_error;

  for (auto& r : results) {
    all_latencies.insert(all_latencies.end(),
                         r.read_latencies_us.begin(), r.read_latencies_us.end());
    all_latencies.insert(all_latencies.end(),
                         r.write_latencies_us.begin(), r.write_latencies_us.end());
    read_latencies.insert(read_latencies.end(),
                          r.read_latencies_us.begin(), r.read_latencies_us.end());
    write_latencies.insert(write_latencies.end(),
                           r.write_latencies_us.begin(), r.write_latencies_us.end());
    read_errors += r.read_errors;
    write_errors += r.write_errors;
    if (first_error.empty() && !r.first_error.empty()) {
      first_error = r.first_error;
    }
  }

  std::sort(all_latencies.begin(), all_latencies.end());

  int total_ok = static_cast<int>(all_latencies.size());
  int total_errors = read_errors + write_errors;
  double qps = (wall_sec > 0) ? total_ok / wall_sec : 0;

  // ── Summary ────────────────────────────────────────────────────
  std::cout << "--- Summary ---" << std::endl;
  std::cout << "Completed:   " << total_ok << " ops" << std::endl;
  std::cout << "Errors:      " << total_errors
            << " (read=" << read_errors << " write=" << write_errors << ")" << std::endl;
  if (!first_error.empty()) {
    std::cout << "First error: " << first_error << std::endl;
  }
  std::cout << "Wall time:   " << wall_sec << " s" << std::endl;
  std::cout << "QPS:         " << static_cast<int>(qps) << std::endl;
  if (total_ok + total_errors > 0) {
    double rate = 100.0 * total_ok / (total_ok + total_errors);
    std::cout << "Success:     " << static_cast<int>(rate) << "%" << std::endl;
  }
  std::cout << std::endl;

  // ── Per-category stats ─────────────────────────────────────────
  if (!all_latencies.empty()) {
    std::cout << "--- Latency ---" << std::endl;
    print_latency_stats("Total", all_latencies, total_errors);
    if (!read_latencies.empty() || read_errors > 0) {
      print_latency_stats("Read", read_latencies, read_errors);
    }
    if (!write_latencies.empty() || write_errors > 0) {
      print_latency_stats("Write", write_latencies, write_errors);
    }
  }

  return 0;
}
