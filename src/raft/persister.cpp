#include "raft/persister.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>

namespace raftkv {

// ── internal helpers ──────────────────────────────────────────────

static std::string raft_state_path(const std::string& data_dir, int node_id) {
  return data_dir + "/raft_state_" + std::to_string(node_id) + ".dat";
}

static std::string snapshot_path(const std::string& data_dir, int node_id) {
  return data_dir + "/snapshot_" + std::to_string(node_id) + ".dat";
}

// Create `dir` if it does not already exist.
// Only handles the case where the parent directory already exists
// (e.g. data_dir = "./data" or "/tmp/node0").
static void ensure_dir(const std::string& dir) {
  if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
    throw std::runtime_error(
        std::string("Persister: cannot create directory ") + dir +
        ": " + std::strerror(errno));
  }
}

// Write `data` to `path` atomically:
//   1. Write to a temporary file alongside the target.
//   2. Close the file (flushes user-space buffer).
//   3. rename() the temp file over the target.
//
// POSIX guarantees that rename() on the same filesystem is atomic:
// the path atomically switches from the old file to the new one.
// A crash between steps 1 and 3 leaves a stale .tmp file but the
// previously committed data is never corrupted.
static void atomic_write(const std::string& path, const std::string& data) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp.c_str(), std::ios::binary | std::ios::trunc);
    if (!f) {
      throw std::runtime_error("Persister: cannot open " + tmp);
    }
    f.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!f) {
      throw std::runtime_error("Persister: write failed for " + tmp);
    }
  }  // destructor flushes and closes
  if (std::rename(tmp.c_str(), path.c_str()) != 0) {
    throw std::runtime_error(
        std::string("Persister: rename failed for ") + tmp +
        ": " + std::strerror(errno));
  }
}

// Read the entire content of `path`.
// Returns an empty string if the file does not exist.
static std::string read_file(const std::string& path) {
  std::ifstream f(path.c_str(), std::ios::binary);
  if (!f) return std::string();
  return std::string(std::istreambuf_iterator<char>(f),
                     std::istreambuf_iterator<char>());
}

// ── constructor ───────────────────────────────────────────────────

Persister::Persister(int node_id, const std::string& data_dir)
    : node_id_(node_id), data_dir_(data_dir) {
  ensure_dir(data_dir_);
  // Pre-load into memory so subsequent load_*() calls never hit disk.
  raft_state_ = read_file(raft_state_path(data_dir_, node_id_));
  snapshot_   = read_file(snapshot_path(data_dir_, node_id_));
}

// ── save (both files) ─────────────────────────────────────────────

void Persister::save(const std::string& raft_state, const std::string& snapshot) {
  std::lock_guard<std::mutex> lock(mu_);
  raft_state_ = raft_state;
  snapshot_   = snapshot;
  atomic_write(raft_state_path(data_dir_, node_id_), raft_state_);
  atomic_write(snapshot_path(data_dir_, node_id_),   snapshot_);
}

// ── save_raft_state ───────────────────────────────────────────────

void Persister::save_raft_state(const std::string& raft_state) {
  std::lock_guard<std::mutex> lock(mu_);
  raft_state_ = raft_state;
  atomic_write(raft_state_path(data_dir_, node_id_), raft_state_);
}

// ── save_snapshot ─────────────────────────────────────────────────

void Persister::save_snapshot(const std::string& snapshot) {
  std::lock_guard<std::mutex> lock(mu_);
  snapshot_ = snapshot;
  atomic_write(snapshot_path(data_dir_, node_id_), snapshot_);
}

// ── load_raft_state ───────────────────────────────────────────────

std::string Persister::load_raft_state() {
  std::lock_guard<std::mutex> lock(mu_);
  return raft_state_;
}

// ── load_snapshot ─────────────────────────────────────────────────

std::string Persister::load_snapshot() {
  std::lock_guard<std::mutex> lock(mu_);
  return snapshot_;
}

// ── raft_state_size ───────────────────────────────────────────────

int Persister::raft_state_size() {
  std::lock_guard<std::mutex> lock(mu_);
  return static_cast<int>(raft_state_.size());
}

}  // namespace raftkv
