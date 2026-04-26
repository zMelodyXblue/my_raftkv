#include "raft/persister.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <dirent.h>

namespace raftkv {

// ── internal helpers ──────────────────────────────────────────────

static std::string raft_state_path(const std::string& data_dir, int node_id,
                                   int version) {
  return data_dir + "/raft_state_" + std::to_string(node_id) +
         "_v" + std::to_string(version) + ".dat";
}

static std::string snapshot_path(const std::string& data_dir, int node_id,
                                 int version) {
  return data_dir + "/snapshot_" + std::to_string(node_id) +
         "_v" + std::to_string(version) + ".dat";
}

static std::string manifest_path(const std::string& data_dir, int node_id) {
  return data_dir + "/manifest_" + std::to_string(node_id) + ".dat";
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

// Parse manifest content → (raft_version, snap_version).
// Returns false if the file doesn't exist or is malformed.
static bool parse_manifest(const std::string& path,
                           int& raft_ver, int& snap_ver) {
  std::string content = read_file(path);
  if (content.empty()) return false;

  std::istringstream ss(content);
  std::string line;
  int found = 0;
  while (std::getline(ss, line)) {
    if (line.compare(0, 13, "raft_version=") == 0) {
      raft_ver = std::atoi(line.c_str() + 13);
      ++found;
    } else if (line.compare(0, 13, "snap_version=") == 0) {
      snap_ver = std::atoi(line.c_str() + 13);
      ++found;
    }
  }
  return found == 2;
}

// ── destructor ────────────────────────────────────────────────────

Persister::~Persister() = default;

// ── constructor ───────────────────────────────────────────────────

Persister::Persister(int node_id, const std::string& data_dir)
    : node_id_(node_id),
      data_dir_(data_dir),
      raft_version_(0),
      snap_version_(0) {
  ensure_dir(data_dir_);

  // Try to load from manifest.
  int rv = 0, sv = 0;
  if (parse_manifest(manifest_path(data_dir_, node_id_), rv, sv)) {
    raft_version_ = rv;
    snap_version_ = sv;
    raft_state_ = read_file(raft_state_path(data_dir_, node_id_, rv));
    snapshot_   = read_file(snapshot_path(data_dir_, node_id_, sv));
  }

  // GC: remove stale versioned files left by incomplete writes.
  gc_old_files(-1, -1);
}

// ── manifest & GC ─────────────────────────────────────────────────

void Persister::write_manifest() {
  std::string content =
      "raft_version=" + std::to_string(raft_version_) + "\n" +
      "snap_version=" + std::to_string(snap_version_) + "\n";
  atomic_write(manifest_path(data_dir_, node_id_), content);
}

void Persister::gc_old_files(int old_raft_ver, int old_snap_ver) {
  // Fast path: delete specific old version files.
  if (old_raft_ver > 0) {
    std::remove(raft_state_path(data_dir_, node_id_, old_raft_ver).c_str());
  }
  if (old_snap_ver > 0) {
    std::remove(snapshot_path(data_dir_, node_id_, old_snap_ver).c_str());
  }

  // Slow path (startup only, when old versions are unknown):
  // scan directory for stale versioned files belonging to this node.
  if (old_raft_ver < 0 || old_snap_ver < 0) {
    std::string raft_prefix = "raft_state_" + std::to_string(node_id_) + "_v";
    std::string snap_prefix = "snapshot_" + std::to_string(node_id_) + "_v";
    std::string raft_current = raft_state_path(data_dir_, node_id_, raft_version_);
    std::string snap_current = snapshot_path(data_dir_, node_id_, snap_version_);

    DIR* dir = opendir(data_dir_.c_str());
    if (!dir) return;
    struct dirent* ent;
    while ((ent = readdir(dir)) != NULL) {
      std::string name(ent->d_name);
      std::string full = data_dir_ + "/" + name;
      // Clean up stale raft_state files
      if (name.compare(0, raft_prefix.size(), raft_prefix) == 0 &&
          full != raft_current) {
        std::remove(full.c_str());
      }
      // Clean up stale snapshot files
      if (name.compare(0, snap_prefix.size(), snap_prefix) == 0 &&
          full != snap_current) {
        std::remove(full.c_str());
      }
      // Clean up .tmp files
      if (name.size() > 4 &&
          name.compare(name.size() - 4, 4, ".tmp") == 0) {
        std::remove(full.c_str());
      }
    }
    closedir(dir);
  }
}

// ── save (both files) ─────────────────────────────────────────────

void Persister::save(const std::string& raft_state, const std::string& snapshot) {
  std::lock_guard<std::mutex> lock(mu_);
  int old_rv = raft_version_;
  int old_sv = snap_version_;

  raft_state_ = raft_state;
  snapshot_   = snapshot;
  ++raft_version_;
  ++snap_version_;

  atomic_write(raft_state_path(data_dir_, node_id_, raft_version_), raft_state_);
  atomic_write(snapshot_path(data_dir_, node_id_, snap_version_),   snapshot_);
  write_manifest();  // commit point
  gc_old_files(old_rv, old_sv);
}

// ── save_raft_state ───────────────────────────────────────────────

void Persister::save_raft_state(const std::string& raft_state) {
  std::lock_guard<std::mutex> lock(mu_);
  int old_rv = raft_version_;

  raft_state_ = raft_state;
  ++raft_version_;

  atomic_write(raft_state_path(data_dir_, node_id_, raft_version_), raft_state_);
  write_manifest();  // commit point
  gc_old_files(old_rv, 0);
}

// ── save_snapshot ─────────────────────────────────────────────────

void Persister::save_snapshot(const std::string& snapshot) {
  std::lock_guard<std::mutex> lock(mu_);
  int old_sv = snap_version_;

  snapshot_ = snapshot;
  ++snap_version_;

  atomic_write(snapshot_path(data_dir_, node_id_, snap_version_), snapshot_);
  write_manifest();  // commit point
  gc_old_files(0, old_sv);
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
