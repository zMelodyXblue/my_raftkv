#include "raft/persister.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "proto/raft.pb.h"

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
      snap_version_(0),
      wal_start_index_(0),
      wal_file_size_(0) {
  ensure_dir(data_dir_);

  // Try to load from manifest.
  int rv = 0, sv = 0;
  if (parse_manifest(manifest_path(data_dir_, node_id_), rv, sv)) {
    raft_version_ = rv;
    snap_version_ = sv;
    raft_state_ = read_file(raft_state_path(data_dir_, node_id_, rv));
    snapshot_   = read_file(snapshot_path(data_dir_, node_id_, sv));
  }

  // Recover WAL state (meta file + WAL log).
  recover_wal();

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
  return static_cast<int>(wal_file_size_);
}

// ── WAL helpers ───────────────────────────────────────────────────

std::string Persister::meta_path() const {
  return data_dir_ + "/raft_meta_" + std::to_string(node_id_) + ".dat";
}

std::string Persister::wal_path() const {
  return data_dir_ + "/raft_wal_" + std::to_string(node_id_) + ".log";
}

// Encode a single LogEntry into WAL record format:
//   [4 bytes little-endian length][protobuf bytes]
static std::string encode_wal_record(const LogEntry& entry) {
  raft::LogEntry pe;
  pe.set_command(entry.command);
  pe.set_term(entry.term);
  pe.set_index(entry.index);
  std::string payload;
  pe.SerializeToString(&payload);

  uint32_t len = static_cast<uint32_t>(payload.size());
  std::string record(4 + payload.size(), '\0');
  record[0] = static_cast<char>(len & 0xFF);
  record[1] = static_cast<char>((len >> 8) & 0xFF);
  record[2] = static_cast<char>((len >> 16) & 0xFF);
  record[3] = static_cast<char>((len >> 24) & 0xFF);
  std::memcpy(&record[4], payload.data(), payload.size());
  return record;
}

// Decode a LogEntry from protobuf bytes.
static LogEntry decode_log_entry(const std::string& payload) {
  raft::LogEntry pe;
  pe.ParseFromString(payload);
  LogEntry e;
  e.command = pe.command();
  e.term = pe.term();
  e.index = pe.index();
  return e;
}

// Read WAL file and populate wal_offsets_, wal_start_index_, wal_file_size_.
// Returns the loaded entries. Truncates incomplete trailing records.
void Persister::recover_wal() {
  wal_offsets_.clear();
  wal_start_index_ = 0;
  wal_file_size_ = 0;

  std::string path = wal_path();
  std::ifstream f(path.c_str(), std::ios::binary);
  if (!f) return;

  // Get file size
  f.seekg(0, std::ios::end);
  int64_t file_size = static_cast<int64_t>(f.tellg());
  f.seekg(0, std::ios::beg);

  int64_t offset = 0;
  bool first = true;
  while (offset + 4 <= file_size) {
    // Read length header
    char len_buf[4];
    f.read(len_buf, 4);
    if (!f) break;

    uint32_t len = static_cast<uint32_t>(
        static_cast<unsigned char>(len_buf[0]) |
        (static_cast<unsigned char>(len_buf[1]) << 8) |
        (static_cast<unsigned char>(len_buf[2]) << 16) |
        (static_cast<unsigned char>(len_buf[3]) << 24));

    // Check if complete record exists
    if (offset + 4 + static_cast<int64_t>(len) > file_size) {
      break;  // Incomplete record — truncate
    }

    std::string payload(len, '\0');
    f.read(&payload[0], len);
    if (!f) break;

    if (first) {
      LogEntry e = decode_log_entry(payload);
      wal_start_index_ = e.index;
      first = false;
    }

    wal_offsets_.push_back(offset);
    offset += 4 + static_cast<int64_t>(len);
  }

  wal_file_size_ = offset;

  // Truncate file if there were incomplete trailing records
  if (offset < file_size) {
    f.close();
    ::truncate(path.c_str(), static_cast<off_t>(offset));
  }
}

// Rewrite WAL file with exactly these entries (used by truncate_prefix).
void Persister::rewrite_wal(const std::vector<LogEntry>& entries) {
  std::string path = wal_path();
  wal_offsets_.clear();
  wal_file_size_ = 0;

  if (entries.empty()) {
    wal_start_index_ = 0;
    // Truncate the file to zero
    std::ofstream f(path.c_str(), std::ios::binary | std::ios::trunc);
    return;
  }

  wal_start_index_ = entries[0].index;

  std::string all_data;
  for (const LogEntry& e : entries) {
    wal_offsets_.push_back(static_cast<int64_t>(all_data.size()));
    all_data += encode_wal_record(e);
  }
  wal_file_size_ = static_cast<int64_t>(all_data.size());

  atomic_write(path, all_data);
}

// Read WAL entries without locking. Requires mu_ held.
std::vector<LogEntry> Persister::load_logs_internal() {
  std::vector<LogEntry> result;
  std::string path = wal_path();
  std::ifstream f(path.c_str(), std::ios::binary);
  if (!f) return result;

  int64_t remaining = wal_file_size_;
  while (remaining >= 4) {
    char len_buf[4];
    f.read(len_buf, 4);
    if (!f) break;

    uint32_t len = static_cast<uint32_t>(
        static_cast<unsigned char>(len_buf[0]) |
        (static_cast<unsigned char>(len_buf[1]) << 8) |
        (static_cast<unsigned char>(len_buf[2]) << 16) |
        (static_cast<unsigned char>(len_buf[3]) << 24));

    if (static_cast<int64_t>(4 + len) > remaining) break;

    std::string payload(len, '\0');
    f.read(&payload[0], len);
    if (!f) break;

    result.push_back(decode_log_entry(payload));
    remaining -= (4 + static_cast<int64_t>(len));
  }
  return result;
}

// ── WAL interface implementation ──────────────────────────────────

void Persister::save_meta(int term, int voted_for,
                          int last_snapshot_index, int last_snapshot_term) {
  std::lock_guard<std::mutex> lock(mu_);
  std::string content =
      "term=" + std::to_string(term) + "\n" +
      "voted_for=" + std::to_string(voted_for) + "\n" +
      "last_snapshot_index=" + std::to_string(last_snapshot_index) + "\n" +
      "last_snapshot_term=" + std::to_string(last_snapshot_term) + "\n";
  atomic_write(meta_path(), content);
}

bool Persister::load_meta(int* term, int* voted_for,
                          int* last_snapshot_index, int* last_snapshot_term) {
  std::lock_guard<std::mutex> lock(mu_);
  std::string content = read_file(meta_path());
  if (content.empty()) return false;

  std::istringstream ss(content);
  std::string line;
  int found = 0;
  while (std::getline(ss, line)) {
    if (line.compare(0, 5, "term=") == 0) {
      *term = std::atoi(line.c_str() + 5);
      ++found;
    } else if (line.compare(0, 10, "voted_for=") == 0) {
      *voted_for = std::atoi(line.c_str() + 10);
      ++found;
    } else if (line.compare(0, 20, "last_snapshot_index=") == 0) {
      *last_snapshot_index = std::atoi(line.c_str() + 20);
      ++found;
    } else if (line.compare(0, 19, "last_snapshot_term=") == 0) {
      *last_snapshot_term = std::atoi(line.c_str() + 19);
      ++found;
    }
  }
  return found == 4;
}

void Persister::append_logs(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;
  std::lock_guard<std::mutex> lock(mu_);

  std::string path = wal_path();

  // Build combined data for all entries, tracking offsets
  std::string data;
  if (wal_offsets_.empty()) {
    wal_start_index_ = entries[0].index;
  }
  for (const LogEntry& e : entries) {
    wal_offsets_.push_back(wal_file_size_ + static_cast<int64_t>(data.size()));
    data += encode_wal_record(e);
  }

  // Append to WAL file
  std::ofstream f(path.c_str(), std::ios::binary | std::ios::app);
  if (!f) {
    throw std::runtime_error("Persister: cannot open WAL file " + path);
  }
  f.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!f) {
    throw std::runtime_error("Persister: write failed for WAL file " + path);
  }

  wal_file_size_ += static_cast<int64_t>(data.size());
}

void Persister::truncate_suffix(int from_index) {
  std::lock_guard<std::mutex> lock(mu_);

  if (wal_offsets_.empty() || from_index < wal_start_index_) {
    // Truncate everything
    rewrite_wal({});
    return;
  }

  int pos = from_index - wal_start_index_;
  if (pos >= static_cast<int>(wal_offsets_.size())) {
    return;  // Nothing to truncate
  }

  int64_t new_size = wal_offsets_[pos];
  wal_offsets_.resize(pos);
  wal_file_size_ = new_size;

  std::string path = wal_path();
  ::truncate(path.c_str(), static_cast<off_t>(new_size));

  if (wal_offsets_.empty()) {
    wal_start_index_ = 0;
  }
}

void Persister::truncate_prefix(int to_index) {
  std::lock_guard<std::mutex> lock(mu_);

  if (wal_offsets_.empty()) return;

  // to_index: remove entries with index < to_index
  // Keep entries with index >= to_index
  int keep_from = to_index - wal_start_index_;
  if (keep_from <= 0) return;  // Nothing to remove

  if (keep_from >= static_cast<int>(wal_offsets_.size())) {
    // Remove all entries
    rewrite_wal({});
    return;
  }

  // Read the entries we want to keep, then rewrite
  std::vector<LogEntry> kept = load_logs_internal();
  std::vector<LogEntry> remaining(kept.begin() + keep_from, kept.end());
  rewrite_wal(remaining);
}

std::vector<LogEntry> Persister::load_logs() {
  std::lock_guard<std::mutex> lock(mu_);
  return load_logs_internal();
}

int Persister::wal_size() {
  std::lock_guard<std::mutex> lock(mu_);
  return static_cast<int>(wal_file_size_);
}

}  // namespace raftkv
