#pragma once

#include <functional>
#include <string>

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// KvEngine — Abstract storage engine interface
// ═════════════════════════════════════════════════════════════════
// KvStore operates on data through this interface, making the
// underlying storage pluggable (HashMap, SkipList, RocksDB, etc.).
//
// Thread safety: callers (KvStore) hold their own mutex.
// Implementations do NOT need internal locking.
//
class KvEngine {
 public:
  virtual ~KvEngine() = default;

  // Retrieve the value for `key`.  Returns true if found.
  virtual bool get(const std::string& key, std::string* value) const = 0;

  // Insert or overwrite.
  virtual void put(const std::string& key, const std::string& value) = 0;

  // Append `value` to the existing value (or create if absent).
  virtual void append(const std::string& key, const std::string& value) = 0;

  // Remove all entries.
  virtual void clear() = 0;

  // Iterate over all key-value pairs (for snapshot serialization).
  virtual void for_each(
      const std::function<void(const std::string&, const std::string&)>& fn)
      const = 0;
};

}  // namespace raftkv
