#pragma once

#include <string>
#include <unordered_map>

#include "common/kv_engine.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// HashMapEngine — KvEngine backed by std::unordered_map
// ═════════════════════════════════════════════════════════════════
// Default engine.  O(1) average for get/put/append.
//
class HashMapEngine : public KvEngine {
 public:
  bool get(const std::string& key, std::string* value) const override {
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    *value = it->second;
    return true;
  }

  void put(const std::string& key, const std::string& value) override {
    map_[key] = value;
  }

  void append(const std::string& key, const std::string& value) override {
    map_[key] += value;
  }

  void clear() override {
    map_.clear();
  }

  void for_each(
      const std::function<void(const std::string&, const std::string&)>& fn)
      const override {
    for (const auto& kv : map_) {
      fn(kv.first, kv.second);
    }
  }

 private:
  std::unordered_map<std::string, std::string> map_;
};

}  // namespace raftkv
