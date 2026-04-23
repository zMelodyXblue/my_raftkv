#pragma once

#include <string>

#include "common/kv_engine.h"
#include "common/skiplist.h"

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// SkipListEngine — KvEngine backed by SkipList
// ═════════════════════════════════════════════════════════════════
// O(log n) expected for get/put/append. Ordered iteration.
//
class SkipListEngine : public KvEngine {
 public:
  bool get(const std::string& key, std::string* value) const override {
    return list_.search(key, value);
  }

  void put(const std::string& key, const std::string& value) override {
    list_.insert(key, value);
  }

  void append(const std::string& key, const std::string& value) override {
    std::string existing;
    if (list_.search(key, &existing)) {
      list_.insert(key, existing + value);
    } else {
      list_.insert(key, value);
    }
  }

  void clear() override {
    list_.clear();
  }

  void for_each(
      const std::function<void(const std::string&, const std::string&)>& fn)
      const override {
    list_.for_each(fn);
  }

 private:
  mutable SkipList<std::string, std::string> list_;
};

}  // namespace raftkv
