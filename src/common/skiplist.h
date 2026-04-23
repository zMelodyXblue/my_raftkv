#pragma once

#include <cstdlib>
#include <ctime>
#include <functional>
#include <string>

namespace raftkv {

// ═════════════════════════════════════════════════════════════════
// SkipList<K, V> — Probabilistic sorted data structure
// ═════════════════════════════════════════════════════════════════
// Expected O(log n) for insert / search / remove.
// NOT thread-safe — callers must hold their own mutex.
//
// Usage:
//   SkipList<std::string, std::string> list;
//   list.insert("key", "value");
//   std::string val;
//   if (list.search("key", &val)) { ... }
//   list.remove("key");
//
template <typename K, typename V>
class SkipList {
 public:
  static const int kMaxLevel = 16;

  // ── Node ──────────────────────────────────────────────────────
  struct Node {
    K key;
    V value;
    // forward[i] points to the next node at level i.
    // Array size = node_level + 1 (levels 0..node_level).
    Node** forward;
    int node_level;

    Node(const K& k, const V& v, int level)
        : key(k), value(v), node_level(level) {
      forward = new Node*[level + 1];
      for (int i = 0; i <= level; ++i) {
        forward[i] = nullptr;
      }
    }

    // Sentinel node (header) — key/value are default-constructed.
    explicit Node(int level) : node_level(level) {
      forward = new Node*[level + 1];
      for (int i = 0; i <= level; ++i) {
        forward[i] = nullptr;
      }
    }

    ~Node() { delete[] forward; }
  };

  // ── Constructor / Destructor ──────────────────────────────────

  SkipList() : current_level_(0), size_(0) {
    // Seed random generator once.
    static bool seeded = false;
    if (!seeded) {
      std::srand(static_cast<unsigned>(std::time(nullptr)));
      seeded = true;
    }
    header_ = new Node(kMaxLevel);
  }

  ~SkipList() {
    Node* current = header_->forward[0];
    while (current != nullptr) {
      Node* next = current->forward[0];
      delete current;
      current = next;
    }
    delete header_;
  }

  // Non-copyable.
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // ── Core Operations ───────────────────────────────────────────

  // Insert or overwrite a key-value pair.
  // If the key already exists, update its value.
  void insert(const K& key, const V& value) {
    Node* update[kMaxLevel + 1];
    Node* current = header_;

    for (int i = current_level_; i >= 0; --i) {
      while (current->forward[i] != nullptr &&
             current->forward[i]->key < key) {
        current = current->forward[i];
      }
      update[i] = current;
    }

    Node* target = current->forward[0];
    if (target != nullptr && target->key == key) {
      target->value = value;
      return;
    }

    int new_level = random_level();
    if (new_level > current_level_) {
      for (int i = current_level_ + 1; i <= new_level; ++i) {
        update[i] = header_;
      }
      current_level_ = new_level;
    }

    Node* new_node = new Node(key, value, new_level);
    for (int i = 0; i <= new_level; ++i) {
      new_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = new_node;
    }
    ++size_;
  }

  // Search for a key. Returns true if found, writes value via pointer.
  bool search(const K& key, V* value) const {
    Node* current = header_;
    for (int i = current_level_; i >= 0; --i) {
      while (current->forward[i] != nullptr &&
             current->forward[i]->key < key) {
        current = current->forward[i];
      }
    }
    Node* target = current->forward[0];
    if (target != nullptr && target->key == key) {
      if (value != nullptr) {
        *value = target->value;
      }
      return true;
    }
    return false;
  }

  // Remove a key. Returns true if the key was found and removed.
  bool remove(const K& key) {
    Node* update[kMaxLevel + 1];
    Node* current = header_;

    for (int i = current_level_; i >= 0; --i) {
      while (current->forward[i] != nullptr &&
             current->forward[i]->key < key) {
        current = current->forward[i];
      }
      update[i] = current;
    }

    Node* target = current->forward[0];
    if (target == nullptr || target->key != key) {
      return false;
    }

    for (int i = 0; i <= current_level_; ++i) {
      if (update[i]->forward[i] != target) break;
      update[i]->forward[i] = target->forward[i];
    }
    delete target;

    while (current_level_ > 0 &&
           header_->forward[current_level_] == nullptr) {
      --current_level_;
    }
    --size_;
    return true;
  }

  // Iterate all key-value pairs in ascending key order.
  void for_each(
      const std::function<void(const K&, const V&)>& fn) const {
    Node* current = header_->forward[0];
    while (current != nullptr) {
      fn(current->key, current->value);
      current = current->forward[0];
    }
  }

  // ── Utility ───────────────────────────────────────────────────

  // Remove all entries. After clear(), size() == 0.
  void clear() {
    Node* current = header_->forward[0];
    while (current != nullptr) {
      Node* next = current->forward[0];
      delete current;
      current = next;
    }
    for (int i = 0; i <= current_level_; ++i) {
      header_->forward[i] = nullptr;
    }
    current_level_ = 0;
    size_ = 0;
  }

  int size() const { return size_; }

 protected:
  // Generate a random level for a new node.
  // Each level has 1/2 probability of being promoted to the next.
  // Returns a value in [0, kMaxLevel].
  int random_level() const {
    int level = 0;
    while (level < kMaxLevel && (std::rand() % 2 == 0)) {
      ++level;
    }
    return level;
  }

  Node* header_;       // Sentinel head node (not a real entry)
  int current_level_;  // Current highest level in use (0-based)
  int size_;           // Number of entries
};

}  // namespace raftkv
