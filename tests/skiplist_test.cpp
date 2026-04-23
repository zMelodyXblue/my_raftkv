#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "common/skiplist.h"

namespace raftkv {
namespace {

// ── Basic Operations ─────────────────────────────────────────────

TEST(SkipListTest, InsertAndSearch) {
  SkipList<std::string, std::string> list;
  list.insert("hello", "world");

  std::string val;
  EXPECT_TRUE(list.search("hello", &val));
  EXPECT_EQ("world", val);
  EXPECT_EQ(1, list.size());
}

TEST(SkipListTest, SearchNotFound) {
  SkipList<std::string, std::string> list;
  list.insert("a", "1");

  std::string val;
  EXPECT_FALSE(list.search("b", &val));
}

TEST(SkipListTest, OverwriteExisting) {
  SkipList<std::string, std::string> list;
  list.insert("key", "old");
  list.insert("key", "new");

  std::string val;
  EXPECT_TRUE(list.search("key", &val));
  EXPECT_EQ("new", val);
  EXPECT_EQ(1, list.size());
}

// ── Remove ───────────────────────────────────────────────────────

TEST(SkipListTest, Remove) {
  SkipList<std::string, std::string> list;
  list.insert("x", "1");
  list.insert("y", "2");
  list.insert("z", "3");

  EXPECT_TRUE(list.remove("y"));
  EXPECT_EQ(2, list.size());

  std::string val;
  EXPECT_FALSE(list.search("y", &val));
  EXPECT_TRUE(list.search("x", &val));
  EXPECT_EQ("1", val);
  EXPECT_TRUE(list.search("z", &val));
  EXPECT_EQ("3", val);
}

TEST(SkipListTest, RemoveNonExistent) {
  SkipList<std::string, std::string> list;
  list.insert("a", "1");

  EXPECT_FALSE(list.remove("not_here"));
  EXPECT_EQ(1, list.size());
}

// ── ForEach (ordered iteration) ──────────────────────────────────

TEST(SkipListTest, ForEachOrdered) {
  SkipList<std::string, std::string> list;
  list.insert("cherry", "3");
  list.insert("apple", "1");
  list.insert("banana", "2");
  list.insert("date", "4");

  std::vector<std::string> keys;
  list.for_each([&keys](const std::string& k, const std::string&) {
    keys.push_back(k);
  });

  ASSERT_EQ(4u, keys.size());
  // SkipList is sorted — keys should be in ascending order.
  EXPECT_EQ("apple", keys[0]);
  EXPECT_EQ("banana", keys[1]);
  EXPECT_EQ("cherry", keys[2]);
  EXPECT_EQ("date", keys[3]);
}

// ── Clear ────────────────────────────────────────────────────────

TEST(SkipListTest, Clear) {
  SkipList<std::string, std::string> list;
  for (int i = 0; i < 20; ++i) {
    list.insert("k" + std::to_string(i), "v" + std::to_string(i));
  }
  EXPECT_EQ(20, list.size());

  list.clear();
  EXPECT_EQ(0, list.size());

  std::string val;
  EXPECT_FALSE(list.search("k0", &val));
  EXPECT_FALSE(list.search("k19", &val));

  // Can insert again after clear.
  list.insert("new", "entry");
  EXPECT_TRUE(list.search("new", &val));
  EXPECT_EQ("entry", val);
}

// ── Stress Test ──────────────────────────────────────────────────

TEST(SkipListTest, StressRandomOps) {
  SkipList<int, int> list;
  std::map<int, int> reference;  // Ground truth

  std::srand(42);  // Deterministic seed for reproducibility
  const int ops = 1000;

  for (int i = 0; i < ops; ++i) {
    int op = std::rand() % 3;
    int key = std::rand() % 200;
    int val = std::rand() % 10000;

    if (op == 0) {
      // Insert
      list.insert(key, val);
      reference[key] = val;
    } else if (op == 1) {
      // Search
      int found_val = -1;
      bool found = list.search(key, &found_val);
      auto it = reference.find(key);
      if (it != reference.end()) {
        EXPECT_TRUE(found) << "key=" << key << " should exist";
        EXPECT_EQ(it->second, found_val) << "key=" << key;
      } else {
        EXPECT_FALSE(found) << "key=" << key << " should not exist";
      }
    } else {
      // Remove
      bool removed = list.remove(key);
      bool existed = reference.count(key) > 0;
      EXPECT_EQ(existed, removed) << "key=" << key;
      reference.erase(key);
    }
  }

  // Final size check.
  EXPECT_EQ(static_cast<int>(reference.size()), list.size());
}

}  // namespace
}  // namespace raftkv
