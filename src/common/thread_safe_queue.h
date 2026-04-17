#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>

namespace raftkv {

// Thread-safe blocking queue for inter-module communication.
// Used for:
//   - Raft → KvStore apply channel (ThreadSafeQueue<ApplyMsg>)
//   - Per-index wait channels in KvStore (ThreadSafeQueue<ApplyResult>)
//
// Shutdown protocol
// -----------------
// Background threads that block on pop() must be unblocked cleanly
// during destruction.  The pattern is:
//
//   // In destructor / shutdown():
//   queue.close();       // unblocks all waiters
//   thread.join();       // safe to join now
//
// After close(), push() is a no-op and pop() / try_pop() return
// std::nullopt so callers can detect the shutdown and exit their loops.
template <typename T>
class ThreadSafeQueue {
 public:
  // Push an item.  No-op if the queue is already closed.
  void push(T item) {
    {
      std::lock_guard lock(mu_);
      if (closed_) return;
      queue_.push(std::move(item));
    }
    cv_.notify_one();
  }

  // Blocking pop.
  // Returns the next item, or std::nullopt if the queue is closed
  // (and empty).  Callers should treat nullopt as a shutdown signal:
  //
  //   while (auto msg = apply_channel->pop()) {
  //     handle(*msg);
  //   }
  //   // queue closed, exit loop
  std::optional<T> pop() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !queue_.empty() || closed_; });
    if (queue_.empty()) return std::nullopt;  // closed
    T item = std::move(queue_.front());
    queue_.pop();
    return item;
  }

  // Non-blocking pop with timeout.
  // Returns std::nullopt on timeout OR if the queue is closed.
  std::optional<T> try_pop(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    if (!cv_.wait_for(lock, timeout,
                      [this] { return !queue_.empty() || closed_; })) {
      return std::nullopt;  // timed out
    }
    if (queue_.empty()) return std::nullopt;  // closed
    T item = std::move(queue_.front());
    queue_.pop();
    return item;
  }

  // Signal shutdown.  Unblocks all threads currently waiting in pop()
  // or try_pop().  Idempotent.
  void close() {
    {
      std::lock_guard lock(mu_);
      closed_ = true;
    }
    cv_.notify_all();
  }

  bool is_closed() const {
    std::lock_guard lock(mu_);
    return closed_;
  }

  bool empty() const {
    std::lock_guard lock(mu_);
    return queue_.empty();
  }

 private:
  mutable std::mutex      mu_;
  std::condition_variable cv_;
  std::queue<T>           queue_;
  bool                    closed_ = false;
};

}  // namespace raftkv
