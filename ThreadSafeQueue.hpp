//
//  ThreadSafeQueue.hpp
//
//  Created by Christopher Goebel on 6/9/19.
//  Copyright © 2019 Christopher Goebel. All rights reserved.
//

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <utility>


/**
 *  ThreadSafeQueue
 *
 *  A multi-producer / multi-consumer queue with the semantics of Python's queue.Queue:
 *
 *    - get() blocks until an item is available.
 *    - task_done() tells the queue that one previously fetched item has been fully processed.
 *    - join() blocks until every item ever pushed has been marked task_done(), i.e. until the
 *      *work* is finished, not merely until the queue is empty.
 *    - An optional max_size bounds the queue; push() then blocks while it is full.
 *
 *    - shutdown(immediate = false) matches Python 3.13's Queue.shutdown(): pushes are refused,
 *      consumers drain what is left and then get() reports the end. With immediate = true the
 *      remaining items are discarded, their tasks are counted as done, and join() is released.
 *      Where Python raises queue.ShutDown, this queue returns false (push) or std::nullopt (get).
 *
 *  The usual shape is
 *
 *      consumer:  while (auto item = q.get()) { work(*item); q.task_done(); }
 *      producer:  for (...) q.push(x);  q.join();  q.shutdown();  then join the consumer threads
 *
 *  Lifetime: the queue must outlive every thread that uses it. Destroying it while a thread is
 *  blocked inside it is undefined behaviour (as for any mutex / condition_variable).
 */
template <typename T>
class ThreadSafeQueue
{
public:
  /** max_size == 0 means unbounded (as in Python). */
  explicit ThreadSafeQueue(std::size_t max_size = 0) : m_max_size{max_size} {}

  ThreadSafeQueue(const ThreadSafeQueue&) = delete;
  ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

  // ---- producers ------------------------------------------------------------------------

  /** Push, blocking while the queue is full. Returns false (and drops value) once shut down. */
  bool push(T value)
  {
    std::unique_lock lock{m_mutex};
    m_not_full.wait(lock, [this] { return m_shutdown || !full_locked(); });
    return push_locked(lock, std::move(value));
  }

  /** Push without blocking. Returns false if the queue is shut down or full. */
  bool try_push(T value)
  {
    std::unique_lock lock{m_mutex};
    if (m_shutdown || full_locked()) {
      return false;
    }
    return push_locked(lock, std::move(value));
  }

  /** Push, waiting at most `timeout` for space. Returns false if shut down or still full. */
  template <class Rep, class Period>
  bool push_for(T value, std::chrono::duration<Rep, Period> timeout)
  {
    std::unique_lock lock{m_mutex};
    if (!m_not_full.wait_for(lock, timeout, [this] { return m_shutdown || !full_locked(); }) || m_shutdown) {
      return false;
    }
    return push_locked(lock, std::move(value));
  }

  /** Construct an item in place, blocking while full. Returns false once shut down. */
  template <class... Args>
  bool emplace(Args&&... args)
  {
    std::unique_lock lock{m_mutex};
    m_not_full.wait(lock, [this] { return m_shutdown || !full_locked(); });
    if (m_shutdown) {
      return false;
    }
    m_queue.emplace(std::forward<Args>(args)...);
    ++m_unfinished;
    lock.unlock();
    m_not_empty.notify_one();
    return true;
  }

  // ---- consumers ------------------------------------------------------------------------

  /** Block until an item is available. std::nullopt means shut down and fully drained. */
  std::optional<T> get()
  {
    std::unique_lock lock{m_mutex};
    m_not_empty.wait(lock, [this] { return m_shutdown || !m_queue.empty(); });
    return pop_locked(lock);
  }

  /** Non-blocking get. std::nullopt if nothing is available right now. */
  std::optional<T> try_get()
  {
    std::unique_lock lock{m_mutex};
    return pop_locked(lock);
  }

  /** Wait at most `timeout` for an item. std::nullopt on timeout, or shut down and drained. */
  template <class Rep, class Period>
  std::optional<T> get_for(std::chrono::duration<Rep, Period> timeout)
  {
    std::unique_lock lock{m_mutex};
    m_not_empty.wait_for(lock, timeout, [this] { return m_shutdown || !m_queue.empty(); });
    return pop_locked(lock);
  }

  /**
   * Mark one fetched item as fully processed. Throws std::logic_error if called more times
   * than items were pushed (Python raises ValueError).
   */
  void task_done()
  {
    std::unique_lock lock{m_mutex};
    if (m_unfinished == 0) {
      throw std::logic_error("ThreadSafeQueue::task_done() called too many times");
    }
    if (--m_unfinished == 0) {
      lock.unlock();
      m_all_done.notify_all();
    }
  }

  // ---- coordination ---------------------------------------------------------------------

  /** Block until every pushed item has been marked task_done(). Does not shut the queue down. */
  void join()
  {
    std::unique_lock lock{m_mutex};
    m_all_done.wait(lock, [this] { return m_unfinished == 0; });
  }

  /**
   * Python 3.13 Queue.shutdown(). Stop accepting pushes and wake every blocked producer and
   * consumer.
   *   immediate == false: items already queued are still handed out; once they are gone,
   *                       get() returns std::nullopt.
   *   immediate == true:  remaining items are discarded and the unfinished-task count drops by
   *                       the number discarded, so join() returns once in-flight items are
   *                       task_done(). get() returns std::nullopt right away.
   * Can be called more than once; a later immediate shutdown still discards what is left.
   */
  void shutdown(bool immediate = false)
  {
    bool all_done = false;
    {
      std::scoped_lock lock{m_mutex};
      m_shutdown = true;
      if (immediate) {
        m_unfinished -= m_queue.size();
        m_queue = {};
        all_done = m_unfinished == 0;
      }
    }
    m_not_empty.notify_all();
    m_not_full.notify_all();
    if (all_done) {
      m_all_done.notify_all();
    }
  }

  // ---- observers (a snapshot only: the answer can change as soon as the lock is released) --

  bool is_shutdown() const { std::scoped_lock lock{m_mutex}; return m_shutdown; }
  bool empty() const { std::scoped_lock lock{m_mutex}; return m_queue.empty(); }
  bool full() const { std::scoped_lock lock{m_mutex}; return full_locked(); }
  std::size_t size() const { std::scoped_lock lock{m_mutex}; return m_queue.size(); }
  std::size_t unfinished_tasks() const { std::scoped_lock lock{m_mutex}; return m_unfinished; }

private:
  bool full_locked() const { return m_max_size != 0 && m_queue.size() >= m_max_size; }

  bool push_locked(std::unique_lock<std::mutex>& lock, T&& value)
  {
    if (m_shutdown) {
      return false;
    }
    m_queue.push(std::move(value));
    ++m_unfinished;
    lock.unlock();
    m_not_empty.notify_one();
    return true;
  }

  std::optional<T> pop_locked(std::unique_lock<std::mutex>& lock)
  {
    if (m_queue.empty()) {
      return std::nullopt;
    }
    std::optional<T> out{std::move(m_queue.front())};
    m_queue.pop();
    lock.unlock();
    m_not_full.notify_one();
    return out;
  }

  std::queue<T> m_queue;
  const std::size_t m_max_size;
  std::size_t m_unfinished{0};   // pushed but not yet task_done()
  bool m_shutdown{false};

  mutable std::mutex m_mutex;    // guards everything above
  std::condition_variable m_not_empty;   // consumers: item available or shut down
  std::condition_variable m_not_full;    // producers: space available or shut down
  std::condition_variable m_all_done;    // join(): unfinished reached zero
};
