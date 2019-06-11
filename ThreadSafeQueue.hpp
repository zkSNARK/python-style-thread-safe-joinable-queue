//
//  ThreadSafeQueue.hpp
//  Project: queued_thread_control
//
//  Created by Christopher Goebel on 6/9/19.
//  Copyright © 2019 Christopher Goebel. All rights reserved.
//

#pragma once

#include <string>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>
#include <optional>


/**
 *  ThreadSafeQueue class.
 *
 *  A wrapper around std::queue which provides safe threaded access.
 *
 *  This queue uses mutexs and its "pop" method is implemented in
 *  terms of nonstd::optional as an experiment.
 */
template <typename T>
class locked_opt_queue
{
public:
  
  /**
   * Destructor.  Invalidate so that any threads waiting on the
   * condition are notified.
   */
  ~locked_opt_queue()
  {
    stop();
  }
  
  /**
   * Wait on queue condition variable indefinitely until the queue
   * is marked for stop or a value is pushed into the queue.
   */
  std::optional<T> wait_pop() {
    std::unique_lock<std::mutex> lock{m_mutex}; // requires unique for
    
    if (m_joined) {
      return {};
    }
    
    // use wait(lock, predicate) to handle spurious wakeup and
    // termination condition
    m_condition.wait(lock,
      [this]() {
        return !m_queue.empty() or m_joined;
      }
    );
    
    if (m_joined) {
      return {};        // return a failed option
    }
    
    T out = std::move(m_queue.front());
    m_queue.pop();
    
    return out;
  }
  
  /**
   * Wait on queue condition variable indefinitely until the queue
   * is marked for stop or a value is pushed into the queue.
   */
  std::optional<T> get() {
    std::unique_lock<std::mutex> lock { m_mutex }; // requires unique for
    
    if (m_joined and m_queue.size() == 0) {
      return {};
    }
    
    // use wait(lock, predicate) to handle spurious wakeup and
    // termination condition
    m_condition.wait(lock,
      [this]() {
        return !m_queue.empty() or (m_joined and m_queue.empty());
      }
    );
    
    if (m_joined and m_queue.size() == 0) {
      return {};        // return a failed option
    }
    
    T out = std::move(m_queue.front());
    m_queue.pop();
    
    m_condition.notify_all();
    return out;
  }
  
  
  /**
   * Push a new value into the queue.  Returns false if queue
   * is stopped.
   */
  bool push(T value)
  {
    std::unique_lock<std::mutex> lock{m_mutex};
    
    if(m_joined) {
      return false;
    }
    
    m_queue.push(std::move(value));
    lock.unlock();
    
    m_condition.notify_one();
    return true;
  }
  
  /**
   * Check whether or not the queue is empty.
   */
  bool empty() const
  {
    std::scoped_lock<std::mutex> lock{m_mutex};
    return m_queue.empty();
  }
  
  /**
   * A queue is 'complete' when it is both marked for join and empty.
   *
   */
  bool complete(){
    std::scoped_lock<std::mutex> lock{m_mutex};
    return m_joined and m_queue.empty();
  }
  
  /**
   * Shut the queue down by marking the valid bit false and
   * notify any waiting threads.
   *
   * All further calls to the queue will result in undefined behavior.
   */
  void stop()
  {
    std::unique_lock<std::mutex> lock { m_mutex };
    m_joined = true;
    
    lock.unlock();
    
    m_condition.notify_all();
  }
  
  void join() {
    std::unique_lock<std::mutex> lock{m_mutex}; // requires unique for
    
    m_joined = true;
    
    if (m_queue.size() == 0) {
      return;
    }
    
    // use wait(lock, predicate) to handle spurious wakeup and
    // termination condition
    m_condition.wait(lock,
      [this]() {
        return m_joined and m_queue.empty();
      }
    );
  }
  
  /**
   * Returns the current size of the queue.
   */
  inline auto size() const
  {
    std::scoped_lock<std::mutex> lock{m_mutex};
    return m_queue.size();
  }
  
private:
  std::queue<T> m_queue;
  
  // the mutable tag allows const functions to modify the mutex...
  // it tells the const functions "except me... I'm not const."
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
  std::atomic_bool m_joined{false};
  
};
