# python-style-thread-safe-joinable-queue

A header-only C++17 multi-producer / multi-consumer queue with the semantics of Python's
[`queue.Queue`](https://docs.python.org/3/library/queue.html), so the patterns from Raymond
Hettinger's PyCon 2016 talk ["Thinking about Concurrency"](https://youtu.be/Bv25Dwe84g0) carry
over directly.

```cpp
#include "ThreadSafeQueue.hpp"

ThreadSafeQueue<Job> jobs{64};            // optional max size; 0 (default) = unbounded

// consumer
while (auto job = jobs.get()) {           // blocks; std::nullopt once shut down and drained
  run(*job);
  jobs.task_done();                       // this item is fully processed
}

// producer
for (auto& j : work) jobs.push(j);        // blocks while the queue is full
jobs.join();                              // waits until every item is task_done(), not just dequeued
jobs.shutdown();                          // consumers drain what is left, then get() returns nullopt
```

| Python `queue.Queue` | `ThreadSafeQueue<T>` |
|---|---|
| `Queue(maxsize)` | `ThreadSafeQueue<T>(max_size)` |
| `put(x)` / `put_nowait(x)` / `put(x, timeout=t)` | `push(x)` / `try_push(x)` / `push_for(x, t)`, plus `emplace(args...)` |
| `get()` / `get_nowait()` / `get(timeout=t)` | `get()` / `try_get()` / `get_for(t)`, returning `std::optional<T>` |
| `task_done()` (ValueError if overcalled) | `task_done()` (`std::logic_error` if overcalled) |
| `join()` | `join()` |
| `qsize()` / `empty()` / `full()` | `size()` / `empty()` / `full()`, plus `unfinished_tasks()` |
| `shutdown()` / `shutdown(immediate=True)` (3.13+) | `shutdown()` / `shutdown(true)` |
| `queue.ShutDown` raised by `put` / `get` | `push` returns `false` / `get` returns `std::nullopt` |

`shutdown()` wakes every blocked producer and consumer; consumers drain what is left, then `get()`
returns `std::nullopt`. `shutdown(true)` discards the remaining items and counts their tasks as done,
so `join()` returns once the items already handed out are `task_done()`. `is_shutdown()` reports the
state. The queue must outlive every thread using it.

`main.cpp` is the worker-pool example from the talk: four workers, a single printer thread that
owns stdout, and `join()` on each stage.

## Build and test

```sh
cmake -S . -B build && cmake --build build
./build/test_queue      # or: ctest --test-dir build
./build/example

# with ThreadSanitizer
cmake -S . -B build-tsan -DTSQ_SANITIZE_THREAD=ON && cmake --build build-tsan && ./build-tsan/test_queue
```
