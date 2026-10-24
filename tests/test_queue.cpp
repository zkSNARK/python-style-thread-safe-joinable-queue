// Tests for ThreadSafeQueue. No framework: each check prints and the run fails on the first
// broken expectation. Anything that could hang runs on a std::async with a deadline instead.
#include "../ThreadSafeQueue.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

#define CHECK(cond)                                                                  \
  do {                                                                               \
    if (!(cond)) {                                                                   \
      std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);           \
      std::exit(1);                                                                  \
    }                                                                                \
  } while (0)

template <class F>
bool finishes_within(std::chrono::milliseconds limit, F&& f)
{
  auto fut = std::async(std::launch::async, std::forward<F>(f));
  if (fut.wait_for(limit) == std::future_status::ready) {
    fut.get();
    return true;
  }
  std::fprintf(stderr, "  (blocked past %lld ms)\n", static_cast<long long>(limit.count()));
  std::quick_exit(1);   // the stuck thread can't be joined; fail the run outright
}

static void test_join_waits_for_the_work_not_the_pops()
{
  ThreadSafeQueue<int> q;
  std::atomic<int> done{0};
  std::thread worker{[&] {
    while (auto v = q.get()) {
      std::this_thread::sleep_for(20ms);
      ++done;
      q.task_done();
    }
  }};
  for (int i = 0; i < 5; ++i) q.push(i);
  q.join();
  CHECK(done == 5);
  q.close();
  worker.join();
}

static void test_join_on_an_idle_queue_returns_and_close_wakes_idle_consumers()
{
  ThreadSafeQueue<int> q;
  std::vector<std::thread> consumers;
  std::atomic<int> exited{0};
  for (int i = 0; i < 3; ++i) {
    consumers.emplace_back([&] { while (auto v = q.get()) q.task_done(); ++exited; });
  }
  q.push(1);
  std::this_thread::sleep_for(50ms);           // everyone drained and is now blocked in get()
  CHECK(finishes_within(1000ms, [&] { q.join(); }));
  q.close();
  CHECK(finishes_within(1000ms, [&] { for (auto& c : consumers) c.join(); }));
  CHECK(exited == 3);
}

static void test_close_drains_then_returns_nullopt()
{
  ThreadSafeQueue<int> q;
  q.push(1);
  q.push(2);
  q.close();
  CHECK(!q.push(3));
  CHECK(!q.emplace(3));
  CHECK(q.get() == 1);
  CHECK(q.get() == 2);
  CHECK(!q.get().has_value());
  CHECK(q.closed());
}

static void test_bounded_queue_blocks_producers_and_close_releases_them()
{
  ThreadSafeQueue<int> q{2};
  CHECK(q.push(1) && q.push(2));
  CHECK(q.full());
  CHECK(!q.try_push(3));
  CHECK(!q.push_for(3, 30ms));

  std::atomic<bool> pushed{false};
  std::thread producer{[&] { pushed = q.push(3); }};
  std::this_thread::sleep_for(50ms);
  CHECK(!pushed);                              // still blocked: queue is full
  CHECK(q.get() == 1);                         // make room
  producer.join();
  CHECK(pushed && q.size() == 2);

  std::atomic<int> result{-1};
  std::thread blocked{[&] { result = q.push(4) ? 1 : 0; }};
  std::this_thread::sleep_for(50ms);
  q.close();
  CHECK(finishes_within(1000ms, [&] { blocked.join(); }));
  CHECK(result == 0);
}

static void test_non_blocking_and_timed_get()
{
  ThreadSafeQueue<int> q;
  CHECK(!q.try_get().has_value());
  auto t0 = std::chrono::steady_clock::now();
  CHECK(!q.get_for(40ms).has_value());
  CHECK(std::chrono::steady_clock::now() - t0 >= 35ms);
  q.push(7);
  CHECK(q.try_get() == 7);
}

static void test_task_done_too_many_times_throws()
{
  ThreadSafeQueue<int> q;
  q.push(1);
  (void)q.get();
  q.task_done();
  bool threw = false;
  try { q.task_done(); } catch (const std::logic_error&) { threw = true; }
  CHECK(threw);
}

static void test_move_only_items()
{
  ThreadSafeQueue<std::unique_ptr<int>> q;
  q.push(std::make_unique<int>(5));
  q.emplace(new int(6));
  CHECK(**q.get() == 5);
  CHECK(**q.get() == 6);
}

static void test_many_producers_many_consumers()
{
  constexpr int producers = 4, consumers = 4, per_producer = 50000;
  ThreadSafeQueue<long> q{64};
  std::atomic<long> sum{0}, count{0};
  std::vector<std::thread> threads;
  for (int c = 0; c < consumers; ++c) {
    threads.emplace_back([&] {
      while (auto v = q.get()) { sum += *v; ++count; q.task_done(); }
    });
  }
  std::vector<std::thread> prods;
  for (int p = 0; p < producers; ++p) {
    prods.emplace_back([&, p] { for (long i = 0; i < per_producer; ++i) q.push(p * per_producer + i); });
  }
  for (auto& t : prods) t.join();
  CHECK(finishes_within(10000ms, [&] { q.join(); }));
  const long n = static_cast<long>(producers) * per_producer;
  CHECK(count == n);
  CHECK(sum == n * (n - 1) / 2);
  CHECK(q.unfinished_tasks() == 0 && q.empty());
  q.close();
  for (auto& t : threads) t.join();
}

int main()
{
  struct { const char* name; void (*fn)(); } tests[] = {
    {"join waits for the work, not the pops", test_join_waits_for_the_work_not_the_pops},
    {"join on idle queue returns; close wakes idle consumers", test_join_on_an_idle_queue_returns_and_close_wakes_idle_consumers},
    {"close drains, then get() returns nullopt", test_close_drains_then_returns_nullopt},
    {"bounded queue blocks producers; close releases them", test_bounded_queue_blocks_producers_and_close_releases_them},
    {"try_get / get_for", test_non_blocking_and_timed_get},
    {"task_done too many times throws", test_task_done_too_many_times_throws},
    {"move-only items", test_move_only_items},
    {"4 producers x 4 consumers, 200k items", test_many_producers_many_consumers},
  };
  for (auto& t : tests) {
    t.fn();
    std::printf("ok   %s\n", t.name);
  }
}
