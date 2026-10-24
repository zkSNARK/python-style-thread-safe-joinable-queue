//
//  main.cpp
//
//  Created by Christopher Goebel on 6/9/19.
//  Copyright © 2019 Christopher Goebel. All rights reserved.
//
//  The worker-pool pattern from Raymond Hettinger's "Thinking about Concurrency" (PyCon 2016):
//  workers pull jobs from one queue, a single printer thread owns stdout and is fed by a second
//  queue, and join() is used to know when each stage has finished its work.
//

#include "ThreadSafeQueue.hpp"

#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main()
{
  ThreadSafeQueue<long> jobs{16};          // bounded: producers block instead of piling up work
  ThreadSafeQueue<std::string> print_queue;

  // The only thread that touches std::cout, so lines never interleave.
  std::thread printer{[&] {
    while (auto line = print_queue.get()) {
      std::cout << *line << '\n';
      print_queue.task_done();
    }
  }};

  std::vector<std::thread> workers;
  for (int id = 0; id < 4; ++id) {
    workers.emplace_back([&, id] {
      while (auto n = jobs.get()) {
        print_queue.push("worker " + std::to_string(id) + ": " + std::to_string(*n) + "^2 = " +
                         std::to_string(*n * *n));
        jobs.task_done();
      }
    });
  }

  for (long n = 1; n <= 20; ++n) {
    jobs.push(n);
  }

  jobs.join();          // every job has been processed, not just dequeued
  print_queue.join();   // every line has been printed
  std::cout << "all work done\n";

  jobs.shutdown();      // let the threads fall out of their loops
  print_queue.shutdown();
  for (auto& w : workers) {
    w.join();
  }
  printer.join();
}
