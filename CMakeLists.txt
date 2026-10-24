cmake_minimum_required(VERSION 3.16)
project(thread_safe_queue CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(Threads REQUIRED)

option(TSQ_SANITIZE_THREAD "Build with ThreadSanitizer" OFF)
if(TSQ_SANITIZE_THREAD)
  add_compile_options(-fsanitize=thread -g -O1)
  add_link_options(-fsanitize=thread)
endif()

add_library(thread_safe_queue INTERFACE)
target_include_directories(thread_safe_queue INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(thread_safe_queue INTERFACE Threads::Threads)

add_executable(example main.cpp)
target_link_libraries(example PRIVATE thread_safe_queue)
target_compile_options(example PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
add_executable(test_queue tests/test_queue.cpp)
target_link_libraries(test_queue PRIVATE thread_safe_queue)
target_compile_options(test_queue PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_queue COMMAND test_queue)
