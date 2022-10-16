add_executable(test_scheduler test_scheduler.cpp test_scheduler1.cpp test_memory.cpp)
target_link_libraries(test_scheduler system)

add_test(NAME test_scheduler COMMAND test_scheduler)
