set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED COMPONENTS file_system)

add_executable(server server.cpp)
target_link_libraries(server engine voila lolepops system ${Boost_LIBRARIES})