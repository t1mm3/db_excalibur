add_library(engine STATIC engine.cpp query.cpp types.cpp stream.cpp type_stream.cpp stream_pass.cpp table.cpp table_reader.cpp blocked_space.cpp xvalue.cpp plan.cpp string_heap.cpp catalog.cpp buffer.cpp sql_types.cpp utils.cpp profile.cpp loleplan_passes.cpp var_life_time_pass.cpp table_utils.cpp table_utils_impl1.cpp table_utils_impl2.cpp table_utils_impl3.cpp table_utils_impl4.cpp table_utils_impl5.cpp table_utils_impl6.cpp table_utils_impl7.cpp table_utils_impl8.cpp table_utils_impl9.cpp budget.cpp protoplan.cpp partitioner.cpp bloomfilter.cpp util_kernels.cpp bandit_utils.cpp)

target_include_directories(engine PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(engine lolepops voila engine_adaptive ${CMAKE_THREAD_LIBS_INIT})

add_subdirectory(lolepops)
add_subdirectory(voila)
add_subdirectory(test)
add_subdirectory(storage)
add_subdirectory(adaptive)
# add_subdirectory(arrow)

configure_file(profile_viewer.py profile_viewer.py COPYONLY)