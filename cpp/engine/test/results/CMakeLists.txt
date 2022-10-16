add_library(test_engine_utils tpch_test_utils.cpp)

add_executable(test_engine test_engine.cpp test_engine1.cpp test_group_by.cpp test_scan.cpp test_join.cpp test_sort.cpp test_types.cpp test_parser.cpp test_strings.cpp test_csv.cpp test_tpch.cpp )

target_link_libraries(test_engine engine test_engine_utils)

add_test(NAME test_engine COMMAND test_engine)

configure_file(tpch_test_utils.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/tpch_test_utils.hpp)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

configure_file(generate_tpch.sh generate_tpch.sh COPYONLY)

configure_file(results_tpch_q1_sf1.txt results_tpch_q1_sf1.txt COPYONLY)
configure_file(results_tpch_q3_sf1.txt results_tpch_q3_sf1.txt COPYONLY)
configure_file(results_tpch_q4_sf1.txt results_tpch_q4_sf1.txt COPYONLY)
configure_file(results_tpch_q6_sf1.txt results_tpch_q6_sf1.txt COPYONLY)
configure_file(results_tpch_q9_sf1.txt results_tpch_q9_sf1.txt COPYONLY)
configure_file(results_tpch_q10_sf1.txt results_tpch_q10_sf1.txt COPYONLY)
configure_file(results_tpch_q12_sf1.txt results_tpch_q12_sf1.txt COPYONLY)
configure_file(results_tpch_q18_sf1.txt results_tpch_q18_sf1.txt COPYONLY)

configure_file(results_tpch_q1_sf10.txt results_tpch_q1_sf10.txt COPYONLY)
configure_file(results_tpch_q3_sf10.txt results_tpch_q3_sf10.txt COPYONLY)
configure_file(results_tpch_q4_sf10.txt results_tpch_q4_sf10.txt COPYONLY)
configure_file(results_tpch_q6_sf10.txt results_tpch_q6_sf10.txt COPYONLY)
configure_file(results_tpch_q9_sf10.txt results_tpch_q9_sf10.txt COPYONLY)
configure_file(results_tpch_q10_sf10.txt results_tpch_q10_sf10.txt COPYONLY)
configure_file(results_tpch_q12_sf10.txt results_tpch_q10_sf12.txt COPYONLY)
configure_file(results_tpch_q18_sf10.txt results_tpch_q18_sf10.txt COPYONLY)

configure_file(results_tpch_q1_sf100.txt results_tpch_q1_sf100.txt COPYONLY)
configure_file(results_tpch_q3_sf100.txt results_tpch_q3_sf100.txt COPYONLY)
configure_file(results_tpch_q6_sf100.txt results_tpch_q6_sf100.txt COPYONLY)
configure_file(results_tpch_q9_sf100.txt results_tpch_q9_sf100.txt COPYONLY)

# Experiments

add_executable(exp_compilation_time exp_compilation_time)
target_link_libraries(exp_compilation_time engine test_engine_utils)