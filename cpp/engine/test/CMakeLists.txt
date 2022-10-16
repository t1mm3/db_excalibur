add_library(test_engine_utils tpch_test_utils.cpp)

add_executable(test_engine test_engine.cpp test_engine1.cpp test_group_by.cpp test_scan.cpp test_join.cpp test_sort.cpp test_types.cpp test_parser.cpp test_strings.cpp test_csv.cpp test_tpch.cpp test_tpch_q18.cpp test_tpch_q1.cpp test_tpch_q3.cpp test_tpch_q6.cpp test_tpch_q9.cpp test_tpch_q10.cpp test_tpch_q12.cpp test_tpch_q4.cpp test_utils.cpp test_flavor_spec.cpp test_config.cpp)

target_link_libraries(test_engine excalibur test_engine_utils)

add_test(NAME test_engine COMMAND test_engine)

configure_file(tpch_test_utils.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/tpch_test_utils.hpp)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

configure_file(generate_tpch.sh generate_tpch.sh COPYONLY)
configure_file(generate_exp.py generate_exp.py)

configure_file(run_combinations.py run_combinations.py)
configure_file(run_experiments.py run_experiments.py)

configure_file(tpch_sf1_q9a.txt tpch_sf1_q9a.txt)
configure_file(tpch_sf1_q9b.txt tpch_sf1_q9b.txt)

configure_file(plot.py plot.py)


# Experiments

add_executable(experiments experiments.cpp test_utils.cpp)
target_link_libraries(experiments excalibur test_engine_utils)

add_executable(join_exp_gen_data join_exp_gen_data.cpp)