add_library(lolepops STATIC scan.cpp lolepop.cpp hash_join.cpp voila_lolepop.cpp sort.cpp sort_helper.cpp output.cpp hash_group_by.cpp hash_op.cpp)
target_link_libraries(lolepops voila storage)