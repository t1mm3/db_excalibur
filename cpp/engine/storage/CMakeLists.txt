find_package(ZLIB REQUIRED)

add_library(storage STATIC scan_interface.cpp file_io.cpp csv_loader.cpp ${ZLIB_LIBRARIES})
target_include_directories(storage PRIVATE ${CMAKE_CURRENT_BINARY_DIR} ${ZLIB_INCLUDE_DIRS})
target_link_libraries(storage engine lz4)

