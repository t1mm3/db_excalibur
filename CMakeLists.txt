CMAKE_MINIMUM_REQUIRED(VERSION 3.1)
project(excalibur)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	message(FATAL_ERROR "No build type was specified. Valid choices are either -DCMAKE_BUILD_TYPE=Release or -DCMAKE_BUILD_TYPE=Debug")
endif()

enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	message(FATAL_ERROR "No build type was specified. Valid choices are either -DCMAKE_BUILD_TYPE=Release or -DCMAKE_BUILD_TYPE=Debug")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

# CCache?
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
	set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
	set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# Compilation flags
set(ADDITIONAL_DEBUG_FLAGS " -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=address -fsanitize=pointer-compare -fsanitize=pointer-subtract  -fcf-protection=full -fsanitize-undefined-trap-on-error")
# set(ADDITIONAL_DEBUG_FLAGS " -fsanitize=thread ")

set(ADDITIONAL_RELEASE_FLAGS "-O3 -g ") # -fsanitize=address


set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer  ${ADDITIONAL_DEBUG_FLAGS} -DIS_DEBUG_BUILD")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer ${ADDITIONAL_DEBUG_FLAGS} -DIS_DEBUG_BUILD")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${ADDITIONAL_RELEASE_FLAGS} -DIS_RELEASE_BUILD")
set(CMAKE_LINKER_FLAGS_RELEASE "${CMAKE_LINKER_FLAGS_RELEASE} ${ADDITIONAL_RELEASE_FLAGS} -DIS_RELEASE_BUILD")

add_subdirectory(cpp)
