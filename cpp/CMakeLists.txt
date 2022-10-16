CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
project(excalibur)

enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")


if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	message(FATAL_ERROR "No build type was specified. Valid choices are either -DCMAKE_BUILD_TYPE=Release or -DCMAKE_BUILD_TYPE=Debug")
endif()


find_package(Threads REQUIRED)

set(EXTRA_LIBS "")
set(EXTRA_INCS "")


find_package(Jemalloc)
if(${JEMALLOC_FOUND})
	list(APPEND EXTRA_LIBS "${JEMALLOC_LIBRARIES}")
	list(APPEND EXTRA_INCS "${JEMALLOC_INCLUDE_DIR}")
else()
	find_package(Tcmalloc_LIBRARIES)
	if({Tcmalloc_FOUND})
		list(APPEND EXTRA_LIBS "${Tcmalloc_LIBRARIES}")
	else()
		message("Neither JEmalloc nor TCmalloc detected")
	endif()
endif()

include(CheckCXXCompilerFlag)

### Set march=native, or mcpu=native, if possible
set(NATIVE_OPTS_FLAGS "")
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
	set(NATIVE_OPTS_FLAGS "${NATIVE_OPTS_FLAGS} -march=native")
else()
CHECK_CXX_COMPILER_FLAG("-mcpu=native" COMPILER_SUPPORTS_MCPU_NATIVE)
if(COMPILER_SUPPORTS_MCPU_NATIVE)
        set(NATIVE_OPTS_FLAGS "${NATIVE_OPTS_FLAGS} -mcpu=native")
endif()
endif()



set(EXTRA_C_FLAGS " -pedantic -Wall -DCOMBINE_ALLOCS")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fPIC ${EXTRA_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fPIC ${EXTRA_C_FLAGS}")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${NATIVE_OPTS_FLAGS} -O3 -ftree-vectorize -ftree-slp-vectorize -funroll-all-loops -fpeel-loops -fomit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${NATIVE_OPTS_FLAGS} -O0")

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${EXTRA_INCS})
include_directories(third_party)
include_directories(third_party/Remotery/lib)
include_directories(third_party/crow-master/include)
include_directories(third_party/PEGTL-3.2.0/include)
include_directories(third_party/lz4/lib)

add_subdirectory(third_party)
add_subdirectory(system)
add_subdirectory(engine)
add_subdirectory(utils)
# add_subdirectory(server)

add_library(excalibur excalibur_context.cpp)
target_link_libraries(excalibur engine voila lolepops system ${EXTRA_LIBS})

add_executable(run_query run_query.cpp)
target_link_libraries(run_query excalibur)
