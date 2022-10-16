find_package(Backtrace)
set(HAS_LIBBACKTRACE ${Backtrace_FOUND})
include_directories(${Backtrace_INCLUDE_DIRS})



find_package(Libunwind)
set(HAS_LIBUNWIND ${LIBUNWIND_FOUND})
if(HAS_LIBUNWIND)
	include_directories(${LIBUNWIND_INCLUDE_DIR})
endif()

include_directories(${CMAKE_CURRENT_BINARY_DIR})

find_package(PAPI)
set(HAS_PAPI ${PAPI_FOUND})
if(HAS_PAPI)
	include_directories(${PAPI_INCLUDE_DIRS})
else()
	set(PAPI_LIBRARIES "")
endif()


set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.51 REQUIRED COMPONENTS context)

include_directories(${Boost_INCLUDE_DIRS})


set(EXTRA_LIBS "")

find_package(Numa)
if(NUMA_FOUND)
	include_directories(${NUMA_INCLUDE_DIR})
	list(APPEND EXTRA_LIBS "${NUMA_LIBRARY}")
	set(HAS_LIBNUMA 1)
endif()

include(CheckSymbolExists)
include(CheckFunctionExists)

CHECK_SYMBOL_EXISTS(mmap "sys/mman.h" HAVE_POSIX_MMAP)
CHECK_SYMBOL_EXISTS(sysconf "unistd.h" HAVE_POSIX_SYSCONF)
CHECK_SYMBOL_EXISTS(MAP_FIXED "sys/mman.h" HAVE_POSIX_MAP_FIXED)

CHECK_SYMBOL_EXISTS(MAP_POPULATE "sys/mman.h" HAVE_LINUX_MAP_POPULATE)
CHECK_SYMBOL_EXISTS(MAP_ANONYMOUS "sys/mman.h" HAVE_POSIX_MAP_ANONYMOUS)
CHECK_SYMBOL_EXISTS(MAP_HUGETLB "sys/mman.h" HAVE_LINUX_MAP_HUGETLB)
CHECK_SYMBOL_EXISTS(MAP_HUGE_2MB "sys/mman.h" HAVE_LINUX_MAP_HUGE_2MB)
CHECK_SYMBOL_EXISTS(MAP_HUGE_1GB "sys/mman.h" HAVE_LINUX_MAP_HUGE_1GB)

CHECK_SYMBOL_EXISTS(MADV_HUGEPAGE "sys/mman.h" HAVE_LINUX_MADV_HUGEPAGE)
CHECK_SYMBOL_EXISTS(MADV_NOHUGEPAGE "sys/mman.h" HAVE_LINUX_MADV_NOHUGEPAGE)

include (CheckSymbolExists)
include (CheckStructHasMember)

CHECK_SYMBOL_EXISTS(_vscprintf "stdio.h;stdarg.h" HAVE__VSCPRINTF)
CHECK_SYMBOL_EXISTS(vsprintf_s "stdio.h;stdarg.h" HAVE_VSPRINTF_S)
CHECK_SYMBOL_EXISTS(sysinfo "sys/sysinfo.h" HAVE_LINUX_SYSINFO)
CHECK_SYMBOL_EXISTS(sysctl "sys/sysctl.h" HAVE_POSIX_SYSCTL)

include(CheckCXXSourceCompiles) 
check_cxx_source_compiles("int main(void){ static const void* array[] = {&&lab1 }; goto *array[0]; lab1: return 0; }" HAS_COMPUTED_GOTO)

check_cxx_source_compiles("
#include <sched.h>
int main(void){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(1, &cpuset);
	return 0;
}" HAS_LINUX_cpu_set_t)


find_package(Git REQUIRED)

set(PRE_CONFIGURE_FILE "build_git.cpp.in")
set(POST_CONFIGURE_FILE "${CMAKE_CURRENT_BINARY_DIR}/build_git.cpp")
include(${CMAKE_SOURCE_DIR}/cmake/git_watcher.cmake)

configure_file(build.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/build.hpp)
configure_file(build.cpp.in ${CMAKE_CURRENT_BINARY_DIR}/build.cpp)

add_library(build_info STATIC ${CMAKE_CURRENT_BINARY_DIR}/build.cpp ${POST_CONFIGURE_FILE})

add_library(system STATIC system.cpp scheduler.cpp profiling.cpp event_bus.cpp configuration.cpp memory.cpp)
target_link_libraries(system ${CMAKE_THREAD_LIBS_INIT} ${Backtrace_LIBRARIES} ${Backtrace_LIBRARY} Remotery ${LIBUNWIND_LIBRARIES} ${Boost_LIBRARIES} ${PAPI_LIBRARIES} ${EXTRA_LIBS} build_info)

add_subdirectory(test)
