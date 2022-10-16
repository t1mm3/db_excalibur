find_package(LLVM REQUIRED CONFIG)
if (NOT LLVM_FOUND)
	message(FATAL_ERROR "LLVM required")
endif()

message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")


include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

add_library(voila_llvmcompiler STATIC llvm_interface.cpp llvm_fragment.cpp llvm_fragment.cpp llvm_compiler.cpp)

llvm_map_components_to_libnames(llvm_libs
	executionengine nativecodegen native AggressiveInstCombine orcjit orcerror native runtimeDyld passes instcombine scalaropts analysis ipo instrumentation ObjCARCOpts globalISel SelectionDAG debugInfoDWARF passes object core irreader transformutils vectorize demangle tablegen support option)

set(EXTRA_LIBS "${llvm_libs}")


find_package(CAPSTONE)

if(CAPSTONE_FOUND)
	list(APPEND EXTRA_LIBS "${CAPSTONE_LIBRARIES}")
	include_directories(${CAPSTONE_INCLUDE_DIRS})

	set(HAS_CAPSTONE 1)
endif()

configure_file(llvm_build_utils.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/llvm_build_utils.hpp)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(voila_llvmcompiler system ${EXTRA_LIBS})

