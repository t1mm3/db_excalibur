add_library(voila STATIC jit.cpp voila.cpp code_cache.cpp compile_request.cpp flavor.cpp signature.cpp jit_prepare_pass.cpp statement_identifier.cpp statement_fragment_selection_pass.cpp)

add_subdirectory(llvmcompiler)

target_link_libraries(voila voila_llvmcompiler engine)
