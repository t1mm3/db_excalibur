add_library(Remotery STATIC Remotery/lib/Remotery.c)

add_custom_command(TARGET Remotery POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy_directory
	${CMAKE_CURRENT_SOURCE_DIR}/Remotery/vis/ ${CMAKE_BINARY_DIR}/vis)


add_library(lz4 lz4/lib/lz4.c lz4/lib/lz4frame.c lz4/lib/lz4hc.c lz4/lib/xxhash.c)
add_library(sqlite sqlite3/sqlite3.c)