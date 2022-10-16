#pragma once

#include "system/system.hpp"

namespace engine {

struct Partitioner {
	typedef void (*runtime_partition_call_t)(size_t* dest_counts,
		char** dest_data, size_t num_parts, char* data,
		size_t width, uint64_t* hashes, size_t num, size_t offset);

	static runtime_partition_call_t get_runtime_partition(size_t num_parts, size_t width);

	// decomposed

	typedef void (*compute_partition_call_t)(uint64_t* partition_id,
		uint64_t* dest_offset, size_t* dest_counts, size_t num_parts,
		uint64_t* hashes, size_t num);
	static compute_partition_call_t get_compute_partition(size_t num_parts);

	typedef void (*compute_dest_pointers_call_t)(char** dest_ptr, char** dest_data,
		uint64_t* partition_id, uint64_t* dest_offset, size_t _width, size_t num);

	static compute_dest_pointers_call_t get_compute_dest_pointers(size_t width);

	typedef void (*scatter_call_t)(char** dest_ptrs, char* source,
		size_t _width, size_t num);

	static scatter_call_t get_scatter(size_t width);
};

} /* engine */