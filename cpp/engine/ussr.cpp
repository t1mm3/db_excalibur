#include "ussr.hpp"

#include "system/system.hpp"

#include <algorithm>

using namespace engine;

const char*
Ussr::_insert(const char* s, hash_t hash, size_t length)
{
	hash_extract_t extract = get_extract_from_hash(hash);
	size_t index = extract;

	for (size_t r=0; r<kMaxCollsions; r++) {
		index = (extract + r) % kHashTableCapacity;

		auto& entry = hash_table[index + r];
		if (!entry.index) {
			position_t pos = next_free_position;
			auto dest_string = string_from_index(pos);

			size_t num_slots = (length + sizeof(data_granularity_t)-1) / sizeof(data_granularity_t);
			num_slots += kHashNumSlots;

			if (next_free_position + num_slots >= data_region_num_slots) {
				return s;
			}

			entry.index = next_free_position;
			entry.hash_extract = extract;

			*get_hash_ptr_from_string(dest_string) = hash;

			memcpy(dest_string, s, length+1);
			next_free_position += num_slots;

			update_size_limit();

			return dest_string;
		}

		if (entry.hash_extract == extract) {
			auto dest_string = string_from_index(index);

			if (hash_from_string(dest_string) == hash && !strcmp(dest_string, s)) {
				return dest_string;
			}
		}
	}

	return s;
}

void
Ussr::update_size_limit()
{
	size_t free_slots = data_region_num_slots - next_free_position;
	size_t free_bytes = free_slots * sizeof(data_granularity_t);

	if (free_slots < kHashNumSlots + 1) {
		// too full
		max_insertion_size = 0;
		return;
	}

	max_insertion_size = std::min(free_bytes, std::max((size_t)2, free_bytes / 64));
}

Ussr::Ussr()
{
	const size_t total_size = 1024*1024;

	base_ptr = malloc(total_size);
	char* p = (char*)base_ptr;
	char* p_end = p + total_size;

	data_region = (data_granularity_t*)(((size_t)base_ptr + kDataRegionSizeBytes) & kMask);
	ASSERT((size_t)p_end > (size_t)data_region);
	data_region_num_slots = kDataRegionSizeBytes / sizeof(data_granularity_t);

	hash_table = (HtEntry*)p;
	if ((size_t)(hash_table + kHashTableCapacity) > (size_t)data_region) {
		// not enough space before data_region, allocate after data region
		hash_table = (HtEntry*)((size_t)data_region + kDataRegionSizeBytes);

		ASSERT((size_t)(hash_table + kHashTableCapacity) <= (size_t)p_end);
	}

	memset(hash_table, 0, sizeof(HtEntry)*kHashTableCapacity);

	memset(data_region, 0, data_region_num_slots*sizeof(data_granularity_t));

	next_free_position = 0;
	max_insertion_size = 0;

	update_size_limit();

	mutex = new std::mutex();
}

Ussr::~Ussr()
{
	delete mutex;
	free(base_ptr);
}
