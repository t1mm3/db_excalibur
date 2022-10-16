#pragma once

#include <stdint.h>
#include <mutex>
#include <cstring>

namespace engine {

struct Ussr {
private:
	typedef uint16_t position_t;
	typedef uint16_t hash_extract_t;
	typedef uint64_t hash_t;
	typedef uint64_t data_granularity_t;

	struct HtEntry {
		position_t index;
		hash_extract_t hash_extract;
	};

	static constexpr char kHashExtractNumBits = sizeof(hash_extract_t)*8;
	static constexpr size_t kMaxCollsions = 3;
	static constexpr size_t kHashTableCapacity = 256*1024 / sizeof(HtEntry);
	static constexpr size_t kDataRegionSizeBytes = 512*1024;
	static constexpr size_t kHashNumSlots = (sizeof(hash_t) + sizeof(data_granularity_t)-1) / sizeof(data_granularity_t);


	char* string_from_index(position_t index) const {
		return (char*)&data_region[index];
	}

	static hash_t* get_hash_ptr_from_string(const char* s) {
		uint64_t* r = (uint64_t*)s;
		return &r[-1];
	}

	static hash_extract_t get_extract_from_hash(hash_t hash) {
		hash_extract_t extract = hash >> (64-kHashExtractNumBits);
		return extract;
	}

	const char* _lookup(const char* s, hash_t hash, size_t length) const {
		if (contains(s)) {
			return s;
		}

		hash_extract_t extract = get_extract_from_hash(hash);
		size_t index = extract;

		for (size_t r=0; r<kMaxCollsions; r++) {
			index = (extract + r) % kHashTableCapacity;

			auto& entry = hash_table[index + r];
			if (!entry.index) {
				return nullptr;
			}

			if (entry.hash_extract == extract) {
				auto dest_string = string_from_index(index);

				if (hash_from_string(dest_string) == hash && !strcmp(dest_string, s)) {
					return dest_string;
				}
			}
		}

		return nullptr;
	};

	const char* _insert(const char* s, hash_t hash, size_t length);

	void update_size_limit();

	HtEntry* hash_table;
	data_granularity_t* data_region;
	size_t data_region_num_slots;
	size_t next_free_position;
	size_t max_insertion_size;

	std::mutex* mutex;
	void* base_ptr;

public:
	Ussr();
	~Ussr();

	static const size_t kMask = ~(sizeof(data_granularity_t)*kHashTableCapacity - 1);

	template<typename T>
	static constexpr T* align(T* p, size_t align)
	{
		return (T*)(((size_t)p + align-1) / align);
	}

	bool contains(const char* s) const {
		return ((size_t)s & (~kMask)) == (size_t)data_region; 
	}

	const char* insert(const char* s, hash_t hash, size_t length) {
		// is string already inside?
		const char* r = _lookup(s, hash, length);
		if (r) {
			// already stored inside
			return r;
		}

		// consider insertion
		if (length > max_insertion_size) {
			return r;
		}

		std::unique_lock lock(*mutex);
		return _insert(s, hash, length);
	}

	struct TempBulkInsert {
		char** s;
	};

	void insert(const char** result, const char** strings, size_t num,
			TempBulkInsert& tmp, const hash_t* hashs, const size_t* lengths) {
		for (size_t i=0; i<num; i++) {
			result[i] = insert(strings[i], hashs[i], lengths[i]);
		}
	}

	static hash_t hash_from_string(const char* s) {
		return *get_hash_ptr_from_string(s);
	}
};

} /* engine */