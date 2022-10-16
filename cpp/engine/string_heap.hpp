#pragma once

#include <memory>
#include <vector>
#include <string>
#include <string.h>

namespace memory {
struct Context;
}

namespace engine {

struct StringHeap {
	typedef uint32_t length_t ;

	StringHeap(memory::Context& mem)
	 : mem_context(mem) {
	}

	~StringHeap();

	char* add_char_ptr(const char* str, size_t len);
	char* add_char_ptr(const char* s) {
		return add_char_ptr(s, strlen(s));
	}

	char* add(const std::string& s) {
		return add_char_ptr(&s[0], s.size());
	}

	static length_t* str_get_length_ptr(const char* s) {
		return (length_t*)(s - sizeof(uint64_t) - sizeof(length_t));
	}

	static uint64_t* str_get_hash_ptr(const char* s) {
		return (uint64_t*)(s - sizeof(uint64_t));
	}

	void add_n(char** dest, char** strings, size_t num);

	void clear();

	static inline length_t heap_strlen(const char* a) {
		return *str_get_length_ptr(a);
	}

	static constexpr size_t kAlign = sizeof(uint64_t);
	static_assert(kAlign >= sizeof(length_t), "Must be properly aligned");

private:
	struct Block {
		Block(size_t cap) : cap(cap) {
			data = (char*)calloc(1, cap);
		}

		~Block() {
			free(data);
		}

		bool has_space_for(size_t s) const {
			return used + s <= cap;
		}

		char* get(size_t s) {
			auto r = data + used;
			used += s;
			return r;
		}

	private:
		char* data;

		size_t used = 0;
		const size_t cap;

	public:
		Block* next = nullptr;
	};

	Block* block_tail = nullptr;

	const size_t block_size_granularity = 2*1024*1024;
	const size_t max_block_size = 128*1024*1024;
	const size_t min_block_size = block_size_granularity;

	Block* block_head = nullptr;
	size_t cur_block_size = min_block_size; 
	memory::Context& mem_context;

	void add_new_block(size_t min_sz);

	
};

} /* engine */