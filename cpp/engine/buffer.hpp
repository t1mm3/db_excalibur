#pragma once

#include <memory>
#include <vector>

namespace engine {
struct Type;
struct StringHeap;
}

namespace memory {
struct Context;
struct UntypedArray;
}

namespace engine {
	
struct Buffer {
	void append_n(void* data, size_t num) {
		prealloc_append_n(num);
		prealloc_insert_n(data, num);
	}

	void* prealloc_append_n(size_t num);
	void prealloc_insert_n(void* data, size_t num);

	char* get_data() const;
	size_t size() const;

	void clear();

	Buffer(Type& data_type);

	Type& get_data_type() const {
		return data_type;
	}

private:
	std::unique_ptr<memory::UntypedArray> buffer;
	Type& data_type;
	std::unique_ptr<StringHeap> heap;
	std::unique_ptr<memory::Context> mem_context;
	std::vector<char> tmp_data;

	size_t width;
};

} /* engine */