#include "buffer.hpp"

#include "string_heap.hpp"
#include "types.hpp"
#include "system/memory.hpp"
#include "system/system.hpp"

using namespace engine;

Buffer::Buffer(Type& data_type)
 : data_type(data_type)
{
	width = data_type.get_width();

	mem_context = std::make_unique<memory::Context>(nullptr, "Buffer", -1);
	buffer = std::make_unique<memory::UntypedArray>(width);

	if (data_type.is_var_len()) {
		heap = std::make_unique<StringHeap>(*mem_context);
	}
}

void*
Buffer::prealloc_append_n(size_t num)
{
	buffer->prealloc(num);
	return buffer->get_tail();
}

void
Buffer::prealloc_insert_n(void* src_data, size_t src_num)
{
	void* insert_data = src_data;

	if (heap) {
		if (tmp_data.size() < src_num * width) {
			tmp_data.resize(src_num * width);
		}

		ASSERT(width == sizeof(char*));

		insert_data = &tmp_data[0];
		heap->add_n((char**)insert_data, (char**)src_data, src_num);
	}

	buffer->insert(insert_data, src_num);
}

char*
Buffer::get_data() const
{
	return (char*)buffer->get_data();
}

size_t
Buffer::size() const
{
	return buffer->size();
}

void
Buffer::clear()
{
	buffer->clear();
	if (heap) {
		heap->clear();
	}
}