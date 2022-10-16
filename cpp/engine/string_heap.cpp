#include "string_heap.hpp"

#include "string_utils.hpp"

#include "system/system.hpp"

using namespace engine;

void
StringHeap::add_new_block(size_t min_sz)
{
	size_t block_size = std::max(min_sz, cur_block_size);
	block_size = (block_size + block_size_granularity-1) / block_size_granularity;
	block_size *= block_size_granularity;

	cur_block_size = std::min(block_size, max_block_size);

	Block* b = new Block(block_size);

	if (block_tail) {
		block_tail->next = b; 
	}
	if (!block_head) {
		block_head = b;
	}
	block_tail = b;
}

StringHeap::~StringHeap()
{
	clear();
}

void
StringHeap::clear()
{
	Block* b = block_head;
	while (b) {
		auto next = b->next;
		delete b;
		b = next;
	}

	block_head = nullptr;
	block_tail = nullptr;
}

void
StringHeap::add_n(char** dest, char** strings, size_t num)
{
	for (size_t i=0; i<num; i++) {
		dest[i] = add_char_ptr(strings[i]);
	}
}

char*
StringHeap::add_char_ptr(const char* str, size_t len)
{
	size_t sz = len+1+2*sizeof(length_t)+sizeof(uint64_t) + kAlign-1;

	if (!block_tail || !block_tail->has_space_for(sz)) {
		add_new_block(sz);
	}

	char* ptr = block_tail->get(sz);

	// align pointer
	{
		size_t p = (size_t)ptr + sizeof(uint64_t) + 2*sizeof(length_t) + kAlign-1;
		p /= kAlign;
		p *= kAlign;

		ptr = (char*)p;
	}

	auto h = StringUtils::hash(str, len);

	// copy string
	auto plen = str_get_length_ptr(ptr);
	*plen = len;

	auto phash = str_get_hash_ptr(ptr);
	*phash = h;

	memmove(ptr, str, len);

#ifdef IS_DEBUG_BUILD
	ASSERT(((size_t)phash % sizeof(uint64_t)) == 0);
	ASSERT(((size_t)plen % sizeof(length_t)) == 0);
	ASSERT(*phash == h);
	ASSERT(*plen == len);
#endif
	return ptr;
}