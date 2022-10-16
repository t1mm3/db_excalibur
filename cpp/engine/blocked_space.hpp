#pragma once

#include "system/system.hpp"
#include "system/memory.hpp"

struct Bandit;

namespace engine {
namespace table {
struct TableLayout;
struct BlockedSpace;

struct Block {
	size_t num;
	char* data;

	size_t width;
	size_t capacity;

	Block* prev;
	Block* next;

	uint64_t* safeguard = nullptr;
	uint64_t* safeguard2 = nullptr;

	BlockedSpace* owner = nullptr;

private:
	friend class BlockedSpace;
	void* alloc_pointer;
	size_t alloc_size;
	void* alloc_allocator;
	int64_t alloc_numa_node;

	static const uint64_t kSafeGuardValue = 0xBADF00DDEADBEEF;

	Block(size_t _width, size_t _capacity, Block* _prev, Block* _next,
			void* _alloc_pointer, size_t _alloc_size, void* _alloc_allocator,
			int64_t _alloc_numa_node, char* _data) {
		prev = _prev;
		next = _next;

		width = _width;
		capacity = _capacity;
		num = 0;

		data = _data;
		alloc_pointer = _alloc_pointer;
		alloc_size = _alloc_size;
		alloc_allocator = _alloc_allocator;
		alloc_numa_node = _alloc_numa_node;
	}

public:
	FORCE_INLINE inline size_t num_free() const { return capacity - num; }
	size_t size() const { return num; }

	FORCE_INLINE inline void debug_validate() const {
		DBG_ASSERT((size_t)safeguard % sizeof(uint64_t) == 0);
		DBG_ASSERT(safeguard && *safeguard == kSafeGuardValue);
		if (safeguard2) {
			DBG_ASSERT(*safeguard2 == kSafeGuardValue);
		}
		DBG_ASSERT(num <= capacity);
		DBG_ASSERT(width > 0);
	}

	void validate() const {
		ASSERT((size_t)safeguard % sizeof(uint64_t) == 0);
		ASSERT(safeguard && *safeguard == kSafeGuardValue);
		if (safeguard2) {
			ASSERT(*safeguard2 == kSafeGuardValue);
		}
		ASSERT(num <= capacity);
		ASSERT(width > 0);
	}
};

struct BlockedSpaceInitialization;

struct BlockedSpace {
	Block* head = nullptr;
	Block* tail = nullptr;
	const size_t width;
	const size_t vector_size;
	const size_t morsel_size;
	bool has_next = false;
	size_t next_offset = 0;
	size_t next_stride = 0;

	bool has_hash = false;
	size_t hash_offset = 0;
	size_t hash_stride = 0;
	memory::Context* mem_context = nullptr;
	std::unique_ptr<memory::Context> alloc_mem_context;

	size_t num_blocks = 0;
	TableLayout& layout;
	size_t num_allocations = 0;

	typedef uint64_t BlockedSpaceFlags;

	const BlockedSpaceFlags flags;
	const std::string dbg_name;

	static const BlockedSpaceFlags kTight = 1 << 1;
	static const BlockedSpaceFlags kNoInit = 1 << 2;

private:
	Block* new_block_at_end(size_t min_rows, int64_t numa);

	Block* new_block(Block* prev, size_t min_rows, int64_t numa);

	void init_block(char* data, size_t capacity) const;
	void init_scatter(char* data, size_t num_vals) const;
	void init_memset0(char* data, size_t num_vals) const;

	void delete_block(Block* b);

	void prepare_initialization();

	BlockedSpaceInitialization* init_columns = nullptr;

public:
	BlockedSpace(size_t width, size_t vector_size,
		size_t morsel_size, TableLayout& layout,
		BlockedSpaceFlags flags = 0,
		memory::Context* mem_context = nullptr,
		const std::string& dbg_name = "") noexcept;

	void set_next(size_t stride, size_t offset) {
		has_next = true;
		next_offset = offset;
		next_stride = stride;
	}

	void set_hash(size_t stride, size_t offset) {
		has_hash = true;
		hash_offset = offset;
		hash_stride = stride;
	}

	FORCE_INLINE inline Block* prealloc(size_t expected_num, int64_t numa) {
		Block* b = current();

		// fits in tail
		if (!b || b->num_free() < expected_num) {
			b = new_block_at_end(expected_num, numa);
		}

		DBG_ASSERT(b && b->num_free() >= expected_num);
		b->debug_validate();
		return b;
	}

	FORCE_INLINE inline  void commit(Block* b, size_t n) {
		b->num += n;
	}

	Block* current() {
		return tail;
	}

	size_t size() const {
		size_t n=0;

		for_each([&] (auto blk) {
			n += blk->size();
		});
		return n;
	}

	template<typename T>
	void for_each(const T& f, Block* start = nullptr) {
		Block* b = start ? start : head;
		while (b) {
			Block* n = b->next;

			b->validate();
			f(b);
			b = n;
		}
	}

	template<typename T>
	void for_each(const T& f, Block* start = nullptr) const {
		Block* b = start ? start : head;
		while (b) {
			Block* n = b->next;

			b->validate();
			f(b);
			b = n;
		}
	}

	void partition(BlockedSpace** partitions, size_t num_partitions,
		int64_t numa_node) const;

	~BlockedSpace();
};

} /* table */
} /* engine */