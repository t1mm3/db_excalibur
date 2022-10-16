#include "blocked_space.hpp"

#include "system/system.hpp"
#include "system/memory.hpp"
#include "system/build.hpp"
#include "system/profiling.hpp"

#include "engine/util_kernels.hpp"
#include "engine/partitioner.hpp"
#include "engine/table.hpp"
#include "engine/types.hpp"
#include "engine/bandit_utils.hpp"

#include <vector>
#include <memory>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "system/build.hpp"

using namespace engine;
using namespace table;

const static size_t kGrowAllocMultiplicator = 2;
const size_t kNumChoices = 2;

namespace engine::table {

struct BlockedSpaceSetColVal {
	size_t offset;
	size_t stride;
	size_t width;

	engine::util_kernels::Utils::scatter_set_call_t setter;
};

struct BlockedSpaceInitialization {
	BlockedSpaceSetColVal* init_columns = nullptr;
	size_t num_cols;
	BanditStaticArms<kNumChoices>* bandit = nullptr;

	static_assert(std::is_pod<BlockedSpaceSetColVal>::value,
		"Must be POD for simple allocation");
};

}


BlockedSpace::BlockedSpace(size_t width, size_t vector_size,
	size_t morsel_size, TableLayout& layout,
	BlockedSpaceFlags flags, memory::Context* given_mem_context,
	const std::string& dbg_name) noexcept
 : head(nullptr), tail(nullptr), width(width),
	vector_size(vector_size), morsel_size(morsel_size),
	mem_context(given_mem_context), layout(layout),
	flags(flags), dbg_name(dbg_name)
{
}

template<typename T>
T round_up_to(const T& a, const T& round)
{
	return ((a + round-1) / round) * round;
}

static const size_t kFirstAllocMinRows = 1024;

Block*
BlockedSpace::new_block(Block* prev, size_t expected_rows, int64_t numa)
{
	if (!init_columns) {
		prepare_initialization();
	}

	size_t min_rows = std::max(2*expected_rows, (size_t)64);

	size_t payload_bytes = width * min_rows;
	size_t payload_safe_guard = sizeof(uint64_t);

	Block* result;

	size_t bytes = 2*sizeof(Block) + width + payload_bytes + 2*payload_safe_guard;

	void* alloc_pointer_allocator = nullptr;
	int64_t alloc_pointer_numa = numa;

	char* alloc_pointer;
	{
		auto alloc_flags = memory::Context::kNoRealloc;
		if (!num_allocations || flags & kTight) {
			// first allocation, be nice
			alloc_pointer = (char*)mem_context->aligned_alloc(bytes,
				alignof(Block), alloc_flags);
		} else {
			// allocate remaining memory chunk and modify 'bytes'
			alloc_pointer = (char*)mem_context->exclusive_alloc(bytes,
				alignof(Block), alloc_flags);
		}

	}

#ifdef IS_DEBUG_BUILD
	memset(alloc_pointer, -1, bytes);
#endif

	size_t payload_offset = round_up_to(
		(size_t)(sizeof(Block) + payload_safe_guard + alloc_pointer),
		width);
	payload_offset -= (size_t)alloc_pointer;

	size_t alloc_rows = (bytes - payload_offset - 4*payload_safe_guard) / width;
	ASSERT(alloc_pointer && bytes > 0 && bytes > payload_offset &&
		alloc_rows >= min_rows);

	char* payload_data = alloc_pointer + payload_offset;
	ASSERT(((size_t)payload_data % width) == 0);

	result = new(alloc_pointer) Block(width, alloc_rows, prev, nullptr,
		alloc_pointer, bytes, alloc_pointer_allocator, alloc_pointer_numa,
		payload_data);
	ASSERT((char*)result + sizeof(Block) <= payload_data);

	// put one safeguard after
	size_t guard_ptr = (size_t)payload_data + width*alloc_rows;
	guard_ptr = ((guard_ptr+payload_safe_guard-1)
		/ payload_safe_guard) * payload_safe_guard;

	result->safeguard = (uint64_t*)guard_ptr;
	ASSERT((size_t)result->safeguard % sizeof(result->safeguard) == 0);

	// ... and one before
	guard_ptr = (size_t)payload_data - payload_safe_guard;
	guard_ptr = ((guard_ptr) / payload_safe_guard) * payload_safe_guard;
	result->safeguard2 = (uint64_t*)guard_ptr;
	ASSERT((size_t)result->safeguard2 % sizeof(result->safeguard2) == 0);

	ASSERT((char*)result + sizeof(Block) <= (char*)result->safeguard2);

	if (init_columns->num_cols) {
		init_block(result->data, result->capacity);
	}

	*result->safeguard = Block::kSafeGuardValue;
	if (result->safeguard2) {
		*result->safeguard2 = Block::kSafeGuardValue;
	}
	result->owner = this;

	num_allocations++;
	return result;
}

const size_t kBlockInitVectorSize = 512;

void
BlockedSpace::prepare_initialization()
{
	init_columns = mem_context->newObj<BlockedSpaceInitialization>();
	init_columns->bandit = mem_context->newObj<BanditStaticArms<kNumChoices>>();
	init_columns->init_columns =
		mem_context->typed_array_alloc<BlockedSpaceSetColVal>(
			layout.get_num_init(), 0, memory::Context::kNoRealloc);
	init_columns->num_cols = layout.get_num_init();

	size_t num_init = 0;
	for (size_t i = 0; i<layout.get_num_columns(); i++) {
		if (!layout.get_column_must_init(i)) {
			continue;
		}

		auto offset = layout.get_column_offset(i);
		auto stride = layout.get_column_stride(i);
		auto width = layout.get_column_type(i)->get_width();

		ASSERT(num_init <= layout.get_num_init());
		LOG_TRACE("offset %lld, stride %lld, width %lld", offset, stride, width);
		auto setter = engine::util_kernels::Utils::get_scatter_set(width, stride);
		ASSERT(setter && offset >= 0 && stride >= 1 && width >= 0);

		init_columns->init_columns[num_init] = BlockedSpaceSetColVal {
			offset, stride, width, setter };

		num_init++;
	}
}

void
BlockedSpace::init_scatter(char* data, size_t num) const
{
	char* null_val_ptr = nullptr;

	for (size_t i=0; i<init_columns->num_cols; i++) {
		auto& init = init_columns->init_columns[i];
		char* dest = data + init.offset;

		LOG_TRACE("dest=%p, offset=%llu, num=%llu", dest, init.offset, num);
		init.setter(dest, init.stride, null_val_ptr, init.width, num);
	}
}

void
BlockedSpace::init_memset0(char* data, size_t num) const
{
	memory::BaseAllocator::memset0(data, width * num);
}

void
BlockedSpace::init_block(char* data, size_t capacity) const
{
	auto& bandit = *init_columns->bandit;
	for (size_t offset = 0; offset < capacity; offset += kBlockInitVectorSize) {
		size_t num = std::min(kBlockInitVectorSize, capacity - offset);
		char* p = data + offset*width;

		LOG_TRACE("p=%p, num=%llu", p, num);
		size_t arm = bandit.choose_arm();
		ASSERT(arm < kNumChoices);

		auto start_clock = profiling::physical_rdtsc();
		switch (arm) {
		case 0: init_memset0(p, num); break;
		case 1: init_scatter(p, num); break;
		}
		auto end_clock = profiling::physical_rdtsc();

		double diff = end_clock - start_clock;
		bandit.record(arm, -diff, num);
	}
}

void
BlockedSpace::delete_block(Block* b)
{
	if (!b) {
		return;
	}

	ASSERT(b->owner == this);

	auto ptr = b->alloc_pointer;
	b->~Block();
	mem_context->free(ptr);
}

Block*
BlockedSpace::new_block_at_end(size_t min_rows, int64_t numa)
{
	if (tail) {
		tail->validate();
	}
	if (!mem_context) {
		ASSERT(!alloc_mem_context);
		alloc_mem_context = std::make_unique<memory::Context>(nullptr,
			"blocked_space(" + dbg_name + ")", numa);
		mem_context = alloc_mem_context.get();
	}

	Block* b = new_block(tail, min_rows, numa);
	b->validate();

	LOG_TRACE("new_block_at_end: %p cap=%lld width=%lld",
		b->data, b->capacity, b->width);
	if (!head) {
		head = b;
	}
	if (tail) {
		tail->next = b;
	}
	tail = b;
	num_blocks++;

	return b;
}

void
partition_data(BlockedSpace** partitions, size_t num_partitions,
	uint64_t* hashes, size_t hash_stride, char* data, size_t width,
	size_t total_num, uint64_t* tmp_hashes, char** part_buf,
	size_t* part_num, Block** part_blk, size_t vsize,
	engine::Partitioner::runtime_partition_call_t runtime_partition,
	int64_t numa_node)
{
	auto prealloc = [&] (size_t num) {
		for (size_t i=0; i<num_partitions; i++) {
			Block* b = partitions[i]->prealloc(num, numa_node);
			b->debug_validate();

			part_blk[i] = b;
			part_buf[i] = b->data + (b->num * width);
			part_num[i] = 0;
		}
	};

	auto commit = [&] () {
		for (size_t i=0; i<num_partitions; i++) {
			partitions[i]->commit(part_blk[i], part_num[i]);
		}
	};

	LOG_TRACE("partition data %p total %lld\n", data, total_num);

	// partition
	for (size_t offset=0; offset < total_num; offset += vsize) {
		const size_t num = std::min(total_num - offset, vsize);

		// make sure everything fits & set buffers
		prealloc(num);

		// fetch hashes
		if (hash_stride) {
			engine::util_kernels::fetch<selvector_t, uint64_t>(tmp_hashes, 0,
				hash_stride,
				(char*)(hashes + offset*hash_stride),
				nullptr, num);
		} else {
			uint64_t null_hash = 0;
			engine::util_kernels::broadcast_typed<uint64_t>((char*)tmp_hashes,
				(char*)&null_hash, num);
		}

		// partition
		runtime_partition(part_num, part_buf, num_partitions, data, width,
			tmp_hashes, num, offset);

		// maintain counts
		commit();
	}
}

void
BlockedSpace::partition(BlockedSpace** partitions, size_t num_partitions,
	int64_t numa_node) const
{
	std::vector<uint64_t> tmp_hashes;
	tmp_hashes.resize(vector_size);

	std::vector<char*> part_buf;
	part_buf.resize(num_partitions);

	std::vector<size_t> part_num;
	part_num.resize(num_partitions);

	std::vector<Block*> part_blk;
	part_blk.resize(num_partitions);

	size_t num = 0;

	auto runtime_partition = engine::Partitioner::get_runtime_partition(
		num_partitions, width);
	ASSERT(runtime_partition);

	for_each([&] (auto block) {
		num++;
		char* blk_data = block->data;
		uint64_t* blk_hashes = (uint64_t*)(blk_data + hash_offset);
		partition_data(partitions, num_partitions, blk_hashes, hash_stride,
			blk_data, width, block->num,
			&tmp_hashes[0], &part_buf[0], &part_num[0], &part_blk[0],
			vector_size, runtime_partition, numa_node);
	});

	ASSERT(!num == !head);
}


BlockedSpace::~BlockedSpace()
{
	// dealloc space
	Block* b = head;
	size_t num = 0;
	while (b) {
		Block* next = b->next;
		num++;
		delete_block(b);
		b = next;
	}

	ASSERT(num == num_blocks);
	head = nullptr;
	tail = nullptr;

	if (init_columns) {
		mem_context->deleteObj(init_columns->bandit);
		mem_context->free(init_columns->init_columns);
		mem_context->deleteObj(init_columns);
	}
	alloc_mem_context.reset();
	mem_context = nullptr;
}
