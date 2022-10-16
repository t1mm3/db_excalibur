#include "table.hpp"

#include "system/system.hpp"
#include "system/profiling.hpp"
#include "system/scheduler.hpp"
#include "system/memory.hpp"

#include "engine/voila/voila.hpp"
#include "engine/types.hpp"
#include "engine/query.hpp"
#include "engine/table_reader.hpp"
#include "engine/table_utils.hpp"
#include "engine/util_kernels.hpp"
#include "engine/bloomfilter.hpp"
#include "engine/budget.hpp"

#include <cstring>
#include <sstream>

using namespace engine::table;

void
TableLayout::compute()
{
	ASSERT(m_recompute);

	m_row_width = 0;
	m_max_width = 0;
	m_hash_index = -1;
	m_next_index = -1;
	m_end_padding = -1;

	LOG_DEBUG("TableLayout: %llu columns", m_columns.size());
	size_t i = 0;
	for (auto& col : m_columns) {
		const auto& type = col.type;
		ASSERT(type);
		const auto width = col.type->get_width();
		ASSERT(width > 0);

		// pad to make self aligned
		size_t col_padding = (((m_row_width + width-1) / width) * width) - m_row_width;
		col.offset = m_row_width + col_padding;
		ASSERT(col.offset % width == 0 && "Must be self-aligned (as in C)");

		if (type->is_hash()) {
			ASSERT(m_hash_index < 0 && "Only one hash allowed");
			m_hash_index = i;
		} else if (type->is_bucket()) {
			ASSERT(m_next_index < 0 && "Only one next allowed");
			m_next_index = i;
		}

		m_row_width += col_padding;
		m_row_width += width;
		m_max_width = std::max(m_max_width, width);
		i++;
	}

	// make sure row_width is divisible by max_width
	ASSERT(m_row_width > 0);
	auto old_row_width = m_row_width;
	m_row_width = ((old_row_width + m_max_width-1)/m_max_width)*m_max_width;
	m_end_padding = m_row_width - old_row_width;
	ASSERT(m_row_width > 0);

	// compute strides
	for (auto& col : m_columns) {
		const auto width = col.type->get_width();
		DBG_ASSERT(m_row_width % width == 0);
		col.stride = m_row_width / width;

		LOG_DEBUG("Column type=%s, width=%llu, offset=%llu, stride=%llu, init=%d",
			col.type->to_cstring(), width, col.offset, col.stride, col.init);
	}


	LOG_DEBUG("TableLayout: m_row_width=%llu, m_end_padding=%llu, old_row_width=%llu",
		m_row_width, m_end_padding, old_row_width);
	m_recompute = false;
}

void
TableLayout::print(int l)
{
	if (m_recompute) {
		compute();
	}

	if (!LOG_WILL_LOG(l)) {
		return;
	}
	LOG_MESSAGE(l, "TableLayout: ");
	int64_t i = 0;
	for (auto& col : m_columns) {
		LOG_MESSAGE(l, " column[%lld]: type=%s, offset=%lld, stride=%lld",
			i, col.type->to_cstring(), col.offset, col.stride);

		i++;
	}
}

std::string
TableLayout::get_signature(const std::string& col_sep)
{
	std::ostringstream ss;

	size_t i = 0;
	for (auto& col : m_columns) {
		if (i > 0) {
			ss << col_sep;
		}

		ss << col.type->to_string();
		i++;
	}

	return ss.str();
}

#if 1
inline static uint64_t
next_power_2(uint64_t x)
{
	uint64_t power = 1;
	while (power < x) {
    	power*=2;
	}
	return power;
}

inline static
size_t calc_num_buckets(size_t count, size_t vector_size,
	size_t min_bucket_count, double load_factor, size_t mul = 1)
{
	size_t bucket_count = count * mul;
	auto bmax = [&] (const auto& v) {
		if (v > bucket_count) {
			bucket_count = v;
		}
	};

	bmax(min_bucket_count);

	const auto loadFactor = load_factor;
	size_t exp = 64 - __builtin_clzll(bucket_count);
	ASSERT(exp < sizeof(uint64_t) * 8);
	if (((size_t) 1 << exp) < bucket_count / loadFactor)
		exp++;
	size_t capacity = ((size_t) 1) << exp;

	return capacity;
}

void
LogicalMasterTable::add(ITable& table)
{
	std::lock_guard<std::mutex> lock(mutex);

	max_partitions = std::max(max_partitions, table.parallelism);

	tables.push_back(&table);
	LOG_TRACE("LogicalMasterTable: adding table %p num_tables(post) %lld",
		&table, tables.size());
}

std::unique_ptr<engine::voila::ReadPosition>
LogicalMasterTable::makeReadPosition(Stream& stream)
{
	return std::make_unique<MasterTableReadPosition>(*this, stream);
}




















ITable::ITable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
	TableLayout&& layout, bool fully_thread_local, bool flush_to_part,
	bool with_hash_index)
 : dbg_name(dbg_name), query(q), m_fully_thread_local(fully_thread_local),
 		m_flush_to_partitions(flush_to_part),
 		m_master_table(master_table),
 		parallelism(q.config->parallelism()), layout(layout),
 		with_hash_index(with_hash_index),
 		m_vector_size(q.config->vector_size()),
 		m_morsel_size(q.config->morsel_size()),
 		m_load_factor(q.config->table_load_factor())
{
	LOG_TRACE("ITable: Construct %p", this);

	m_par_build_part = 0;

	if (LOG_WILL_LOG(TRACE)) {
		layout.print(TRACE);
	}

	size_t i;
	bool has = false;
	size_t num_cols = layout.get_num_columns();

	i = layout.get_hash_column(has);
	if (has) {
		ASSERT(i < num_cols);

		hash_stride = layout.get_column_stride(i);
		hash_offset = layout.get_column_offset(i);
	} else {
		hash_stride = 0;
		hash_offset = 0;
	}


	i = layout.get_next_column(has);
	if (has) {
		ASSERT(i < num_cols);

		next_stride = layout.get_column_stride(i);
		next_offset = layout.get_column_offset(i);
	} else {
		next_stride = 0;
		next_offset = 0;
	}


	size_t num_write_parts = m_fully_thread_local ? 1 : g_scheduler.get_num_threads();

	for (size_t t=0; t<num_write_parts; t++) {
		m_write_partitions.emplace_back(new_blocked_space(nullptr, false));
	}

	remove_hash_index();

	if (m_master_table) {
		m_master_table->add(*this);
	}

	if (with_hash_index) {
		build_index(true, -1);
		ASSERT(hash_index_head);
	}
}

std::unique_ptr<BlockedSpace>
ITable::new_blocked_space(memory::Context* given_mem_context,
	bool for_partitioning)
{
	BlockedSpace::BlockedSpaceFlags flags = 0;
	if (for_partitioning) {
		flags |= BlockedSpace::kTight | BlockedSpace::kNoInit;
	}
	auto p = std::make_unique<BlockedSpace>(get_row_width(),
		m_vector_size, m_morsel_size, layout, flags, given_mem_context,
		std::string(dbg_name));
	if (next_stride > 0) {
		p->set_next(next_stride, next_offset);
	}
	if (hash_stride > 0) {
		p->set_hash(hash_stride, hash_offset);
	}
	return p;
}

ITable::~ITable()
{
	LOG_TRACE("ITable: Destruct %p", this);
	remove_hash_index();

	if (!m_write_partitions.empty()) {
		LOG_DEBUG("~ITable: free m_write_partitions");
		m_write_partitions.clear();
	}

	// must be deallocated before it's Context
	if (!m_flush_partitions.empty()) {
		LOG_DEBUG("~ITable: free m_flush_partitions");
		m_flush_partitions.clear();
		LOG_DEBUG("~ITable: free m_flush_partitions_mem");
		ASSERT(m_flush_partitions_mem);
		m_flush_partitions_mem.reset();
	} else {
		ASSERT(!m_flush_partitions_mem);
	}

}

void
ITable::init()
{
	reset_pointers();
}

void
ITable::reset_pointers()
{
	ASSERT(false && "overwritten");
}


static void
create_hash_index_for_block(void** buckets, uint64_t mask, const Block& block,
	size_t hash_offset, size_t hash_stride, size_t next_offset, size_t next_stride,
	bool parallel, TableReinsertContext& ctx, TableUtils::reinsert_call_t reinserter)
{
	char* blk_data = block.data;
	const size_t num = block.num;
	const size_t offset = 0;
	const size_t width = block.width;

	uint64_t* blk_hashes = (uint64_t*)(blk_data + hash_offset);
	void** blk_nexts = (void**)(blk_data + next_offset);

	LOG_TRACE("create_hash_index_for_block: buckets=%p hashes=%p nexts=%p num=%lld "
		"offset=%lld width=%lld\n",
		buckets, blk_hashes, blk_nexts, num, offset, width);

	static_assert(sizeof(uint64_t) == sizeof(void*), "64-bit pointers required");

	reinserter(ctx, buckets, mask, blk_hashes, hash_stride,
			blk_nexts, next_stride, parallel, blk_data, width,
			offset, num);
}

void
ITable::create_hash_index_handle_space(void** buckets, uint64_t mask,
	bool parallel, const BlockedSpace* space, TableReinsertContext& ctx)
{
	TableUtils::reinsert_call_t reinserter = nullptr;
	size_t last_width = 0;

	space->for_each([&] (Block* block) {
		LOG_TRACE("create_hash_index_for_block: %s "
			"space=%p blk=%p blk_base=%p num=%lld width=%lld",
			parallel ? "PARALLEL" : "SEQUENTIAL",
			space, block, block->data, block->num, block->width);

		block->validate();

		if (reinserter) {
			ASSERT(block->width == last_width);
		} else {
			last_width = block->width;
			reinserter = TableUtils::get_reinsert(parallel, hash_stride, next_stride,
				last_width);
			ASSERT(reinserter);
		}


		create_hash_index_for_block(buckets, mask, *block,
			hash_offset, hash_stride, next_offset, next_stride,
			parallel, ctx, reinserter);
	});
}

struct DynamicAllocReinsertContext : TableReinsertContext {
	void alloc(size_t sz) {
		if (sz == tmp_size) {
			return;
		}

		tmp_size = sz;

		a_chk_ptr.resize(sz);
		a_chk_next.resize(sz);
		a_chk_b.resize(sz);
		a_chk_old.resize(sz);
		a_chk_ok.resize(sz);

		tmp_ptr = &a_chk_ptr[0];
		tmp_next = &a_chk_next[0];
		tmp_b = &a_chk_b[0];
		tmp_old = &a_chk_old[0];
		tmp_ok = &a_chk_ok[0];
	}

private:
	std::vector<void**> a_chk_ptr;
	std::vector<void**> a_chk_next;
	std::vector<void*> a_chk_b;
	std::vector<void*> a_chk_old;
	std::vector<char> a_chk_ok;
};


static constexpr size_t kChunkSize = 1024;

void
ITable::create_hash_index(void** buckets, uint64_t mask)
{
	LOG_TRACE("create_hash_index: mask=%p, hash_stride=%llu, hash_offset=%llu, "
		"next_stride=%llu, next_offset=%llu, row_width=%llu",
		mask, hash_stride, hash_offset,
		next_stride, next_offset, get_row_width());
	ASSERT(hash_stride > 0 && hash_offset > 0);
	ASSERT(next_stride > 0 && next_offset > 0);

	DynamicAllocReinsertContext ctx;

	bool parallel = !m_fully_thread_local;
	if (parallel) {
		while (1) {
			auto tid = m_par_build_part.fetch_add(1);
			if (tid >= m_write_partitions.size()) {
				break;
			}
			ASSERT(tid >= 0);

			ctx.alloc(kChunkSize);
			create_hash_index_handle_space(buckets, mask, 
				parallel, m_write_partitions[tid].get(), ctx);
		}
	} else {
		ctx.alloc(kChunkSize);
		ForEachSpace([&] (auto& space) {
			create_hash_index_handle_space(buckets, mask,
				parallel, space.get(), ctx);
		});
	}
}

void
ITable::remove_hash_index()
{
	if (!hash_index_head) {
		return;
	}

	LOG_DEBUG("remove_hash_index(%p, %s, %p)",
		this, dbg_name, hash_index_buffer.get());
	hash_index_head = nullptr;

	hash_index_mask = 0;
	hash_index_capacity = 0;
	hash_index_tuple_counter_seq = 0;
	hash_index_buffer.reset();
}

bool
ITable::build_index(bool force, int64_t numa_node)
{
	const size_t count = get_non_flushed_table_count();
	const size_t new_num_buckets = calc_num_buckets(count, m_vector_size,
		kMinBucketCount, m_load_factor);
	const uint64_t mask = new_num_buckets - 1;

	// try to not rebuild needlessly
	if (!force) {
		if (hash_index_head && mask == hash_index_mask) {
			// re-use hash index
			return false;
		}
	}


	{
		std::lock_guard<std::mutex> lock(mutex);
		if (!hash_index_head || mask != hash_index_mask) {
			// throw away old hash index
			remove_hash_index();

			LOG_DEBUG("build_index(%p, %s, %p): count %d",
				this, dbg_name, hash_index_buffer.get(),
				count);

			// create new hash index
			hash_index_mask = mask;
			hash_index_capacity = new_num_buckets;
			ASSERT(new_num_buckets > 0);

			size_t bytes = sizeof(void*) * new_num_buckets;
			ASSERT(bytes > 0);

			uint64_t buffer_flags = memory::Buffer::kZero;

			if (!m_fully_thread_local && parallelism > 1) {
				buffer_flags |= memory::Buffer::kNumaInterleaved;
			}

			hash_index_buffer = std::make_unique<memory::Buffer>(bytes, buffer_flags);
			if (!hash_index_buffer) {
				LOG_ERROR("Could not allocate %llu bytes (%llu GiB) for hash index",
					bytes, bytes/1024/1024/1024);
				throw std::bad_alloc();
			}
			hash_index_head = hash_index_buffer->get_as<void*>();
			ASSERT(hash_index_head);
		}
	}

	create_hash_index(hash_index_head, hash_index_mask);

	if (LOG_WILL_LOG(DEBUG)) {
		auto buckets = (uint64_t*)hash_index_head;
		size_t n = 0;
		for (uint64_t i=0; i<=hash_index_mask; i++) {
			if (buckets[i]) {
				LOG_TRACE("HASHTABLE[%llu] = %p", i, buckets[i]);
				n++;
			}
		}
		LOG_DEBUG("HASHTABLE: num %llu", n);
	}

	return true;
}

void
ITable::build_bloom_filter(BudgetUser* budget_user, bool is_main_thread)
{
	{
		std::lock_guard<std::mutex> lock(mutex);
		if (!bloom_filter) {
			bloom_filter = std::make_unique<BloomFilter>(
				post_build_get_total_row_count_nolock() * 4,
				m_write_partitions.size(), this);
		}
	}

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "build_bloom_filter");
	}


	bool parallel = m_write_partitions.size() > 1;

	const int64_t kVectorSize = 1024;

	std::vector<uint64_t> tmp_hashes;
	tmp_hashes.resize(kVectorSize);
	const int64_t kInvalidPartition = -1;

	int64_t partition_id = kInvalidPartition;
	Block* partition_block = nullptr;
	int64_t partition_offset = 0;

	auto inserter = bloom_filter->new_inserter(kVectorSize, parallel);

	try {
		while (1) {
			partition_id = kInvalidPartition;
			partition_block = nullptr;
			partition_offset = 0;

			if (!is_main_thread && bloom_filter->is_main_thread_waiting()) {
				// cancel this, main thread is waiting and will pick up our work
				LOG_DEBUG("ITable::build_bloom_filter: Main thread is waiting ... abort");
				throw OutOfBudgetException("ITable::build_bloom_filter: Main thread is waiting");
			}

			if (!bloom_filter->begin_parallel_partition(partition_id, partition_block,
					partition_offset)) {
				partition_id = kInvalidPartition;
				break;
			}
			ASSERT(partition_id >= 0 && partition_id <= (int64_t)m_write_partitions.size());
			ASSERT(partition_offset >= 0);

			auto space = m_write_partitions[partition_id].get();
			space->for_each([&] (Block* block) {
				LOG_DEBUG("build_bloom_filter: %s "
					"space=%p blk=%p blk_base=%p num=%lld width=%lld partition_offset?%llu",
					parallel ? "PARALLEL" : "SEQUENTIAL",
					space, block, block->data, block->num, block->width, partition_offset);

				partition_block = block;

				const int64_t block_num = block->num;
				ASSERT(partition_offset <= block_num);

				while (partition_offset<block_num) {
					int64_t num = std::min(block_num-partition_offset, kVectorSize);
					ASSERT(num <= (int64_t)tmp_hashes.size());
					ASSERT(num >= 0);

					if (budget_user) {
						budget_user->ensure_sufficient(kDefaultBudget,
							"build_bloom_filter");
					}

					if (!is_main_thread && bloom_filter->is_main_thread_waiting()) {
						// cancel this, main thread is waiting and will pick up our work
						LOG_DEBUG("ITable::build_bloom_filter: Main thread is waiting ... abort");
						throw OutOfBudgetException("ITable::build_bloom_filter: Main thread is waiting");
					}

					util_kernels::fetch<int, uint64_t>(&tmp_hashes[0], 0, hash_stride,
						block->data + hash_offset +
							partition_offset * hash_stride * sizeof(uint64_t),
						(int*)nullptr, num);
					(*inserter)(&tmp_hashes[0], num);

					partition_offset += num;
				}

				partition_offset = 0;
			}, partition_block);

			bloom_filter->end_parallel_partition(partition_id);
		}
	} catch (...) {
		// continue next time
		if (partition_id != kInvalidPartition) {
			ASSERT(partition_id >= 0 && partition_id <= (int64_t)m_write_partitions.size());
			bloom_filter->add_failed_partition(partition_id, partition_block,
				partition_offset);
		}
		throw;
	}
}

void
ITable::flush2partitions(size_t fanout, int64_t numa_node)
{
	ASSERT(m_write_partitions.size() == 1);
	ASSERT(m_master_table);

	if (!m_flush_partitions.size()) {
		if (!m_flush_partitions_mem) {
			m_flush_partitions_mem = std::make_unique<memory::Context>(nullptr,
				"flush_partitions_mem(" + std::string(dbg_name) + ")", numa_node);
		}

		ASSERT(m_flush_partitions_mem);

		for (size_t i=0; i<fanout; i++) {
			m_flush_partitions.emplace_back(
				new_blocked_space(m_flush_partitions_mem.get(), true));
		}
	}

	ASSERT(m_flush_partitions_mem);

	std::vector<BlockedSpace*> partitions;
	partitions.reserve(m_flush_partitions.size());
	for (auto& f : m_flush_partitions) {
		partitions.push_back(f.get());
	}

	// flush
	auto& bspace = m_write_partitions[0];
	bspace->partition(&partitions[0], m_flush_partitions.size(), numa_node);

	// remove flushed tuples from table
	bspace.reset();

	// delete hash index
	if (hash_index_head) {
		remove_hash_index();
	}
}


std::unique_ptr<engine::voila::ReadPosition>
ITable::makeReadPosition(Stream& stream)
{
	if (m_master_table) {
		ASSERT(m_flush_partitions.size() > 0);
		return m_master_table->makeReadPosition(stream);
	}

	return std::make_unique<LocalTableReadPosition>(*this);
}

IHashTable::IHashTable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
	TableLayout&& layout, bool fully_thread_local, bool flush_to_part, bool with_hash_index)
 : ITable(dbg_name, q, master_table, std::move(layout), fully_thread_local, flush_to_part,
 	with_hash_index)
{
}

void
IHashTable::get_current_block_range(char** out_begin, char** out_end,
		size_t num) {
	const size_t width = get_row_width();

	ASSERT(m_fully_thread_local);
	ASSERT(m_write_partitions.size() == 1);
	auto part = m_write_partitions[0].get();
	Block* block = part->current();

	ASSERT(block && block->data);

	size_t offset = block->num;

	char* begin = block->data + (offset*width);
	char* end = begin + (num*width);

	ASSERT(offset + num <= block->capacity);

	*out_begin = begin;
	*out_end = end;
}

#endif