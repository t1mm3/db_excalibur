#pragma once

#include <atomic>
#include <mutex>
#include <memory>
#include <vector>

#include "system/system.hpp"

#include "blocked_space.hpp"

struct Query;
struct BudgetUser;

namespace memory {
struct Buffer;
struct Context;
}

namespace engine {
struct Type;
struct Stream;

namespace voila {
struct ReadPosition;
}

namespace table {

struct TableLayout {
	size_t add_column(Type* type, bool init = false) {
		size_t idx = m_columns.size();
		m_columns.push_back(Col { type, -1, -1, init });
		m_recompute = true;
		if (init) {
			m_num_init++;
		}
		return idx;
	}


	size_t get_row_width() {
		ensure_computed();
		return m_row_width;
	}

	size_t get_num_columns() const {
		return m_columns.size();
	}

	size_t get_hash_column(bool& has_hash) {
		ensure_computed();
		has_hash = m_hash_index >= 0;
		return has_hash ? m_hash_index : 0;
	}

	size_t get_next_column(bool& has_next) {
		ensure_computed();
		has_next = m_next_index >= 0;
		return has_next ? m_next_index : 0;
	}

	size_t get_column_stride(size_t i) {
		ensure_computed();
		return m_columns[i].stride;
	}

	size_t get_column_offset(size_t i) {
		ensure_computed();
		return m_columns[i].offset;
	}

	Type* get_column_type(size_t i) {
		return m_columns[i].type;
	}

	size_t get_row_padding_at_end() {
		ensure_computed();
		return m_end_padding;
	}

	bool get_column_must_init(size_t i) const {
		return m_columns[i].init;
	}

	size_t get_num_init() const {
		return m_num_init;
	}

	void ensure_computed() {
		if (m_recompute) {
			compute();
		}
	}

	void print(int log_level);

	std::string get_signature(const std::string& col_sep = "_");

	template<typename T>
	void foreach_column(const T& f)
	{
		for (auto& col : m_columns) {
			f(col.type, col.offset, col.stride);
		}
	}

protected:
	void compute();

	struct Col {
		Type* type;
		int64_t offset;
		int64_t stride;
		bool init;
	};
	std::vector<Col> m_columns;
	bool m_recompute = true;

	size_t m_row_width = 0;
	size_t m_max_width = 0;

	int64_t m_hash_index = -1;
	int64_t m_next_index = -1;

	int64_t m_end_padding = -1;

	size_t m_num_init = 0;
};

struct ITable;
struct MasterTableReadPosition;
struct LocalTableReadPosition;
struct TableReinsertContext;

struct LogicalMasterTable {
	std::unique_ptr<voila::ReadPosition> makeReadPosition(engine::Stream& s);

	void add(ITable& table);

	LogicalMasterTable() {
		m_par_read_part = 0;
	}

private:
	friend struct MasterTableReadPosition;

	std::atomic<size_t> m_par_read_part;
	std::mutex mutex;
	std::vector<ITable*> tables;
	size_t max_partitions = 0;
};

struct BloomFilter;

struct ITable {
protected:
	friend struct LogicalMasterTable;
	friend struct MasterTableReadPosition;
	friend struct LocalTableReadPosition;

	const char* dbg_name;
	Query& query;
	const bool m_fully_thread_local;
	const bool m_flush_to_partitions;

	LogicalMasterTable* m_master_table;

	const size_t parallelism;
	std::vector<std::unique_ptr<BlockedSpace>> m_flush_partitions;

public:
	std::vector<std::unique_ptr<BlockedSpace>> m_write_partitions;

protected:
	mutable std::mutex mutex;

	std::atomic<size_t> m_par_build_part = {0};

	std::unique_ptr<BlockedSpace> new_blocked_space(
		memory::Context* given_mem_context,
		bool for_partitioning);

public:
	void** hash_index_head = nullptr;
	uint64_t hash_index_mask = 0;

	std::unique_ptr<BloomFilter> bloom_filter;

private:
	std::unique_ptr<memory::Buffer> hash_index_buffer;

	TableLayout layout;

	const bool with_hash_index;

	size_t hash_stride = 0;
	size_t hash_offset = 0;

	size_t next_stride = 0;
	size_t next_offset = 0;


	const size_t m_vector_size;
	const size_t m_morsel_size;
	static const size_t kMinBucketCount = 1024;

	const double m_load_factor;

	void create_hash_index_handle_space(void** buckets, uint64_t mask, 
		bool parallel, const BlockedSpace* space, TableReinsertContext& ctx);

	void create_hash_index(void** buckets, uint64_t mask);
	void remove_hash_index();

	template<typename T>
	void ForEachSpace(const T& f) {
		for (auto& write_part : m_write_partitions) {
			f(write_part);
		}
		for (auto& flush_part : m_flush_partitions) {
			f(flush_part);
		}
	}

	std::unique_ptr<memory::Context> m_flush_partitions_mem;

public:
	ITable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
		TableLayout&& layout, bool fully_thread_local, bool flush_to_part,
		bool with_hash_index);

	virtual ~ITable();

	void init();

	virtual void reset_pointers();

	TableLayout& get_layout() {
		return layout;
	}

	bool has_hash_index() const {
		return with_hash_index;
	}

	FORCE_INLINE inline void
	get_write_pos(size_t& out_offset, char*& out_data,
			size_t num, int64_t id, int64_t numa_node) {
		auto& space = m_write_partitions[id];

		Block* b = space->prealloc(num, numa_node);

		out_data = b->data;
		out_offset = b->num * b->width;

		LOG_TRACE("get_write_pos: tid=%llu data=%p begin=%p end=%p offwset=%lld num=%lld blk->num=%lld",
			id, out_data,
			out_data + (b->num * b->width),
			out_data + ((num + b->num) * b->width),
			out_offset, num, b->num);

		space->commit(b, num);
	}

	void** get_hash_index() const {
		return hash_index_head;
	}

	uint64_t get_hash_index_mask() const {
		return hash_index_mask;
	}

	size_t get_next_offset() const {
		return next_offset;
	}

	size_t get_next_stride() const {
		return next_stride;
	}

	bool build_index(bool force, int64_t numa_node);

	size_t get_non_flushed_table_count() const {
		size_t c = 0;
		for (const auto& write : m_write_partitions) {
			c += write->size();
		}
		return c;
	}

	size_t post_build_get_total_row_count_nolock() const {
		if (!post_build_get_total_row_count_cache) {
			post_build_get_total_row_count_cache = get_non_flushed_table_count();
		}

		return post_build_get_total_row_count_cache;
	}

	size_t post_build_get_total_row_count() const {
		std::unique_lock lock(mutex);

		return post_build_get_total_row_count_nolock();
	}

	void flush2partitions(size_t fanout, int64_t numa_node);

	std::unique_ptr<voila::ReadPosition> makeReadPosition(engine::Stream&);

	size_t get_row_width() {
		return layout.get_row_width();
	}

public:
	void build_bloom_filter(BudgetUser* budget_user, bool is_main_thread = false);

	size_t hash_index_capacity = 0; //!< #Buckets in 'hash_index_head'
	size_t hash_index_tuple_counter_seq = 0;

private:
	mutable size_t post_build_get_total_row_count_cache = 0;
};

struct IHashTable : ITable {
	IHashTable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
		TableLayout&& layout, bool fully_thread_local, bool flush_to_part,
		bool with_hash_index);

	static const size_t kFillFactorMul = 2;

	void potentially_rebuild(size_t num, int64_t numa_node) {
		const size_t count = hash_index_tuple_counter_seq;

		if (UNLIKELY(has_hash_index() && (count + num) * kFillFactorMul > hash_index_capacity)) {
			build_index(false, numa_node);
		}
	}


	Block* hash_append_prealloc(size_t num, int64_t numa_node) {
		// load factor > 50%?
		potentially_rebuild(num, numa_node);

		Block* b = m_write_partitions[0]->prealloc(num, numa_node);
		LOG_TRACE("prealloc:= n=%d\n", num);
		return b;
	}

	void hash_append_prune(Block* b, size_t n) {
		// proper count
		hash_index_tuple_counter_seq+=n;
		LOG_TRACE("prune:= b=%d +  n=%d\n", b->num, n);

		m_write_partitions[0]->commit(b, n);
	}

	void get_current_block_range(char** begin, char** end,
		size_t num);
};

} /* table */
} /* engine */