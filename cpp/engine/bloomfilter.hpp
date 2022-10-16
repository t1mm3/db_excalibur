#pragma once

#include <stdint.h>
#include <string.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>

struct BudgetUser;

namespace memory {
struct Buffer;
}

namespace engine::table {
struct ITable;
struct Block;
}


namespace engine::table {

struct IBloomFilter {
	uint64_t* word_array_ptr;
	uint64_t num_words;
	uint64_t num_bits;
	uint64_t mask;
};

struct BloomFilter : IBloomFilter {
	constexpr static size_t kWordBits = sizeof(uint64_t)*8;
	constexpr static size_t kWordBytes = sizeof(uint64_t);

	constexpr static size_t kCellWordShift = 6;
	constexpr static size_t kCellBitMask = 63;

	constexpr static size_t kNumBits = 2;

	BloomFilter(size_t _num_bits, int64_t _par_partitons, ITable* _table);
	~BloomFilter();

	struct Inserter {
		virtual void operator()(const uint64_t* hashes, size_t num) = 0;
	};

	std::unique_ptr<Inserter> new_inserter(size_t vectorsize, bool parallel);

	bool begin_parallel_partition(int64_t& par_id, Block*& block, int64_t& offset);
	void end_parallel_partition(int64_t par_id);

	void add_failed_partition(int64_t par_id, Block* block, int64_t offset);


	void ensure_built(BudgetUser* budget_user);

	bool is_main_thread_waiting() const {
		return main_thread_waiting.load();
	}

private:
	bool is_fully_built_nolock() const;

	const int64_t par_build_max;
	std::atomic<int64_t> par_build_id = {0};
	mutable std::mutex mutex;
	std::condition_variable cond_built;

	std::atomic<int64_t> par_built_id = {0};

	std::unique_ptr<memory::Buffer> words_buffer;
	std::vector<std::tuple<int64_t, Block*, int64_t>> failed_partitions;
	ITable* table;

	std::atomic<bool> main_thread_waiting = { false };
};

} /* engine::table */ 
