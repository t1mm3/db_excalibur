#include "bloomfilter.hpp"

#include "table.hpp"

#include "system/system.hpp"
#include "system/memory.hpp"
#include "system/profiling.hpp"

#include "engine/bandit_utils.hpp"

#include <algorithm>

using namespace engine::table;

#ifdef __AVX512F__
#include <immintrin.h>
#endif


#define PREFETCH_WRITE(ptr) __builtin_prefetch(ptr, 1, 3);

inline static uint64_t
next_power_2(uint64_t x)
{
	uint64_t power = 1;
	while (power < x) {
    	power*=2;
	}
	return power;
}

template<int AVX512_FLAG>
static void
_bf_insert1_precompute(const uint64_t* RESTRICT hashes, size_t num,
	size_t word_mask, uint64_t* RESTRICT tmp_idx,
	uint64_t* RESTRICT tmp_mask)
{
	ASSERT(tmp_mask && tmp_mask != tmp_idx);

#define A(i) { \
		uint64_t tmp = hashes[i]; \
		size_t shift0 = tmp; \
		size_t bidx0 = shift0 & BloomFilter::kCellBitMask; \
		uint64_t bitmask = 1ull << bidx0; \
		if (BloomFilter::kNumBits == 2) { \
			size_t shift1 = tmp >> BloomFilter::kCellWordShift; \
			size_t bidx1 = shift1 & BloomFilter::kCellBitMask; \
			bitmask |= 1ull << bidx1; \
		} \
		size_t shift2 = tmp >> (2*BloomFilter::kCellWordShift); \
		tmp_idx[i] = shift2 & word_mask; \
		tmp_mask[i] = bitmask; \
	}

	size_t k=0;

#ifdef __AVX512F__
	if (AVX512_FLAG == 1) {
		auto cellmask = _mm512_set1_epi64(BloomFilter::kCellBitMask);
		auto wordmask = _mm512_set1_epi64(word_mask);
		auto one = _mm512_set1_epi64(1);
		for (; k+8<num; k+=8) {
			auto tmp = _mm512_loadu_si512(hashes+k);

			auto shift0 = tmp;
			auto bidx0 = _mm512_and_epi64(shift0, cellmask);
			auto bitmask = _mm512_sllv_epi64(one, bidx0);

			if (BloomFilter::kNumBits == 2) {
				auto shift1 = _mm512_srli_epi64(tmp, BloomFilter::kCellWordShift);
				auto bidx1 = _mm512_and_epi64(shift1, cellmask);
				bitmask = _mm512_or_epi64(
					bitmask, _mm512_sllv_epi64(one, bidx1));
			}

			_mm512_storeu_si512(tmp_mask + k, bitmask);

			auto shift2 = _mm512_srli_epi64(tmp, 2*BloomFilter::kCellWordShift);

			_mm512_storeu_si512(tmp_idx + k, _mm512_and_epi64(shift2, wordmask));
		}
	}
#endif

	for (; k<num; k++) {
		A(k);
	}

#undef A
}


template<bool PARALLEL, int FLAVOR, int PREFETCH = 0>
NOINLINE void
bf_insert(uint64_t* RESTRICT words, size_t num_bits,
	const uint64_t* RESTRICT hashes, size_t num, bool parallel,
	size_t word_mask, uint64_t* RESTRICT tmp_idx,
	uint64_t* RESTRICT tmp_mask)
{
	size_t i;

	switch (FLAVOR) {
	case 1:
		_bf_insert1_precompute<FLAVOR>(hashes, num, word_mask, tmp_idx, tmp_mask);

		i = 0;
		if (PREFETCH) {
			for (; i+PREFETCH<num; i++) {
				PREFETCH_WRITE(&words[tmp_idx[i+PREFETCH]]);
				auto w = &words[tmp_idx[i]];
				auto bitmask = tmp_mask[i];
				if (PARALLEL) {
					__sync_fetch_and_or(w, bitmask);
				} else {
					*w |= bitmask;
				}
			}
		}

		for (; i<num; i++) {
			auto w = &words[tmp_idx[i]];
			auto bitmask = tmp_mask[i];
			if (PARALLEL) {
				__sync_fetch_and_or(w, bitmask);
			} else {
				*w |= bitmask;
			}
		}
		break;

	default:
		i = 0;

		if (PREFETCH) {
			for (; i+PREFETCH<num; i++) {
				{
					size_t s = hashes[i+PREFETCH] >> (2*BloomFilter::kCellWordShift);
					PREFETCH_WRITE(&words[s & word_mask]);
				}

				uint64_t tmp = hashes[i];

				size_t shift0 = tmp;
				size_t bidx0 = shift0 & BloomFilter::kCellBitMask;
				uint64_t bitmask = 1ull << bidx0;

				if (BloomFilter::kNumBits == 2) {
					size_t shift1 = tmp >> BloomFilter::kCellWordShift;
					size_t bidx1 = shift1 & BloomFilter::kCellBitMask;
					bitmask |= 1ull << bidx1;
				}

				size_t shift2 = tmp >> (2*BloomFilter::kCellWordShift);

				auto w = &words[shift2 & word_mask];

				if (PARALLEL) {
					__sync_fetch_and_or(w, bitmask);
				} else {
					*w |= bitmask;
				}
			}
		}

		for (; i<num; i++) {
			uint64_t tmp = hashes[i];

			size_t shift0 = tmp;
			size_t bidx0 = shift0 & BloomFilter::kCellBitMask;
			uint64_t bitmask = 1ull << bidx0;

			if (BloomFilter::kNumBits == 2) {
				size_t shift1 = tmp >> BloomFilter::kCellWordShift;
				size_t bidx1 = shift1 & BloomFilter::kCellBitMask;
				bitmask |= 1ull << bidx1;
			}

			size_t shift2 = tmp >> (2*BloomFilter::kCellWordShift);

			auto w = &words[shift2 & word_mask];
			if (PARALLEL) {
				__sync_fetch_and_or(w, bitmask);
			} else {
				*w |= bitmask;
			}
		}
		break;
	}
}

typedef void (*insert_call_t)(uint64_t* words, size_t num_bits,
	const uint64_t* hashes, size_t num, bool parallel, size_t word_mask,
	uint64_t* tmp_idx, uint64_t* tmp_mask);

static const insert_call_t flavor_lut[] = {
	bf_insert<true, 0, 0>,
	bf_insert<true, 1, 0>,
	bf_insert<true, 0, 64>,
	bf_insert<true, 1, 64>,
	bf_insert<true, 0, 32>,
	bf_insert<true, 1, 32>,
};


struct BFInserter : BloomFilter::Inserter {
	static constexpr size_t num_flavors = sizeof(flavor_lut) / sizeof(flavor_lut[0]); 

	BanditStaticArms<num_flavors> bandit;

	void operator()(const uint64_t* hashes, size_t num) final
	{
		auto arm = bandit.choose_arm();
		auto fun = flavor_lut[arm];

		auto prof_start = profiling::physical_rdtsc();
		fun(words, num_bits, hashes, num, parallel, word_mask,
			&tmp_idx[0], &tmp_msk[0]);
		auto prof_end = profiling::physical_rdtsc();

		double diff = prof_end-prof_start;
		bandit.record(arm, -diff, num);
	}

	BFInserter(size_t vectorsize, bool parallel, uint64_t* _words, size_t word_mask,
		size_t num_bits)
	 : vectorsize(vectorsize), parallel(parallel), words(_words),
		word_mask(word_mask), num_bits(num_bits)
	{
		tmp_idx.resize(vectorsize);
		tmp_msk.resize(vectorsize);
	}

	const size_t vectorsize;
	const bool parallel;
	uint64_t* words;
	const size_t word_mask;
	const size_t num_bits;

	std::vector<uint64_t> tmp_idx;
	std::vector<uint64_t> tmp_msk;
};

std::unique_ptr<BloomFilter::Inserter>
BloomFilter::new_inserter(size_t vectorsize, bool parallel)
{
	return std::make_unique<BFInserter>(vectorsize, parallel, word_array_ptr,
		mask, num_bits);
}


BloomFilter::BloomFilter(size_t _num_bits, int64_t _par_partitons, ITable* _table)
 : par_build_max(_par_partitons), table(_table)
{
	num_bits = _num_bits;

	num_bits = std::max<size_t>(num_bits, (size_t)(64*64));
	num_bits = ((num_bits + kWordBits-1) / kWordBits) * kWordBits;
	ASSERT((num_bits % kWordBits) == 0);


	num_words = num_bits / kWordBits;
	num_words = next_power_2(num_words);
	ASSERT(num_words == next_power_2(num_words));

	uint64_t buffer_flags = memory::Buffer::kZero;
	buffer_flags |= memory::Buffer::kNumaInterleaved;

	words_buffer = std::make_unique<memory::Buffer>(num_words * kWordBytes,
		buffer_flags);
	word_array_ptr = words_buffer->get_as<uint64_t>();

	mask = num_words-1;
}

BloomFilter::~BloomFilter()
{

}

bool
BloomFilter::begin_parallel_partition(int64_t& par_id, Block*& block, int64_t& offset)
{
	block = nullptr;
	par_id = par_build_id.fetch_add(1);
	offset = 0;

	if (par_id < par_build_max) {
		LOG_TRACE("begin_parallel_partition: atomic %lld", par_id);
		return true;
	}

	// later continue failed partitions
	std::unique_lock lock(mutex);

	if (failed_partitions.empty()) {
		LOG_TRACE("begin_parallel_partition: done");
		return false;
	}

	auto& pair = failed_partitions.back();
	par_id = std::get<0>(pair);
	block = std::get<1>(pair);
	offset = std::get<2>(pair);
	failed_partitions.pop_back();

	LOG_TRACE("begin_parallel_partition(%p, %lld, %p, %lld)",
		this, par_id, block, offset);

	return true;
}

void
BloomFilter::end_parallel_partition(int64_t par_id)
{
	auto num_built = par_built_id.fetch_add(1)+1;

	LOG_TRACE("end_parallel_partition");

	if (num_built >= par_build_max) {
		ASSERT(num_built == par_build_max);

		// trigger wakeup
		ASSERT(is_fully_built_nolock());
		std::unique_lock lock(mutex);
		cond_built.notify_all();

		LOG_TRACE("end_parallel_partition(%p): trigger wakeup", this);
	}
}

void
BloomFilter::add_failed_partition(int64_t par_id, Block* block, int64_t offset)
{
	LOG_TRACE("add_failed_partition(%lld, %p, %lld)", par_id, block, offset);
	std::unique_lock lock(mutex);

	ASSERT(!is_fully_built_nolock());
	failed_partitions.push_back({ par_id, block, offset });

	cond_built.notify_all();
}

bool
BloomFilter::is_fully_built_nolock() const
{
	auto a = par_built_id.load();
	LOG_TRACE("is_fully_built: a=%lld, max=%lld, b=%lld", a, par_build_max, par_build_id.load());
	return a >= par_build_max;
}

void
BloomFilter::ensure_built(BudgetUser* budget_user)
{
	if (!table) {
		return;
	}

	while (1) {
		LOG_TRACE("ensure_built(%p): build remaining ...", this);
		table->build_bloom_filter(budget_user, true);

		// wait until fully built
		std::unique_lock lock(mutex);

		LOG_TRACE("ensure_built(%p): %llu failed one",
			this, failed_partitions.size());

		cond_built.wait(lock, [&] {
			return is_fully_built_nolock() || !failed_partitions.empty(); });

		if (is_fully_built_nolock()) {
			main_thread_waiting = false;
			break;
		}

		// other threads muts have failed building partitions
		// cancel them and try to pick faile partitions up
		main_thread_waiting.store(true);
	}

	LOG_TRACE("ensure_built(%p): done", this);
}
