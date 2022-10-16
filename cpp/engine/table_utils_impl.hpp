#pragma once

#include "table_utils.hpp"

#include <atomic>
#include <cmath>
#include <limits>
#include "system/profiling.hpp"

#include "bandit_utils.hpp"

namespace engine::table {

template<size_t NUM_ARMS>
struct BanditWrapper : TableReinsertContextExtraData {
	BanditStaticArms<NUM_ARMS> bandit;

	BanditWrapper(double c = 2.0/3.0)
	 : bandit(c)
	{
	}
};


using reinsert_call_t = TableUtils::reinsert_call_t;

#define PREFETCH_WRITE(ptr) __builtin_prefetch(ptr, 1, 3);

template<bool PARALLEL, size_t NEXT_STRIDE, size_t HASH_STRIDE, size_t WIDTH,
	int USE_PREFETCH>
static NOINLINE void
__reinsert_hash_buckets(void** RESTRICT buckets,
	uint64_t* RESTRICT hashs, size_t _hash_stride,
	void** RESTRICT nexts, size_t _next_stride,
	uint64_t mask, size_t offset, char* RESTRICT base_ptr, size_t _width,
	size_t num, size_t start, void*** RESTRICT tmp_ptr,
	void*** RESTRICT tmp_next, void** RESTRICT tmp_b,
	void** RESTRICT tmp_old, char* RESTRICT tmp_ok)
{
	uint64_t _k=start;

	(void)tmp_ptr;
	(void)tmp_next;
	(void)tmp_b;
	(void)tmp_old;
	(void)tmp_ok;

	auto par_insert_pre = [&] (void** next, void* b, void** ptr, void* old) -> bool {
		DBG_ASSERT(old != b);
		*next = old;
#if 1
		auto atomic = (std::atomic<void*>*)ptr;
		return atomic->compare_exchange_weak(old, b,
			std::memory_order_relaxed, std::memory_order_relaxed);
#else
		return __sync_bool_compare_and_swap(ptr, old, b);
#endif
	};

	auto par_insert_full = [&] (void** next, void* b, void** ptr) {
		while (!par_insert_pre(next, b, ptr, *ptr)) {
#if defined(__x86_64__) || defined(__i386__)
			__builtin_ia32_pause();
#else
#endif
		}
	};


	auto seq_insert = [&] (void** next, auto b, void** ptr) {
		DBG_ASSERT(*ptr != b);
		*next = *ptr;
		*ptr = b;
	};

	const size_t next_stride = NEXT_STRIDE ? NEXT_STRIDE : _next_stride;
	const size_t hash_stride = HASH_STRIDE ? HASH_STRIDE : _hash_stride;
	const size_t width = WIDTH ? WIDTH : _width;

	ASSERT(next_stride == _next_stride);
	ASSERT(hash_stride == _hash_stride);
	ASSERT(width == _width);

#define PROLOGUE2(pos) \
		uint64_t k##pos=pos+_k; \
		uint64_t i##pos = offset+k##pos; \
		uint64_t idx##pos; \
		void* RESTRICT b##pos; \
		void** RESTRICT ptr##pos; \
		void** RESTRICT next##pos; \
		uint64_t h##pos = hashs[i##pos * hash_stride]; \
		idx##pos = h##pos & mask; \
		b##pos = base_ptr + (i##pos * width); \
		ptr##pos = &buckets[idx##pos]; \
		next##pos = &nexts[i##pos * next_stride];

#define A(pos) \
		if (PARALLEL) { \
			par_insert_full(next##pos, b##pos, ptr##pos); \
		} else { \
			seq_insert(next##pos, b##pos, ptr##pos); \
		}

#if 1
	switch (USE_PREFETCH) {
	case 8:
		for (; _k+8<num; _k+=8) {
			PROLOGUE2(0)
			PROLOGUE2(1)
			PROLOGUE2(2)
			PROLOGUE2(3)

			PREFETCH_WRITE(ptr0)
			PREFETCH_WRITE(ptr1)
			PREFETCH_WRITE(ptr2)
			PREFETCH_WRITE(ptr3)

			PROLOGUE2(4)
			PROLOGUE2(5)
			PROLOGUE2(6)
			PROLOGUE2(7)

			PREFETCH_WRITE(ptr4)
			PREFETCH_WRITE(ptr5)
			PREFETCH_WRITE(ptr6)
			PREFETCH_WRITE(ptr7)

			A(0)
			A(1)
			A(2)
			A(3)

			A(4)
			A(5)
			A(6)
			A(7)
		}
		break;

	// distance-based prefetch
	case -16:
	case -32:
	case -40:
	case -48:
	case -56:
	case -64:
		{
			size_t dist = -USE_PREFETCH;

			size_t real_num = num - start;
			size_t i=0;

			// preprocess
			if (real_num > 0) {
				// do first item manually and prefetch
				_k = start+i;
				PROLOGUE2(0);

				(void)idx0;
				(void)next0;
				(void)b0;

				tmp_ptr[i] = ptr0;
				PREFETCH_WRITE(tmp_ptr[i]);
			}

			i++;

			for (; i<real_num; i++) {
				_k = start+i;
				PROLOGUE2(0);

				(void)idx0;
				(void)next0;
				(void)b0;

				tmp_ptr[i] = ptr0;
			}

			// update and prefetch
			i = 0;
			for (;i+dist < real_num; i++) {
				PREFETCH_WRITE(tmp_ptr[i+dist]);

				_k = start+i;
				PROLOGUE2(0);
				ptr0 = tmp_ptr[i];
				A(0);
			}

			for (; i<real_num; i++) {
				_k = start+i;
				PROLOGUE2(0);
				ptr0 = tmp_ptr[i];
				A(0);
			}
		}
		return;

	// batch prefetch
	case 32:
	case 64:
	case 128:
		{
			size_t kVec = USE_PREFETCH;
			for (size_t z=start; z<num; z+=kVec) {
				size_t n = std::min(kVec, num-z);
				// pre-compute and prefetch
				for (size_t i=0; i<n; i++) {
					_k = z+i;
					PROLOGUE2(0);

					(void)idx0;
					(void)next0;
					(void)b0;

					tmp_ptr[i] = ptr0;
					PREFETCH_WRITE(ptr0);
				}

				for (size_t i=0; i<n; i++) {
					_k = z+i;
					PROLOGUE2(0);
					ptr0 = tmp_ptr[i];
					A(0);
				}
			}
		}
		return;

	default:
		break;
	};
#endif
	for (; _k+4<num; _k+=4) {
		PROLOGUE2(0)
		PROLOGUE2(1)
		PROLOGUE2(2)
		PROLOGUE2(3)

		A(0)
		A(1)
		A(2)
		A(3)
	}

	for (; _k<num; _k++) {
		PROLOGUE2(0)
		A(0)
	}

#undef A
}

static constexpr size_t kNumChoices = 8;

template<bool PARALLEL, size_t NEXT_STRIDE, size_t HASH_STRIDE, size_t WIDTH>
static void
_reinsert_hash_buckets(TableReinsertContext& ctx,
	void** RESTRICT buckets, uint64_t mask, 
	uint64_t* RESTRICT hashs, size_t _hash_stride,
	void** RESTRICT nexts, size_t _next_stride,
	bool parallel, char* RESTRICT base_ptr, size_t _width,
	size_t offset, size_t total_num)
{
	if (!ctx.extra_data) {
		// create
		ctx.extra_data = std::make_unique<BanditWrapper<kNumChoices>>();
	}

	auto& bandit = ((BanditWrapper<kNumChoices>*)ctx.extra_data.get())->bandit;

#define CALL(START, NUM, USE_PREFETCH) \
	__reinsert_hash_buckets<PARALLEL, NEXT_STRIDE, HASH_STRIDE, WIDTH, \
		USE_PREFETCH>( \
	buckets, hashs, _hash_stride, \
	nexts, _next_stride, mask, offset, base_ptr, _width, NUM, START, \
	ctx.tmp_ptr, ctx.tmp_next, ctx.tmp_b, ctx.tmp_old, ctx.tmp_ok)

	size_t vsize = ctx.tmp_size;
	for (size_t i=0; i<total_num; i+=vsize) {
		const size_t num = std::min(vsize, total_num-i);
		const size_t end = i+num;

		size_t arm = bandit.choose_arm();
		ASSERT(arm < kNumChoices);

		auto start_clock = profiling::physical_rdtsc();
		switch (arm) {
		case 0: CALL(i, end, 0); break;
		case 1:	CALL(i, end, 8); break;
		case 2:	CALL(i, end, 64); break;
		case 3:	CALL(i, end, 128); break;
		case 4:	CALL(i, end, -16); break;
		case 5:	CALL(i, end, -32); break;
		case 6:	CALL(i, end, -48); break;
		case 7: CALL(i, end, -64); break;
		}
		auto end_clock = profiling::physical_rdtsc();

		double diff = end_clock - start_clock;
		bandit.record(arm, -diff, num);
	}

#undef CALL
}


template<bool parallel, int partition>
static TableUtils::reinsert_call_t
_reinsert_hash_buckets(size_t hash_stride, size_t next_stride, size_t width)
{
	LOG_DEBUG("reinsert_hash_buckets: hash_stride %llu, next_stride %llu, width %llu",
		hash_stride, next_stride, width);
#if 1
#define C(NEXT_STRIDE, HASH_STRIDE, WIDTH) \
			case WIDTH: \
				return (reinsert_call_t)&_reinsert_hash_buckets<parallel, NEXT_STRIDE, HASH_STRIDE, WIDTH>; 

#define B(NEXT_STRIDE, HASH_STRIDE) \
			case HASH_STRIDE: \
				switch (width) { \
				C(NEXT_STRIDE, HASH_STRIDE, 1); \
				C(NEXT_STRIDE, HASH_STRIDE, 2); \
				C(NEXT_STRIDE, HASH_STRIDE, 4); \
				C(NEXT_STRIDE, HASH_STRIDE, 8); \
				C(NEXT_STRIDE, HASH_STRIDE, 16); \
				C(NEXT_STRIDE, HASH_STRIDE, 24); \
				C(NEXT_STRIDE, HASH_STRIDE, 32); \
				C(NEXT_STRIDE, HASH_STRIDE, 40); \
				C(NEXT_STRIDE, HASH_STRIDE, 48); \
				C(NEXT_STRIDE, HASH_STRIDE, 64); \
				C(NEXT_STRIDE, HASH_STRIDE, 80); \
				default: \
					return (reinsert_call_t)&_reinsert_hash_buckets<parallel, NEXT_STRIDE, HASH_STRIDE, 0>; \
				} \
				break;

#define A(NEXT_STRIDE) \
		case NEXT_STRIDE: \
			switch (hash_stride) { \
			B(NEXT_STRIDE, 2); \
			B(NEXT_STRIDE, 3); \
			B(NEXT_STRIDE, 4); \
			B(NEXT_STRIDE, 5); \
			B(NEXT_STRIDE, 6); \
			B(NEXT_STRIDE, 7); \
			B(NEXT_STRIDE, 8); \
			B(NEXT_STRIDE, 9); \
			B(NEXT_STRIDE, 10); \
			default: \
				return (reinsert_call_t)&_reinsert_hash_buckets<parallel, NEXT_STRIDE, 0, 0>; \
			} \
			break;

	switch (partition) {
	case 1:
		switch (next_stride) {
			A(2)
			default: break;
		}
		break;

	case 2:
		switch (next_stride) {
			A(3)
			default: break;
		}
		break;

	case 3:
		switch (next_stride) {
			A(4)
			default: break;
		}
		break;

	case 4:
		switch (next_stride) {
			A(5)
			default: break;
		}
		break;

	case 5:
		switch (next_stride) {
			A(6)
			default: break;
		}
		break;

	case 6:
		switch (next_stride) {
			A(7)
			default: break;
		}
		break;

	case 7:
		switch (next_stride) {
			A(8)
			default: break;
		}
		break;

	case 8:
		switch (next_stride) {
			A(9)
			default: break;
		}
		break;

	case 9:
		switch (next_stride) {
			A(10)
			default: break;
		}
		break;

	default:
		ASSERT(false && "Invalid partition"); break;
	}

#undef A
#undef B
#undef C

#endif
	return nullptr;
}

template<int partition>
static TableUtils::reinsert_call_t
reinsert_hash_buckets(bool parallel, size_t hash_stride, size_t next_stride, size_t width)
{
	LOG_DEBUG("reinsert_hash_buckets: hash_stride %llu, next_stride %llu, width %llu",
		hash_stride, next_stride, width);

	if (parallel) {
		return _reinsert_hash_buckets<true, partition>(hash_stride, next_stride, width);
	} else {
		return _reinsert_hash_buckets<false, partition>(hash_stride, next_stride, width);
	}
}

} /* engine::table */
