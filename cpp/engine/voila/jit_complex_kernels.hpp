#pragma once

#include "system/build.hpp"
#include "system/system.hpp"

#include "engine/util_kernels.hpp"

#ifdef __AVX512F__
#include <immintrin.h>
#endif

#include <string>

template<typename SEL_T, typename BUCKET_T>
FORCE_INLINE inline size_t
__vec_bucket_insert(BUCKET_T* RESTRICT res, uint64_t* RESTRICT indices,
	SEL_T* RESTRICT sel, size_t inum, BUCKET_T* RESTRICT nexts,
	BUCKET_T* RESTRICT heads, uint64_t bucket_mask,
	uint64_t ptr_begin, uint64_t ptr_end, size_t row_width, size_t next_stride)
{
	ASSERT(indices && nexts && heads && res);
	uint64_t num_inserted = 0;

	engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
		sel, inum);

	if (false && LOG_WILL_LOG(DEBUG)) {
		engine::util_kernels::_map8<SEL_T>(sel, (SEL_T)inum, [&] (auto i) FORCE_INLINE {
			const uint64_t idx = indices[i] & bucket_mask;
			auto& head = heads[idx];
			res[i] = 0;
			bool conflict = head >= ptr_begin && head <= ptr_end;
			LOG_DEBUG("bucket_insert @ %lld: %llu, hash %p, idx %p, conflict %d, head %p, sel %p",
				i, (void*)idx, indices[i], indices[i] & bucket_mask, conflict, head, sel);
			if (!conflict) {
				res[i] = (uint64_t)((char*)ptr_begin + (row_width*num_inserted));
				LOG_DEBUG("result %p @ %lld", res[i], i);
				nexts[num_inserted * next_stride] = head;
				head = res[i];
				num_inserted++;
			}
		});
	} else {

#if 0
#define A(i) {\
		const uint64_t idx = indices[i] & bucket_mask; \
		auto& head = heads[idx]; \
		res[i] = 0; \
		bool conflict = head >= ptr_begin && head <= ptr_end; \
		if (!conflict) { \
			res[i] = (uint64_t)((char*)ptr_begin + (row_width*num_inserted)); \
			nexts[num_inserted * next_stride] = head; \
			head = res[i]; \
			num_inserted++; \
		} \
	}

		if (sel) {
			for (SEL_T k=0; k<num; k++) {
				A(sel[k]);
			}
		} else {
			for (SEL_T k=0; k<num; k++) {
				A(k);
			}
		}
#else
		engine::util_kernels::_map8<SEL_T>(sel, (SEL_T)inum, [&] (auto i) FORCE_INLINE {
			const uint64_t idx = indices[i] & bucket_mask;
			auto& head = heads[idx];
			res[i] = 0;
			bool conflict = head >= ptr_begin && head <= ptr_end;
			if (!conflict) {
				res[i] = (uint64_t)((char*)ptr_begin + (row_width*num_inserted));
				nexts[num_inserted * next_stride] = head;
				head = res[i];
				num_inserted++;
			}
		});
#endif
	}

	return num_inserted;
}

inline static size_t
vec_bucket_insert(uint64_t* res, engine::table::IHashTable* table,
	uint64_t* indices, selvector_t* sel, size_t inum, int64_t numa_node)
{
	static_assert(sizeof(void*) == sizeof(uint64_t), "64-bit pointers");

	/* prealloc space for potential new groups */
	auto block = table->hash_append_prealloc(inum, numa_node);

	/* to later detect conflicts, determine pointer range inside current block */
	uint64_t ptr_begin = 0;
	uint64_t ptr_end = 0;
	table->get_current_block_range((char**)&ptr_begin, (char**)&ptr_end, inum);

	/* set common table stuff */
	const uint64_t bucket_mask = table->get_hash_index_mask();
	uint64_t* heads = (uint64_t*)table->get_hash_index();

	DBG_ASSERT(block && heads && bucket_mask);

	const size_t next_offset = table->get_next_offset();
	const size_t next_stride = table->get_next_stride();
	uint64_t* nexts = (uint64_t*)((char*)ptr_begin + next_offset);

	const size_t row_width = table->get_row_width();
	uint64_t num_inserted = __vec_bucket_insert<selvector_t, uint64_t>(res,
		indices, sel, inum, nexts, heads, bucket_mask,
		ptr_begin, ptr_end, row_width, next_stride);

	table->hash_append_prune(block, num_inserted);

	DBG_ASSERT(block->num <= block->capacity);

	LOG_TRACE("vec_bucket_insert: num inserted %d", (int)num_inserted);
	return num_inserted;
}


template<typename SEL_T, bool PREDICATED, bool NO_COPY>
FORCE_INLINE inline SEL_T
_selunion(SEL_T* RESTRICT result, SEL_T maxnum,
	SEL_T* RESTRICT larr, SEL_T lnum,
	SEL_T* RESTRICT rarr, SEL_T rnum,
	SEL_T* RESTRICT res)
{
	ASSERT(larr && larr != rarr);

	SEL_T k=0, l=0, r=0;

	if (PREDICATED) {
		while ((l < lnum) & (r < rnum)) {
			const auto lval = larr[l];
			const auto rval = rarr[r];

			bool le = lval <= rval;
			bool ge = lval >= rval;

			res[k] = lval;
			k += le;
			res[k] = rval;
			k += !le;

			l += le;
			r += ge;
		}
	} else {
		while (l < lnum && r < rnum) {
			if (larr[l] < rarr[r]) {
				res[k++] = larr[l++];
			} else {
				res[k++] = rarr[r++];
				l += larr[l] == rarr[r];
			}
		}
	}

	while (l < lnum) {
		const auto val = larr[l];
		if (!k || val > res[k-1]) {
			res[k++] = val;
		}

		l++;
	}

	while (r < rnum) {
		const auto val = rarr[r];
		if (!k || val > res[k-1]) {
			res[k++] = val;
		}

		r++;
	}

	if (!NO_COPY) {
		// copy 'res' into 'result'
		for (SEL_T i=0; i<k; i++) {
			result[i] = res[i];
		}

	}

	ASSERT(k <= maxnum);
	return k;
}


template<typename SEL_T, bool PREDICATED>
inline SEL_T
selunion(SEL_T* RESTRICT result, SEL_T maxnum,
	SEL_T* RESTRICT larr, SEL_T lnum,
	SEL_T* RESTRICT rarr, SEL_T rnum,
	SEL_T* RESTRICT res)
{
	engine::util_kernels::debug_assert_valid_selection_vector<SEL_T>(
		larr, lnum);

	engine::util_kernels::debug_assert_valid_selection_vector<SEL_T>(
		rarr, rnum);

	if (!lnum || !rnum) {
		if (!lnum && !rnum) {
			return 0;
		}
		SEL_T* source = rnum ? rarr : larr;
		SEL_T count = rnum ? rnum : lnum;
		if (result != source) {
			memmove(result, source, count*sizeof(SEL_T));
		}

		return count;
	}

	LOG_TRACE("selunion: result %p, larr %p, lnum %lld, rarr %p, rnum %lld, res %p",
		result, larr, lnum, rarr, rnum, res);
	SEL_T k = _selunion<SEL_T, PREDICATED, false>(result,
		maxnum, larr, lnum, rarr, rnum, res);

	engine::util_kernels::debug_assert_valid_selection_vector<SEL_T>(
		result, k);

	return k;
}

template<bool IS_TRUE, bool EMPTY_OUT, bool TWICE>
inline int32_t
avx512_selcond_i8_i32(int32_t* RESTRICT res, char* RESTRICT pred,
	int32_t* RESTRICT sel, int32_t num, int32_t* RESTRICT tmp)
{
	int32_t k=0;
	int32_t i=0;

	if (sel) {
		auto stop_idx = sel[num-1];
		if (stop_idx+1 == num) {
			sel = nullptr;
		}
	}

	bool sel_res_overlap = res == sel;

#if 1
#ifdef __AVX512F__
	if (sel) {
		const auto mask = _mm512_set1_epi32(0xFF);

		auto dest = sel_res_overlap ? tmp : res;
		if (TWICE) {
			for (;i+32<=num; i+=32) {
				auto sids = _mm512_loadu_si512(sel+i);
				auto values = _mm512_i32gather_epi32(sids, pred, 1);
				auto mask16 = IS_TRUE ?
					_mm512_test_epi32_mask(values, mask) :
					_mm512_testn_epi32_mask(values, mask);

				auto sids2 = _mm512_loadu_si512(sel+i+16);
				auto values2 = _mm512_i32gather_epi32(sids2, pred, 1);
				auto mask162 = IS_TRUE ?
					_mm512_test_epi32_mask(values2, mask) :
					_mm512_testn_epi32_mask(values2, mask);

				if (EMPTY_OUT & !mask16 & !mask162) {
					continue;
				}

				_mm512_mask_compressstoreu_epi32(dest + k, mask16, sids);
				k += __builtin_popcount(mask16);
				_mm512_mask_compressstoreu_epi32(dest + k, mask162, sids2);
				k += __builtin_popcount(mask162);
			}
		} else {
			for (;i+16<=num; i+=16) {
				auto sids = _mm512_loadu_si512(sel+i);
				auto values = _mm512_i32gather_epi32(sids, pred, 1);
				auto mask16 = IS_TRUE ?
					_mm512_test_epi32_mask(values, mask) :
					_mm512_testn_epi32_mask(values, mask);

				if (EMPTY_OUT && !mask16) {
					continue;
				}

				_mm512_mask_compressstoreu_epi32(dest + k, mask16, sids);

				k += __builtin_popcount(mask16);
			}
		}
	} else {
		auto ids = _mm512_set_epi32(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
		const auto null8 = _mm_set1_epi8(0);

		if (TWICE) {
			for (;i+16<=num; i+=16) {
				auto values = _mm_loadu_si128((__m128i*)(pred+i));
				auto mask16 = IS_TRUE ?
					_mm_cmpneq_epi8_mask(values, null8) :
					_mm_cmpeq_epi8_mask(values, null8);

				if (EMPTY_OUT && !mask16) {
					ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));
					continue;
				}

				_mm512_mask_compressstoreu_epi32(res + k, mask16, ids);

				k += __builtin_popcount(mask16);
				ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));
			}
		} else {
			for (;i+32<=num; i+=32) {
				auto values = _mm_loadu_si128((__m128i*)(pred+i));
				auto mask16 = IS_TRUE ?
					_mm_cmpneq_epi8_mask(values, null8) :
					_mm_cmpeq_epi8_mask(values, null8);
				auto values2 = _mm_loadu_si128((__m128i*)(pred+i+16));
				auto mask162 = IS_TRUE ?
					_mm_cmpneq_epi8_mask(values2, null8) :
					_mm_cmpeq_epi8_mask(values2, null8);

				if (EMPTY_OUT & !mask16 & !mask162) {
					ids = _mm512_add_epi32(ids, _mm512_set1_epi32(32));
					continue;
				}

				_mm512_mask_compressstoreu_epi32(res + k, mask16, ids);

				k += __builtin_popcount(mask16);
				ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));

				_mm512_mask_compressstoreu_epi32(res + k, mask162, ids);

				k += __builtin_popcount(mask162);
				ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));
			}
		}
	}
#endif
#endif

	if (sel) {
		if (sel_res_overlap) {
			if (IS_TRUE) {
				for (;i<num; i++) {
					if (pred[sel[i]]) {
						tmp[k] = sel[i];
						k++;
					}
				}
			} else {
				for (;i<num; i++) {
					if (!pred[sel[i]]) {
						tmp[k] = sel[i];
						k++;
					}
				}
			}
		} else {
			if (IS_TRUE) {
				for (;i<num; i++) {
					if (pred[sel[i]]) {
						res[k] = sel[i];
						k++;
					}
				}
			} else {
				for (;i<num; i++) {
					if (!pred[sel[i]]) {
						res[k] = sel[i];
						k++;
					}
				}
			}
		}
	} else {
		if (IS_TRUE) {
			for (;i<num; i++) {
				if (pred[i]) {
					res[k] = i;
					k++;
				}
			}
		} else {
			for (;i<num; i++) {
				if (!pred[i]) {
					res[k] = i;
					k++;
				}
			}
		}
	}

	if (sel && sel_res_overlap) {
		// copy tmp to result
		memcpy(res, tmp, k * sizeof(int32_t));
	}

	return k;
}

template<bool IS_TRUE, bool IS_PREDICATED>
inline int32_t
regular_selcond_i8_i32(int32_t* RESTRICT res, char* RESTRICT pred,
	int32_t* RESTRICT sel, int32_t num, int32_t* RESTRICT tmp)
{
	if (sel) {
		auto stop_idx = sel[num-1];
		if (stop_idx+1 == num) {
			sel = nullptr;
		}
	}

	int32_t k = 0;

	if (IS_TRUE) {
		engine::util_kernels::_map8<int32_t>(sel, num, [&] (auto i) FORCE_INLINE {
			if (IS_PREDICATED) {
				res[k] = i;
				k+= !!pred[i];
			} else {
				if (pred[i]) {
					res[k] = i;
					k++;
				}
			}
		});
	} else {
		engine::util_kernels::_map8<int32_t>(sel, num, [&] (auto i) FORCE_INLINE {
			if (IS_PREDICATED) {
				res[k] = i;
				k+= !pred[i];
			} else {
				if (!pred[i]) {
					res[k] = i;
					k++;
				}
			}
		});
	}


	return k;
}

template<bool IS_TRUE, bool MOSTLY>
inline int32_t
mostly_selcond_i8_i32(int32_t* RESTRICT res, char* RESTRICT pred,
	int32_t* RESTRICT sel, int32_t num, int32_t* RESTRICT tmp)
{
	if (sel) {
		auto stop_idx = sel[num-1];
		if (stop_idx+1 == num) {
			sel = nullptr;
		}
	}

	int32_t k = 0;
	int32_t i = 0;

	if (sel) {
		if (IS_TRUE) {
			if (MOSTLY) {
				for (;i+4<num; i+=4) {
					auto v0 = pred[sel[i+0]];
					auto v1 = pred[sel[i+1]];
					auto v2 = pred[sel[i+2]];
					auto v3 = pred[sel[i+3]];

					if (v0 & v1 & v2 & v3) {
						res[k] = sel[i+0]; k++;
						res[k] = sel[i+1]; k++;
						res[k] = sel[i+2]; k++;
						res[k] = sel[i+3]; k++;

						continue;
					}

					if (v0) {
						res[k] = sel[i+0]; k++;
					}
					if (v1) {
						res[k] = sel[i+1]; k++;
					}
					if (v2) {
						res[k] = sel[i+2]; k++;
					}
					if (v3) {
						res[k] = sel[i+3]; k++;
					}
				}
			}

			for (;i<num; i++) {
				if (pred[sel[i]]) {
					res[k] = sel[i];
					k++;
				}
			}
		} else {
			for (;i<num; i++) {
				if (!pred[sel[i]]) {
					res[k] = sel[i];
					k++;
				}
			}
		}
	} else {
		if (IS_TRUE) {
			if (MOSTLY) {
				for (;i+4<num; i+=4) {
					auto v0 = pred[i+0];
					auto v1 = pred[i+1];
					auto v2 = pred[i+2];
					auto v3 = pred[i+3];

					if (v0 & v1 & v2 & v3) {
						res[k] = i+0; k++;
						res[k] = i+1; k++;
						res[k] = i+2; k++;
						res[k] = i+3; k++;

						continue;
					}

					if (v0) {
						res[k] = i+0; k++;
					}
					if (v1) {
						res[k] = i+1; k++;
					}
					if (v2) {
						res[k] = i+2; k++;
					}
					if (v3) {
						res[k] = i+3; k++;
					}
				}
			}

			for (;i<num; i++) {
				if (pred[i]) {
					res[k] = i;
					k++;
				}
			}
		} else {
			for (;i<num; i++) {
				if (!pred[i]) {
					res[k] = i;
					k++;
				}
			}
		}
	}

	return k;
}
