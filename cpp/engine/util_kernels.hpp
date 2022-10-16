#pragma once

#include "system/system.hpp"
#include <string.h>

namespace engine::util_kernels {

template<typename SEL_T, typename T>
FORCE_INLINE inline void
_map_nosel(SEL_T* RESTRICT sel, SEL_T num, const T& fun)
{
	SEL_T k=0;

	for (; k<num; k++) {
		fun(k);
	}
}

template<typename SEL_T, typename T>
FORCE_INLINE inline void
_map_nosel8(SEL_T* RESTRICT sel, SEL_T num, const T& fun)
{
	SEL_T k=0;

	for (; k+8<num; k+=8) {
		fun(k+0);
		fun(k+1);
		fun(k+2);
		fun(k+3);

		fun(k+4);
		fun(k+5);
		fun(k+6);
		fun(k+7);
	}

	for (; k<num; k++) {
		fun(k);
	}
}

template<typename SEL_T, typename T>
FORCE_INLINE inline void
_map_sel(SEL_T* RESTRICT sel, SEL_T num, const T& fun)
{
	SEL_T k=0;

	for (; k+8<num; k+=8) {
		fun(sel[k+0]);
		fun(sel[k+1]);
		fun(sel[k+2]);
		fun(sel[k+3]);

		fun(sel[k+4]);
		fun(sel[k+5]);
		fun(sel[k+6]);
		fun(sel[k+7]);
	}

	for (; k<num; k++) {
		fun(sel[k]);
	}
}

template<typename SEL_T, typename T>
FORCE_INLINE inline void
_map(SEL_T* RESTRICT sel, SEL_T num, const T& fun)
{
	if (sel) {
		_map_sel<SEL_T, T>(sel, num, fun);
	} else {
		_map_nosel<SEL_T, T>(sel, num, fun);
	}
}

template<typename SEL_T, typename T>
FORCE_INLINE inline void
_map8(SEL_T* RESTRICT sel, SEL_T num, const T& fun)
{
	if (sel) {
		_map_sel<SEL_T, T>(sel, num, fun);
	} else {
		_map_nosel8<SEL_T, T>(sel, num, fun);
	}
}

template<size_t STRIDE, typename SEL_T, typename T>
void _fetch(T* RESTRICT result, size_t offset, size_t _stride, char* RESTRICT data,
	SEL_T* sel, SEL_T num)
{
	size_t stride = STRIDE ? STRIDE : _stride;
	T* RESTRICT arr = (T*)(data + offset);

	_map(sel, num, [&] (auto i) {
		result[i] = arr[i * stride];
	});
}

template<typename SEL_T, typename T>
void fetch(T* RESTRICT result, size_t offset, size_t stride, char* RESTRICT data,
	SEL_T* RESTRICT sel, SEL_T num)
{
	if (stride == 1) {
		_fetch<1, SEL_T, T>(result, offset, stride, data, sel, num);
	} else {
		_fetch<0, SEL_T, T>(result, offset, stride, data, sel, num);
	}
}

template<typename T>
void
broadcast_typed(char* _out, char* _src, size_t num)
{
	T* out = (T*)(_out);
	T* src = (T*)(_src);

	for (size_t i=0; i<num; i++) {
		out[i] = *src;
	}
}

inline void
broadcast(char* output, char* src, size_t width, size_t num) {
	ASSERT(output);
	ASSERT(src);
	ASSERT(width > 0);

	size_t i=0;

	switch (width) {

#define IMPL(T) case sizeof(T): broadcast_typed<T>(output, src, num); break;
	
	IMPL(uint8_t)
	IMPL(uint16_t)
	IMPL(uint32_t)
	IMPL(uint64_t)

#undef IMPL

	default:
		for (; i<num; i++) {
			char* dest = output + width*i;
			memcpy(dest, src, width);
		}
		break;
	}
}

template<typename T, typename SEL_T>
void
_copy_typed_sel(T* out, T* in, SEL_T* sel, size_t num)
{
	_map_sel<SEL_T>(sel, num, [&] (auto i) {
		out[i] = in[i];
	});
}

struct Some128BitType {
	uint64_t a;
	uint64_t b;
};

template<typename SEL_T>
void
copy(char* output, char* src, size_t width, SEL_T* sel, size_t num)
{
	ASSERT(output);
	ASSERT(src);
	ASSERT(width > 0);

	if (!sel) {
		memcpy(output, src, num*width);
	}

	switch (width) {

#define IMPL(T) case sizeof(T): \
		_copy_typed_sel<T, SEL_T>((T*)output, (T*)src, sel, num); break;
	
	IMPL(uint8_t)
	IMPL(uint16_t)
	IMPL(uint32_t)
	IMPL(uint64_t)
	IMPL(Some128BitType);

#undef IMPL

	default:
		_map_sel(sel, (SEL_T)num, [&] (auto i) {
			char* d = output + width*i;
			char* s = src + width*i;
			memcpy(d, s, width);
		});
		break;
	}
}

template<typename SEL_T, typename T>
static void
assert_valid_selection_vector(const SEL_T* const sel, SEL_T num, const T& fun)
{
	return;

	if (!sel || !num) {
		return;
	}

	SEL_T p = sel[0];
	for (SEL_T k=1; k<num; k++) {
		if (sel[k] <= p) {
			fun(k);
		}
		p = sel[k];
	}
}

template<typename SEL_T>
static void
debug_assert_valid_selection_vector(const SEL_T* const sel, SEL_T num)
{
#ifdef IS_DEBUG_BUILD
	assert_valid_selection_vector<SEL_T>(sel, num,
		[&] (auto k) {
			LOG_ERROR("Invalid index in selection vector at %lld",
				(long)k);
			ASSERT(false);
	});
#endif
}

struct Utils {
	typedef void (*scatter_set_call_t)(char* dest_ptr, size_t stride, char* val_ptr,
		size_t width, size_t num);

	static scatter_set_call_t get_scatter_set(size_t width, size_t stride);
};

} /* engine::util_kernels */