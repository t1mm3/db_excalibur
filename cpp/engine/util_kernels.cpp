#include "util_kernels.hpp"
#include <string.h>

using namespace engine::util_kernels;

template<size_t STRIDE, typename T>
static NOINLINE void
typed_scatter_set(char* dest_ptr, size_t _stride, char* val_ptr,
	size_t width, size_t num)
{
	auto stride = STRIDE ? STRIDE : _stride;
	ASSERT(width == sizeof(T));

	auto arr = (T*)dest_ptr;

	if (val_ptr) {
		auto val = *((T*)val_ptr);
		for (size_t i=0; i<num; i++) {
			arr[i*stride] = val;
		}
	} else {
		T val;
		memset(&val, 0, sizeof(val));
		for (size_t i=0; i<num; i++) {
			arr[i*stride] = val;
		}
	}
}

static NOINLINE void
scatter_set_copy(char* dest_ptr, size_t stride, char* val_ptr,
	size_t width, size_t num)
{
	if (val_ptr) {
		for (size_t i=0; i<num; i++) {
			memcpy(&dest_ptr[i*width*stride], val_ptr, width);
		}
	} else {
		for (size_t i=0; i<num; i++) {
			memset(&dest_ptr[i*width*stride], 0, width);
		}
	}
}

Utils::scatter_set_call_t
Utils::get_scatter_set(size_t width, size_t stride)
{

#define B(T, N) case N: return &typed_scatter_set<N, T>;

	switch (width) {
#define A(T) \
		case sizeof(T): { \
			switch (stride) { \
B(T, 1)\
B(T, 2)\
B(T, 3)\
B(T, 4)\
B(T, 5)\
B(T, 6)\
B(T, 7)\
B(T, 8)\
B(T, 9)\
B(T, 10)\
B(T, 11)\
B(T, 12)\
B(T, 13)\
B(T, 14)\
B(T, 15)\
B(T, 16)\
B(T, 17)\
B(T, 18)\
B(T, 19)\
B(T, 20)\
B(T, 21)\
B(T, 22)\
B(T, 23)\
B(T, 24)\
B(T, 25)\
B(T, 26)\
B(T, 27)\
B(T, 28)\
B(T, 29)\
B(T, 30)\
B(T, 31)\
B(T, 32)\
B(T, 33)\
B(T, 34)\
B(T, 35)\
B(T, 36)\
B(T, 37)\
B(T, 38)\
B(T, 39)\
B(T, 40)\
B(T, 41)\
B(T, 42)\
B(T, 43)\
B(T, 44)\
B(T, 45)\
B(T, 46)\
B(T, 47)\
B(T, 48)\
B(T, 49)\
B(T, 50)\
B(T, 51)\
B(T, 52)\
B(T, 53)\
B(T, 54)\
B(T, 55)\
B(T, 56)\
B(T, 57)\
B(T, 58)\
B(T, 59)\
B(T, 60)\
B(T, 61)\
B(T, 62)\
B(T, 63)\
B(T, 64)\
B(T, 65)\
B(T, 66)\
B(T, 67)\
B(T, 68)\
B(T, 69)\
B(T, 70)\
B(T, 71)\
B(T, 72)\
B(T, 73)\
B(T, 74)\
B(T, 75)\
B(T, 76)\
B(T, 77)\
B(T, 78)\
B(T, 79)\
B(T, 80)\
B(T, 81)\
B(T, 82)\
B(T, 83)\
B(T, 84)\
B(T, 85)\
B(T, 86)\
B(T, 87)\
B(T, 88)\
B(T, 89)\
B(T, 90)\
B(T, 91)\
B(T, 92)\
B(T, 93)\
B(T, 94)\
B(T, 95)\
B(T, 96)\
B(T, 97)\
B(T, 98)\
B(T, 99)\
B(T, 100)\
B(T, 101)\
B(T, 102)\
B(T, 103)\
B(T, 104)\
B(T, 105)\
B(T, 106)\
B(T, 107)\
B(T, 108)\
B(T, 109)\
B(T, 110)\
B(T, 111)\
B(T, 112)\
B(T, 113)\
B(T, 114)\
B(T, 115)\
B(T, 116)\
B(T, 117)\
B(T, 118)\
B(T, 119)\
B(T, 120)\
B(T, 121)\
B(T, 122)\
B(T, 123)\
B(T, 124)\
B(T, 125)\
B(T, 126)\
B(T, 127)\
			default:return &typed_scatter_set<0, T>; \
			} \
		}

	A(uint8_t)
	A(uint16_t)
	A(uint32_t)
	A(uint64_t)
	A(Some128BitType)
#undef A
	default:	return &scatter_set_copy;
	}
}
