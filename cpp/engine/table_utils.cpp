#include "table_utils.hpp"
#include "table_utils_impl.hpp"

using namespace engine::table;

reinsert_call_t
TableUtils::get_reinsert(bool parallel, size_t hash_stride,
		size_t next_stride, size_t width)
{
	reinsert_call_t result = nullptr;

#define A(N) \
		result = get_reinsert##N(parallel, hash_stride, \
			next_stride, width); \
		if (result) { return result; }

	A(1)
	A(2)
	A(3)
	A(4)
	A(5)
	A(6)
	A(7)
	A(8)
	A(9)

#undef A

	// fallback implementation
	if (!parallel) {
		return (reinsert_call_t)&_reinsert_hash_buckets<false,0,0,0>;
	}
	return (reinsert_call_t)&_reinsert_hash_buckets<true,0,0,0>;
}