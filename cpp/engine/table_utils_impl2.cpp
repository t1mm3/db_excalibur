#include "table_utils.hpp"
#include "table_utils_impl.hpp"

using namespace engine::table;

reinsert_call_t
TableUtils::get_reinsert2(bool parallel, size_t hash_stride,
	size_t next_stride, size_t width)
{
	return reinsert_hash_buckets<2>(parallel, hash_stride, next_stride, width);
}