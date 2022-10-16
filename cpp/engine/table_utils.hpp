#pragma once

#include "system/system.hpp"
#include <memory>

namespace engine::table {

struct TableReinsertContextExtraData {
	virtual ~TableReinsertContextExtraData() = default;
};

struct TableReinsertContext {
	void*** tmp_ptr;
	void*** tmp_next;
	void** tmp_b;
	void** tmp_old;
	char* tmp_ok;

	size_t tmp_size = 0;

	std::unique_ptr<TableReinsertContextExtraData> extra_data;
}; 

struct TableUtils {
	typedef void (*reinsert_call_t)(TableReinsertContext& ctx,
		void** buckets, uint64_t mask, uint64_t* blk_hashes, size_t hash_stride,
		void** blk_nexts, size_t next_stride, bool parallel, char* base_ptr,
		size_t width, size_t offset, size_t total_num);

	static reinsert_call_t get_reinsert(bool parallel, size_t hash_stride,
		size_t next_stride, size_t width);

private:

#define A(i) static reinsert_call_t get_reinsert##i(bool parallel, \
		size_t hash_stride, size_t next_stride, size_t width);

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
};

} /* engine::table */