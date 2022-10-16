#include "sort_helper.hpp"
#include "system/system.hpp"
#include <stdlib.h>

using namespace engine::lolepop;

static thread_local SortHelper::Item* inject_items;

static int
sort_helper_compare(const void* a, const void* b)
{
	const char* data_a = (const char*)a;
	const char* data_b = (const char*)b;

	SortHelper::Item* is = inject_items;
	ASSERT(is);
	while (1) {
		SortHelper::Item* item = is++;
		if (!item->call) {
			return 0;
		}
		int r = item->call(data_a + item->offset,
			data_b + item->offset);
		if (r) {
			return r;
		}
	}
	return 0;
}

void
SortHelper::sort(char* base, size_t num)
{
	finalize();
	const size_t width = offset;

	inject_items = &m_items[0];
	qsort(base, num, width, sort_helper_compare);
	inject_items = nullptr;
}
