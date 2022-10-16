#include <vector>
#include <stdlib.h>
#include "sort.hpp"
#include "sort_helper.hpp"

#if 0
struct SortItem 
{
	bool ascending;
	size_t offset;

	virtual bool less_than(char* a, char* b) = 0;
};

template<typename T>
struct PrimSortItem : SortItem {
	bool less_than(char* _a, char* _b) final {
		auto a = (T*)_a;
		auto b = (T*)_b;

		return *a < *b;
	}
};

struct Sorter {
	std::vector<SortItem> key_cols;

	void* data;
	size_t count;
	size_t row_width;

	bool less_than(char* row_a, char* row_b) {
		for (auto& key : key_cols) {
			char* a = row_a + key.offset;
			char* b = row_b + key.offset;
			if (key.less_than(a, b) == key.ascending) {
				return true;
			}
		}
		return false;
	}

	void sort() {
		qsort(data, count, row_width, [&] (void* a, void* b) {
			return -1;
		});
	}
};

#endif