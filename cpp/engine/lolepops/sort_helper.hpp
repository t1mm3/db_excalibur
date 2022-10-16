#pragma once

#include <vector>
#include "engine/types.hpp"

namespace engine {
namespace lolepop {

struct SortHelper {
	void ascending(Type* t) { add<false>(t); }
	void descending(Type* t) { add<true>(t); }

	void sort(char* base, size_t num);

	using call_t = Type::compare_t;

	struct Item {
		call_t call;
		size_t offset;
	};

private:

	static int wrapped_compare(const void* a, const void* b);

	template<bool ASCENDING>
	void add(Type* t) {
		add(t->get_compare_func(ASCENDING), t->get_width());
	}

	void add(call_t call, size_t width) {
		m_items.push_back({ call, offset });

		offset += width;
	}

	void finalize() {
		if (finalized) {
			return;
		}
		add(nullptr, 0);

		finalized = true;
	}

	std::vector<Item> m_items;
	size_t offset = 0;
	bool finalized = false;
};

} /* lolepop */
} /* engine */

