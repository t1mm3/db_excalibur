#pragma once

#include "lolepop.hpp"

#include <vector>

namespace engine {
struct Type;
}

namespace engine {
namespace lolepop {

struct Output : Lolepop {
	Output(Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
		const std::shared_ptr<Lolepop>& child);
	~Output();

	NextResult next(NextContext& context) final;

private:
	const bool m_enabled;

	std::vector<size_t> m_widths;
	std::vector<char*> m_first_ptrs;
	std::vector<Type*> m_types;
};

} /* lolepop */
} /* engine */