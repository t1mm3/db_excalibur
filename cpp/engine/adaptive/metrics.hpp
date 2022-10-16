#pragma once

#include <ostream>
#include <string>

namespace engine::adaptive {

struct Metrics {
	double cyc_per_tup = .0;
	size_t num_tuples = 0;
	size_t num_cycles = 0;

	void to_string(std::ostream& o) const;
	std::string to_string() const;

	void aggregate(const Metrics& o) {
		num_tuples += o.num_tuples;
		num_cycles += o.num_cycles;

		cyc_per_tup = o.cyc_per_tup;
	}
};

} /* engine::adaptive */