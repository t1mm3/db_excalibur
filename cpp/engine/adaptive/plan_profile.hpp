#pragma once

#include <vector>
#include <stdint.h>
#include <memory>
#include <string>

namespace profiling {
struct ProfileData;
} /* profiling */

namespace engine {
struct LolepopProf;
} /* engine	 */

namespace engine::voila {
struct StatementRange;
} /* engine::voila */

namespace engine::adaptive {

struct GenericProfData {
	uint64_t sum_input_tuples = 0;
	uint64_t sum_output_tuples = 0;
	uint64_t sum_cycles = 0;

	void from_profile(const profiling::ProfileData& data);

	void update() {

	}

	void dump();

	double get_cycles_per_tuple() const {
		return (double)sum_cycles / (double)sum_input_tuples;
	}

	double get_selectivity() const {
		return (double)sum_output_tuples / (double)sum_input_tuples;
	}
};

struct PrimProf {
	using StatementRange = engine::voila::StatementRange;

	size_t index = 0;

	std::shared_ptr<StatementRange> statement_range;

	GenericProfData prof_data;
	const std::string dbg_name;
	int64_t voila_func_tag;

	PrimProf(size_t index, const std::shared_ptr<StatementRange>& statement_range,
		const std::string& dbg_name, int64_t voila_func_tag)
	 : index(index), statement_range(statement_range), dbg_name(dbg_name), voila_func_tag(voila_func_tag)
	{
	}

	void dump() {
		prof_data.dump();
	}
};

struct PlanProf {
	std::unique_ptr<PlanProf> child;

	PlanProf(size_t id_from_root) : id_from_root(id_from_root) {}

	GenericProfData prof_data;
	std::vector<std::unique_ptr<PrimProf>> primitives;

	const size_t id_from_root;
};

struct PlanProfile {
	std::unique_ptr<PlanProf> root;
	std::vector<PlanProf*> flattend;

	PlanProfile();

	void update(LolepopProf* prof);
	bool empty() const { return !root.get(); }
};

} /* engine::adaptive */

