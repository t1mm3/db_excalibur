#include "plan_profile.hpp"
#include "system/profiling.hpp"
#include "system/system.hpp"
#include "engine/profile.hpp"

using namespace engine;
using namespace adaptive;

void
GenericProfData::from_profile(const profiling::ProfileData& data)
{
	using ProfIdent = profiling::ProfileData::ProfIdent;

	data.get_uint64(&sum_cycles, ProfIdent::kSumTime);
	data.get_uint64(&sum_input_tuples, ProfIdent::kNumInputTuples);
	data.get_uint64(&sum_output_tuples, ProfIdent::kNumOutputTuples);
}

void
GenericProfData::dump()
{
	LOG_WARN("in=%llu, out=%llu, cyc_tup=%f, sel=%f",
		sum_input_tuples, sum_output_tuples,
		get_cycles_per_tuple(), get_selectivity());
}




static void
propagate_profiling_info(
	std::unique_ptr<PlanProf>& out_plan_prof,
	std::vector<PlanProf*>& out_plan_prof_flat,
	LolepopProf* prof, const size_t depth = 0)
{
	if (!prof) {
		return;
	}
	const bool new_node = !out_plan_prof;

	if (!out_plan_prof) {
		out_plan_prof = std::make_unique<PlanProf>(depth);
	}

	if (out_plan_prof->primitives.size() != prof->primitives.size()) {
		out_plan_prof->primitives.resize(prof->primitives.size());
	}

	// overwrite profiling data with new one
	out_plan_prof->prof_data.from_profile(*prof);
	out_plan_prof->prof_data.update();

	for (size_t i=0; i<prof->primitives.size(); i++) {
		auto& dest = out_plan_prof->primitives[i];
		const auto& src = prof->primitives[i];
		ASSERT(src);

		if (!dest) {
			if (!src->statement_range) {
				LOG_WARN("propagate_profiling_info: No location");
			}
			dest = std::make_unique<PrimProf>(i, src->statement_range,
				src->get_name(), src->voila_func_tag);
		}

		dest->prof_data.from_profile(*src);
		dest->prof_data.update();
	}

	propagate_profiling_info(out_plan_prof->child, out_plan_prof_flat,
		prof->child.get(), depth+1);

	if (new_node) {
		out_plan_prof_flat.push_back(out_plan_prof.get());
	}
}

PlanProfile::PlanProfile()
{
	
}

void
PlanProfile::update(LolepopProf* prof)
{
	propagate_profiling_info(root, flattend, prof, 0);
}