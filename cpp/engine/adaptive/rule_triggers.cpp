#include "rule_triggers.hpp"
#include "rule_triggers_impl.hpp"
#include "decisions.hpp"
#include "brain.hpp"
#include "plan_profile.hpp"

#include "engine/voila/flavor.hpp"
#include "engine/voila/statement_fragment_selection_pass.hpp"
#include "engine/loleplan_passes.hpp"
#include "system/system.hpp"


using namespace engine::adaptive;

RuleTrigger::Input::Input(const Brain& brain, Actions* previous,
	const std::shared_ptr<engine::lolepop::Lolepop>& example_plan,
	const std::shared_ptr<voila::Context>& voila_context,
	PlanProfile* given_plan_profile)
 : brain(brain), previous(previous), example_plan(example_plan),
		voila_context(voila_context), given_plan_profile(given_plan_profile) {
	memset(&class_type_counter[0], 0, sizeof(class_type_counter));

	if (previous) {
		previous->for_each([&] (auto d) {
			class_type_counter[d->get_type()]++;
			num_decisions++;
		});
	}
	jit_fragments.reserve(num_decisions);

	if (previous) {
		previous->for_each([&] (auto d) {
			if (auto jit = dynamic_cast<JitStatementFragment*>(d)) {
				jit_fragments.push_back(jit->get_statement_range());
			} else if (auto flavor = dynamic_cast<SetScopeFlavor*>(d)) {
				flavor_fragments.push_back(flavor->get_statement_range());
			}
		});
	}
}

RuleTrigger::Input::~Input()
{

}

InputPlanProfile*
RuleTrigger::Input::get_input_plan_profile()
{
	if (plan_profile) {
		return plan_profile.get();
	}

	if (!given_plan_profile) {
		return nullptr;
	}

	plan_profile = std::make_unique<InputPlanProfile>();
	for (auto& op : given_plan_profile->flattend) {
		for (auto& prim : op->primitives) {
			ASSERT(prim);
			if (!prim->statement_range) {
				continue;
			}
			auto& seq = plan_profile->begin_prims[prim->statement_range->begin];
			seq.push_back(prim.get());
		}
	}

	return plan_profile.get();
}




RuleTriggers::RuleTriggers()
{
	using FlavorSpec = engine::voila::FlavorSpec;

	triggers.emplace_back(std::make_unique<triggers::EnableBloomFilterForHighestSelJoin>(2));
	triggers.emplace_back(std::make_unique<triggers::EnableBloomFilterForHighestSelJoin>(1));
	triggers.emplace_back(std::make_unique<triggers::PushDownMostSelectiveFilter>());

	SetVectorKey::for_each_vector_size([&] (const auto& val) {
		triggers.emplace_back(std::make_unique<triggers::SetVectorSize>(val));
	});

	auto data_centric_flavor = FlavorSpec::get_data_centric_flavor(0);

	triggers.emplace_back(std::make_unique<triggers::FindJittableFragment>(
				data_centric_flavor, 0, true));


	// std::vector<std::unique_ptr<RuleTrigger>> extra_triggers;

	for_each_flavor([&] (const auto& flavor) {
#if 1
		for (auto& profile_guided : {0, -1, -2, -3}) { // no 1
			if (!profile_guided && flavor == data_centric_flavor) continue;

			triggers.emplace_back(std::make_unique<triggers::FindJittableFragment>(
				flavor,
				profile_guided,
				true));
		}

		triggers.emplace_back(std::make_unique<triggers::JitExpressionsStrategy>(flavor));
		triggers.emplace_back(std::make_unique<triggers::SetScopeFlavor>(flavor));
		triggers.emplace_back(std::make_unique<triggers::SetDefaultFlavor>(flavor));
#endif
	});
}
