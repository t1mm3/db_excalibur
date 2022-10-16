#pragma once

#include <stdint.h>
#include <cstring>
#include <functional>

#include "decision.hpp"
#include "decisions.hpp"
#include "system/system.hpp"

namespace engine::voila {
struct Context;
}

namespace engine::lolepop {
struct Lolepop;
}

namespace engine::adaptive {
struct Brain;
struct Actions;
struct DecisionFactory;
struct PlanProfile;
struct PrimProf;
}

namespace engine::adaptive {

struct InputPlanProfile {
	using StatementIdentifier = engine::voila::StatementIdentifier;

	std::unordered_map<StatementIdentifier, std::vector<PrimProf*>> begin_prims;
};

struct RuleTrigger {
	struct Input {
		const Brain& brain;
		Actions* previous;
		uint16_t class_type_counter[DecisionType::kMAX];
		uint16_t num_decisions = 0;
		std::shared_ptr<engine::lolepop::Lolepop> example_plan;
		std::shared_ptr<voila::Context> voila_context;
		PlanProfile* given_plan_profile;

		using StatementRange = voila::StatementRange;

		std::vector<StatementRange> jit_fragments;
		std::vector<StatementRange> flavor_fragments;
		std::unique_ptr<InputPlanProfile> plan_profile;

		Input(const Brain& brain, Actions* previous,
			const std::shared_ptr<engine::lolepop::Lolepop>& example_plan,
			const std::shared_ptr<voila::Context>& voila_context,
			PlanProfile* given_plan_profile);
		~Input();

		bool jit_fragment_range_overlaps(const StatementRange& range) const {
			for (auto& j : jit_fragments) {
				if (range.overlaps(j)) {
					return true;
				}
			}
			return false;
		}

		bool flavor_fragment_range_overlaps(const StatementRange& range) const {
			for (auto& j : flavor_fragments) {
				if (range.overlaps(j)) {
					return true;
				}
			}
			return false;
		}

		bool any_range_overlaps(const StatementRange& range) const {
			if (jit_fragment_range_overlaps(range)) {
				return true;
			}
			return flavor_fragment_range_overlaps(range);
		}

		InputPlanProfile* get_input_plan_profile();
	};

	struct Context {
		virtual ~Context() = default;
	};

	virtual bool can_trigger(Input& i, std::unique_ptr<RuleTrigger::Context>& context) = 0;
	virtual std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
		std::unique_ptr<RuleTrigger::Context>& context) = 0;
	virtual ~RuleTrigger() = default;

	std::string get_name() const {
		return m_name;
	}

	RuleTrigger(const std::string name) : m_name(name) {}

private:
	const std::string m_name;
};

struct RuleTriggers {
	template<typename T>
	void for_valid_triggers(RuleTrigger::Input& input, const T& push) const {
		std::unique_ptr<RuleTrigger::Context> ctx;
		for (auto& trigger : triggers) {
			if (!trigger->can_trigger(input, ctx)) {
				continue;
			}

			if (!push(trigger.get(), std::move(ctx))) {
				return;
			}
		}
	}

	size_t size() const { return triggers.size(); }

	RuleTriggers();

	RuleTrigger* get(size_t i) const { return triggers[i].get(); }

private:
	std::vector<std::unique_ptr<RuleTrigger>> triggers;
};

template<typename T>
struct TypedRuleSpaceIterator {
	virtual bool next() = 0;
	virtual RuleTrigger* get_trigger() const = 0;
	virtual std::unique_ptr<RuleTrigger::Context>& get_context() = 0;

	virtual T& get_data() = 0;
};

template<typename T>
struct RuleSpaceScan : TypedRuleSpaceIterator<T> {
	RuleSpaceScan(const RuleTriggers& triggers, RuleTrigger::Input& input)
	 : m_triggers(triggers), m_input(input) { }

	bool next() final {
		while (1) {
			m_context.reset();

			m_trigger_idx++;
			ASSERT(m_trigger_idx >= 0);
			if (m_trigger_idx >= (int64_t)m_triggers.size()) {
				return false;
			}
			m_trigger = m_triggers.get(m_trigger_idx);

			if (!m_trigger->can_trigger(m_input, m_context)) {
				continue;
			}

			return true;
		};
	}

	RuleTrigger* get_trigger() const final { return m_trigger; }
	std::unique_ptr<RuleTrigger::Context>& get_context() final { return m_context; }
	T& get_data() final { return m_data; }

protected:
	std::unique_ptr<RuleTrigger::Context> m_context;
	RuleTrigger* m_trigger;
	const RuleTriggers& m_triggers;
	RuleTrigger::Input& m_input;
	int64_t m_trigger_idx = -1;

	T m_data;
};

template<typename T, typename CHILD_T>
struct RuleSpaceFilter : TypedRuleSpaceIterator<T> {
	bool next() final {
		while (1) {
			bool r = m_child.next();
			if (!r) {
				return false;
			}

			if (!m_filter(*this)) {
				continue;
			}

			break;
		};

		return true;
	}

	RuleTrigger* get_trigger() const final { return m_child.get_trigger(); }
	std::unique_ptr<RuleTrigger::Context>& get_context() final { return m_child.get_context(); }
	T& get_data() final { return m_child.get_data(); }

	RuleSpaceFilter(CHILD_T& child,
		const std::function<bool(TypedRuleSpaceIterator<T>& iter)>& filter)
	 : m_child(child), m_filter(filter)
	{
	}

protected:
	CHILD_T& m_child;

private:
	const std::function<bool(TypedRuleSpaceIterator<T>& iter)> m_filter;
};

template<typename T, typename CHILD_T>
struct RuleSpaceGenerate : TypedRuleSpaceIterator<T> {
	bool next() final {
		while (1) {
			if (!m_has_data) {
				bool r = m_child.next();
				if (!r) {
					return false;
				}
				m_has_data = true;
			}

			ASSERT(m_has_data);
			auto& data = get_data();
			auto& context = get_context();
			auto trigger = get_trigger();
			data.generated_choices = trigger->generate(m_input,
				m_decision_factory, context);

			if (context) {
				if (data.generated_choices.empty()) {
					m_has_data = false;
				} else {
					ASSERT(!data.generated_choices.empty());
					break;
				}
			} else {
				m_has_data = false;
				if (!data.generated_choices.empty()) {
					break;
				}
			}
		};

		ASSERT(!get_data().generated_choices.empty());

		return true;
	}

	RuleTrigger* get_trigger() const final { return m_child.get_trigger(); }
	std::unique_ptr<RuleTrigger::Context>& get_context() final { return m_child.get_context(); }
	T& get_data() final { return m_child.get_data(); }

	RuleSpaceGenerate(CHILD_T& child, RuleTrigger::Input& input,
			DecisionFactory& decision_factory)
	 : m_child(child), m_input(input), m_decision_factory(decision_factory)
	{
	}

protected:
	CHILD_T& m_child;

	RuleTrigger::Input& m_input;
	DecisionFactory& m_decision_factory;

	bool m_has_data = false;
};

template<typename T>
static void
for_each_flavor(const T& f)
{
	for (int unroll_sel : {0, 4, 8}) {
		for (int unroll_nosel : {0, 8, 4, 16, 32}) {
			for (int predicated : {0, 1, 2}) {
				f(engine::voila::FlavorSpec(unroll_nosel, unroll_sel, predicated,
					false, 0 /* simdize nosel */, 1 /* simdize sel*/ ));
			}
		}
	}
}


} /* engine::adaptive */
