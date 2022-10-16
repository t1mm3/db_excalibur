#pragma once

#include "rule_triggers.hpp"
#include "decisions.hpp"

#include "engine/voila/voila.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/voila/jit_prepare_pass.hpp"
#include "engine/voila/statement_fragment_selection_pass.hpp"
#include "engine/loleplan_passes.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/lolepops/voila_lolepop.hpp"

namespace engine::relop {
struct HashJoin;
} /* engine::relop */

namespace engine::adaptive::triggers {

struct SetDefaultFlavor : RuleTrigger {
	using Decision = engine::adaptive::Decision;
	using FlavorSpec = engine::voila::FlavorSpec;

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context);

	SetDefaultFlavor(const FlavorSpec& spec);

private:
	const std::shared_ptr<FlavorSpec> spec;
};

struct SetVectorSize : RuleTrigger {
	using Decision = engine::adaptive::Decision;

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context);

	SetVectorSize(const SetVectorKey& values);

private:
	const std::shared_ptr<SetVectorKey> values;
};

struct DataCentricStrategy : RuleTrigger {
	using Decision = engine::adaptive::Decision;
	using FlavorSpec = engine::voila::FlavorSpec;

	bool can_trigger(Input& input,
			std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context) override;

	DataCentricStrategy(const FlavorSpec& spec, bool inline_all);

private:
	const std::shared_ptr<FlavorSpec> spec;
	const bool inline_all;
};


struct JitExpressionsStrategy : RuleTrigger {
	using Decision = engine::adaptive::Decision;
	using FlavorSpec = engine::voila::FlavorSpec;

	bool can_trigger(Input& input,
			std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context) override;

	JitExpressionsStrategy(const FlavorSpec& spec);

private:
	const std::shared_ptr<FlavorSpec> spec;
};


struct FindJittableFragment : RuleTrigger {
	using Decision = engine::adaptive::Decision;
	using Stmt = engine::voila::Stmt;
	using FuseGroups = engine::voila::FuseGroups;
	using StatementFragmentSelection = engine::voila::StatementFragmentSelection;
	using StatementFragmentSelectionPass = engine::voila::StatementFragmentSelectionPass;
	using Candidate = engine::voila::Candidate;
	using StatementIdentifier = engine::voila::StatementIdentifier;
	using StatementRange = engine::voila::StatementRange;
	using FlavorSpec = engine::voila::FlavorSpec;

	struct LocalContext : RuleTrigger::Context {
		size_t num_voila_ops = 0;
		size_t num_ops = 0;
		size_t current_op = 0;
		size_t iteration = 0;

		bool has_inline = true;
	};

	JitStatementFragmentOpts jit_opts;

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;
	std::vector<Decision*> generate(Input& input, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& _context);

	size_t statements(std::vector<Decision*>& result,
			const Input& input,
			DecisionFactory& factory,
			const std::vector<Stmt>& stmts,
			LocalContext& context,
			std::vector<size_t> line_numbers,
			FuseGroups* fuse_groups,
			const StatementFragmentSelection::ConstructCandidateOpts& opts);

	FindJittableFragment(const FlavorSpec& spec, int profile_guided,
		bool requires_inline = true);
private:
	const std::shared_ptr<FlavorSpec> spec;
	const int profile_guided;
	const bool requires_inline = true;
	bool needs_profile;
	bool data_centric;
};

struct PushDownMostSelectiveFilter : RuleTrigger {
	using Decision = engine::adaptive::Decision;

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context);

	PushDownMostSelectiveFilter()
	 : RuleTrigger("PushDownMostSelectiveFilter") {}

};

struct EnableBloomFilterForHighestSelJoin : RuleTrigger {
	using Decision = engine::adaptive::Decision;

	struct LocalContext : RuleTrigger::Context {
		struct Entry {
			std::shared_ptr<engine::relop::HashJoin> rel_op;
			double sel;
		};

		const std::vector<Entry> qualifying_joins;
		size_t curr_index = 0;

		LocalContext(std::vector<Entry>&& qualifying_joins)
		 : qualifying_joins(std::move(qualifying_joins))
		{}
	};

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
			std::unique_ptr<RuleTrigger::Context>& context);

	EnableBloomFilterForHighestSelJoin(size_t num_bits)
	 : RuleTrigger("EnableBloomFilterForHighestSelJoin(" +
	 	std::to_string(num_bits) +")"), num_bits(num_bits)
	{}

	const size_t num_bits;
};

struct SetScopeFlavor : RuleTrigger {
	using Decision = engine::adaptive::Decision;
	using FlavorSpec = engine::voila::FlavorSpec;

	bool can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context) override;

	std::vector<Decision*> generate(Input& i, DecisionFactory& factory,
		std::unique_ptr<RuleTrigger::Context>& context) override;

	SetScopeFlavor(const FlavorSpec& flavor)
	 : RuleTrigger("SetScopeFlavor"), flavor(std::make_shared<FlavorSpec>(flavor)) {}
private:
	const std::shared_ptr<FlavorSpec> flavor;
};

} /* engine::adaptive::triggers */
