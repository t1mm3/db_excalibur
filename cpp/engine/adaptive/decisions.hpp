#pragma once

#include "decision.hpp"

#include "engine/voila/flavor.hpp"
#include "engine/voila/statement_identifier.hpp"

#include <unordered_map>
#include <functional>

namespace engine::adaptive {

enum DecisionType {
	kDummy = 0,
	kSetDefaultFlavor,
	kDataCentric,
	kInline,
	kSetVectorSize,
	kSetScopeFlavor,
	kJitStatementFragment,
	kSwapOperators,
	kEnableBloomFilter,
	kJitExpressions,

	kMAX
};


struct Dummy : Decision {
	Dummy() : Decision(DecisionType::kDummy, "Dummy") {}
};

struct DataCentric : Decision {
	using FlavorSpec = engine::voila::FlavorSpec;
	DataCentric(const std::shared_ptr<FlavorSpec>& spec);

	void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) override;

	void to_string(std::ostream& o) const override;

	FlavorSpec& get_flavor_spec() const {
		return *_state.flavor_spec;
	}
};

struct JitExpressions : Decision {
	using FlavorSpec = engine::voila::FlavorSpec;
	JitExpressions(const std::shared_ptr<FlavorSpec>& spec);

	void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) override;

	void to_string(std::ostream& o) const override;

	FlavorSpec& get_flavor_spec() const {
		return *_state.flavor_spec;
	}
};

struct Inline : Decision {
	Inline() : Decision(DecisionType::kInline, "Inline") {}

	void apply_passes_internal(DecisionContext* ctx,
		Stream& stream, BudgetUser* budget_user) override;
};

struct SwapOperatorsValue {
	int64_t destination_index = -1;
	int64_t source_index = -1;

	bool operator==(const SwapOperatorsValue& o) const {
		return o.destination_index == destination_index &&
			o.source_index == source_index;
	}
};

struct SwapOperators : Decision {
	SwapOperators(const SwapOperatorsValue& value);

	void apply_protoplan_internal(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op) override;

	void to_string(std::ostream& o) const override;

	const SwapOperatorsValue value;
};

struct EnableBloomFilter : Decision {
	EnableBloomFilter(size_t stable_op_id, size_t bits);

	void apply_protoplan_internal(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op) override;

	void to_string(std::ostream& o) const override;

	const size_t stable_op_id;
	const size_t bits;
};


struct SetDefaultFlavor : Decision {
	using FlavorSpec = engine::voila::FlavorSpec;
	SetDefaultFlavor(const std::shared_ptr<engine::voila::FlavorSpec>& spec);

	void apply_fragment_internal(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group) override;

	void to_string(std::ostream& o) const override;

	FlavorSpec& get_flavor_spec() const {
		return *_state.flavor_spec;
	}
};

struct SetVectorKey {
	int64_t vector_size;
	bool full_eval;
	int bit_score_divisor;
	bool simd_opts;

	static const int64_t kMinVectorSize = 256;
	static const int64_t kMaxVectorSize = 1024;

	static const int kMinBitScopeDivisor = 16;
	static const int kMaxBitScopeDivisor = 64;

	SetVectorKey(int64_t vector_size, bool full_eval, int bit_score_divisor, bool simd_opts);

	bool operator==(const SetVectorKey& o) const {
		return vector_size == o.vector_size && full_eval == o.full_eval &&
			bit_score_divisor == o.bit_score_divisor && simd_opts == o.simd_opts;
	}

	bool operator!=(const SetVectorKey& o) const { return !(*this == o); }

	static size_t gowers_distance_count() {
		return 3;
	}

	double gowers_distance_diff(const SetVectorKey& o) const {
		double sum = 0;

		if (full_eval != o.full_eval) {
			sum += 1.0;
		}
		sum += std::abs((double)(vector_size - o.vector_size)) /
			(double)(kMaxVectorSize - kMinVectorSize + 1);
		sum += std::abs((double)(bit_score_divisor - o.bit_score_divisor)) /
			(double)(kMaxBitScopeDivisor - kMinBitScopeDivisor + 1);

		return sum;
	}

	static void for_each_vector_size(const std::function<void(SetVectorKey&&)>& f);
};

struct SetVectorSize : Decision {
	SetVectorSize(const std::shared_ptr<SetVectorKey>& values);

	bool get_config_long_internal(
		long& out,
		QueryConfig& config,
		ConfigOption key) const override;

	bool get_config_bool_internal(
		bool& out,
		QueryConfig& config,
		ConfigOption key) const override;

	void to_string(std::ostream& o) const override;
};


struct JitStatementFragmentOpts {
	double selectivity_margin = 0.10;
	double mem_max_in_cache_cyc_tup = 10;

	// -1 means maybe, 0 no, 1 yes
	int can_pass_sel = 1;

	int can_pass_mem = 1;

	int can_pass_complex = 1;

	bool operator==(const JitStatementFragmentOpts& o) const {
		return o.can_pass_sel == can_pass_sel && o.can_pass_mem == can_pass_mem &&
			o.selectivity_margin == selectivity_margin &&
			o.mem_max_in_cache_cyc_tup == mem_max_in_cache_cyc_tup &&
			o.can_pass_complex == can_pass_complex;
	}

	void to_string(std::ostream& o) const;
};

struct JitStatementFragmentId {
	using StatementRange = engine::voila::StatementRange;
	using FlavorSpec = engine::voila::FlavorSpec;

	const std::shared_ptr<engine::voila::FlavorSpec> flavor;
	const std::shared_ptr<StatementRange> range;

	JitStatementFragmentId(
		const std::shared_ptr<FlavorSpec>& flavor,
		const std::shared_ptr<StatementRange>& range)
	 : flavor(flavor), range(range)
	{
	}


	bool operator==(const JitStatementFragmentId& o) const {
		return *o.flavor == *flavor && *o.range == *range;
	}
};


struct JitStatementFragment : Decision {
	using StatementRange = engine::voila::StatementRange;

	JitStatementFragment(
		const std::shared_ptr<StatementRange>& statement_range,
		const std::shared_ptr<engine::voila::FlavorSpec>& spec);

	void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) override;

	void to_string(std::ostream& o) const override;

	StatementRange get_statement_range() const {
		return *_state.statement_range;
	}
};

struct SetScopeFlavor : Decision {
	using StatementRange = engine::voila::StatementRange;

	SetScopeFlavor(
		const std::shared_ptr<StatementRange>& statement_range,
		const std::shared_ptr<engine::voila::FlavorSpec>& flavor_spec);

	void apply_fragment_internal(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group) override;

	void to_string(std::ostream& o) const override;

	StatementRange get_statement_range() const {
		return *_state.statement_range;
	}
};

} /* engine::adaptive */


namespace std {
template <>
struct hash<engine::adaptive::JitStatementFragmentOpts>
{
	std::size_t operator()(const engine::adaptive::JitStatementFragmentOpts& k) const
	{
		using std::size_t;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<int>()(k.can_pass_sel));
		combine(hash<int>()(k.can_pass_mem));
		combine(hash<double>()(k.selectivity_margin));
		combine(hash<double>()(k.mem_max_in_cache_cyc_tup));
		combine(hash<int>()(k.can_pass_complex));
		return seed;
	}
};

template <>
struct hash<engine::adaptive::JitStatementFragmentId>
{
	std::size_t operator()(const engine::adaptive::JitStatementFragmentId& k) const
	{
		using std::size_t;
		using engine::voila::StatementIdentifier;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<engine::voila::FlavorSpec>()(*k.flavor));
		combine(hash<engine::voila::StatementRange>()(*k.range));
		return seed;
	}
};

template <>
struct hash<engine::adaptive::SetVectorKey>
{
	std::size_t operator()(const engine::adaptive::SetVectorKey& k) const
	{
		using std::size_t;
		using engine::voila::StatementIdentifier;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<size_t>()(k.vector_size));
		combine(hash<bool>()(k.full_eval));
		combine(hash<int>()(k.bit_score_divisor));
		combine(hash<bool>()(k.simd_opts));
		return seed;
	}
};

template <>
struct hash<engine::adaptive::SwapOperatorsValue>
{
	std::size_t operator()(const engine::adaptive::SwapOperatorsValue& k) const
	{
		using std::size_t;
		using engine::voila::StatementIdentifier;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<int64_t>()(k.source_index));
		combine(hash<int64_t>()(k.destination_index));
		return seed;
	}
};

} /* std */

namespace engine::adaptive {

struct DecisionFactory {
	using FlavorSpec = engine::voila::FlavorSpec;
	using StatementRange = engine::voila::StatementRange;

	Decision* new_inline();

	Decision* new_data_centric(const std::shared_ptr<FlavorSpec>& flavor);
	Decision* new_jit_expressions(const std::shared_ptr<FlavorSpec>& flavor);

	Decision* new_set_default_flavor(const std::shared_ptr<FlavorSpec>& flavor);
	Decision* new_set_vector_size(const std::shared_ptr<SetVectorKey>& key);

	Decision* new_jit_statement_fragment(
		const std::shared_ptr<StatementRange>& range,
		const std::shared_ptr<FlavorSpec>& flavor);

	Decision* new_set_scope_flavor(
		const std::shared_ptr<StatementRange>& range,
		const std::shared_ptr<FlavorSpec>& flavor);

	Decision* new_swap_operators(const SwapOperatorsValue& val);
	Decision* new_enable_bloom_filter(size_t stable_op_id, size_t bits);

	Decision* new_from_state(const DecisionState& state);

	DecisionFactory();
private:
	Decision* dec_inline;

	std::unordered_map<FlavorSpec, Decision*> set_DataCentric;
	std::unordered_map<FlavorSpec, Decision*> set_JitExpressions;
	std::unordered_map<FlavorSpec, Decision*> set_SetDefaultFlavor;
	std::unordered_map<SetVectorKey, Decision*> set_SetVectorSize;
	std::unordered_map<JitStatementFragmentId, Decision*> set_JitStatementFragment;
	std::unordered_map<JitStatementFragmentId, Decision*> set_SetScopeFlavor;
	std::unordered_map<SwapOperatorsValue, Decision*> set_SwapOperators;
	std::unordered_map<size_t, Decision*> set_EnableBloomFilter1;
	std::unordered_map<size_t, Decision*> set_EnableBloomFilter2;

	std::vector<std::unique_ptr<Decision>> allocated;
};


} /* engine::adaptive */
