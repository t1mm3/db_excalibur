#include "decision.hpp"

#include "decisions.hpp"

#include "system/system.hpp"
#include "engine/budget.hpp"
#include "engine/query.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/voila/statement_identifier.hpp"

#include <sstream>

using namespace engine;
using namespace voila;
using namespace adaptive;

bool
DecisionState::operator==(const DecisionState& o) const
{
	if (node_type != o.node_type && destination_index != o.destination_index &&
			source_index != o.source_index && stable_op_id != o.stable_op_id) {
		return false;
	}


	if (!flavor_spec != !o.flavor_spec) {
		return false;
	}

	if (flavor_spec && flavor_spec != o.flavor_spec) {
		return false;
	}


	if (!set_vector_key != !o.set_vector_key) {
		return false;
	}

	if (set_vector_key && *set_vector_key != *o.set_vector_key) {
		return false;
	}


	if (!statement_range != !o.statement_range) {
		return false;
	}

	if (statement_range && *statement_range  != *o.statement_range) {
		return false;
	}

	return true;
}

size_t
DecisionState::hash() const
{
	size_t seed = 13;

	auto combine = [&] (size_t val) {
		seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
	};

	combine(std::hash<uint64_t>()(node_type));
	combine(std::hash<int64_t>()(destination_index));
	combine(std::hash<int64_t>()(source_index));
	combine(std::hash<size_t>()(stable_op_id));

	if (flavor_spec) {
		combine(std::hash<FlavorSpec>()(*flavor_spec));
	}

	if (set_vector_key) {
		combine(std::hash<SetVectorKey>()(*set_vector_key));
	}

	if (statement_range) {
		combine(std::hash<StatementRange>()(*statement_range));
	}
	return seed;
}




Decision::Decision(Type type, const std::string& dbg_name)
 : _type(type), dbg_name(dbg_name)
{
	_state.node_type = type;
}

Decision::~Decision()
{
}

void
Decision::apply_protoplan(
	DecisionContext* ctx,
	std::shared_ptr<engine::protoplan::PlanOp>& op)
{
	LOG_TRACE("%s: apply_protoplan", dbg_name.c_str());
	apply_protoplan_internal(ctx, op);
}

void
Decision::apply_protoplan_internal(
	DecisionContext* ctx,
	std::shared_ptr<engine::protoplan::PlanOp>& op)
{

}

void
Decision::apply_passes(
	DecisionContext* ctx,
	Stream& s,
	BudgetUser* budget_user)
{
	LOG_TRACE("%s: apply_passes", dbg_name.c_str());
	apply_passes_internal(ctx, s, budget_user);
}

void
Decision::apply_passes_internal(
	DecisionContext* ctx,
	Stream& stream,
	BudgetUser* budget_user)
{

}

void
Decision::apply_jit(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{
	LOG_TRACE("%s: apply_jit", dbg_name.c_str());

	apply_jit_internal(ctx, out_fuse_groups, s, stmts, operator_id, budget_user);
}

void
Decision::apply_jit_internal(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{

}

void
Decision::apply_fragment(
	DecisionContext* ctx,
	CompileRequest& request,
	FuseGroup* fuse_group)
{
	LOG_TRACE("%s: apply_fragment", dbg_name.c_str());

	apply_fragment_internal(ctx, request, fuse_group);
}

bool
Decision::get_config_long(
	long& out,
	QueryConfig& config,
	ConfigOption key) const
{
	LOG_TRACE("%s: get_config_long", dbg_name.c_str());

	return get_config_long_internal(out, config, key);
}

bool
Decision::get_config_bool(
	bool& out,
	QueryConfig& config,
	ConfigOption key) const
{
	LOG_TRACE("%s: get_config_bool", dbg_name.c_str());

	return get_config_bool_internal(out, config, key);
}


void
Decision::apply_fragment_internal(
	DecisionContext* ctx,
	CompileRequest& request,
	FuseGroup* fuse_group)
{

}

bool
Decision::get_config_long_internal(
	long& out,
	QueryConfig& config,
	ConfigOption key) const
{
	return false;
}

bool
Decision::get_config_bool_internal(
	bool& out,
	QueryConfig& config,
	ConfigOption key) const
{
	return false;
}

std::unique_ptr<DecisionContext>
Decision::make_decision_context()
{
	return nullptr;
}

void
Decision::to_string(std::ostream& o) const
{
	o << get_dbg_name();
}

size_t
Decision::gowers_distance_count()
{
	return DecisionState::gowers_distance_count();
}

double
Decision::gowers_distance(size_t& count, const IDecision* o) const
{
	ASSERT(o);
	DBG_ASSERT(dynamic_cast<const Decision*>(o));
	auto dec = static_cast<const Decision*>(o);
	ASSERT(dec);

	count = gowers_distance_count();
	return _state.gowers_distance_diff(dec->_state);
}




void
Actions::apply_protoplan_internal(
	DecisionContext* ctx,
	std::shared_ptr<engine::protoplan::PlanOp>& op)
{
	auto actions_context = dynamic_cast<ActionsContext*>(ctx);
	ASSERT(actions_context);

	for_each_bottomup_with_index([&] (auto i, auto decision) {
		decision->apply_protoplan(actions_context->contexts[i].get(),
			op);
	});
}

void
Actions::apply_passes_internal(
	DecisionContext* ctx,
	Stream& stream,
	BudgetUser* budget_user)
{
	auto actions_context = dynamic_cast<ActionsContext*>(ctx);
	ASSERT(actions_context);

	for_each_bottomup_with_index([&] (auto i, auto decision) {
		decision->apply_passes(actions_context->contexts[i].get(),
			stream, budget_user);
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "Actions");
		}
	});
}

void
Actions::apply_jit_internal(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{
	auto actions_context = dynamic_cast<ActionsContext*>(ctx);
	ASSERT(actions_context);

	for_each_bottomup_with_index([&] (auto i, auto decision) {
		decision->apply_jit(actions_context->contexts[i].get(),
			out_fuse_groups, s, stmts, operator_id, budget_user);
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "Actions");
		}
	});
}

void
Actions::apply_fragment_internal(
	DecisionContext* ctx,
	CompileRequest& request,
	FuseGroup* fuse_group)
{
	auto actions_context = dynamic_cast<ActionsContext*>(ctx);
	ASSERT(actions_context);

	for_each_bottomup_with_index([&] (auto i, auto decision) {
		decision->apply_fragment(actions_context->contexts[i].get(),
			request, fuse_group);
	});
}

bool
Actions::get_config_long_internal(
	long& out,
	QueryConfig& config,
	ConfigOption key) const
{
	bool found = false;
	for_each([&] (auto decision) {
		found |= decision->get_config_long(out, config, key);
	});
	return found;
}

bool
Actions::get_config_bool_internal(
	bool& out,
	QueryConfig& config,
	ConfigOption key) const
{
	bool found = false;
	for_each([&] (auto decision) {
		found |= decision->get_config_bool(out, config, key);
	});
	return found;
}

void
Actions::to_string(std::ostream& o) const
{
	if (previous) {
		previous->to_string(o);
		o << " -> ";
	}

	if (decision) {
		decision->to_string(o);
	}
}

std::string
Actions::to_string() const
{
	std::stringstream ss;
	to_string(ss);
	return ss.str();
}

static void
materialize_actions_rev(std::vector<const Decision*>& result,
	const Actions* actions)
{
	if (!actions) {
		return;
	}

	result.push_back(actions->get_decision());
	materialize_actions_rev(result, actions->get_previous());
}

double
Actions::gowers_distance(std::vector<const Decision*>& left_tmp,
	std::vector<const Decision*>& right_tmp,
	size_t& count, const IDecision* o) const
{
	DBG_ASSERT(dynamic_cast<const Actions*>(o));
	auto dec = static_cast<const Actions*>(o);

	left_tmp.clear();
	right_tmp.clear();

	const Actions* left = this;
	const Actions* right = dec;

	materialize_actions_rev(left_tmp, left);
	materialize_actions_rev(right_tmp, right);

	auto left_sz = (int64_t)left_tmp.size();
	auto right_sz = (int64_t)right_tmp.size();

	double sum = .0;
	double discount_factor = 1.0;
	const double discount_rate = 1.2;

	int64_t min = std::min(left_sz, right_sz);

	int64_t tail_idx = 0;
	for (; tail_idx<min; tail_idx++) {
		int64_t left_idx = left_sz-1-tail_idx;
		int64_t right_idx = right_sz-1-tail_idx;

		ASSERT(left_idx >= 0 && left_idx < left_sz);
		ASSERT(right_idx >= 0 && right_idx < right_sz);

		size_t c = 0;
		double s = left_tmp[left_idx]->gowers_distance(c, right_tmp[right_idx]);
		ASSERT(s / (double)c <= 1.0);

		count += c;
		sum += s / discount_factor;

		discount_factor /= discount_rate;
	}

	// ASSERT(sum / (double)count <= 1.0);

	// remainder
	auto c = Decision::gowers_distance_count();

	if (tail_idx<left_sz || tail_idx<right_sz) {
		ASSERT((tail_idx<left_sz) != (tail_idx<right_sz));
	}
	// remainder on left
	for (; tail_idx<left_sz; tail_idx++) {
		sum += discount_factor* (double)c;
		count += c;
		discount_factor /= discount_rate;
	}

	// remainder on right
	for (; tail_idx<right_sz; tail_idx++) {
		sum += discount_factor* (double)c;
		count += c;
		discount_factor /= discount_rate;
	}

	// ASSERT(sum / (double)count <= 1.0);
	return sum;
}

double
Actions::gowers_distance(size_t& count, const IDecision* o) const
{
	std::vector<const Decision*> tmp_actions1;
	std::vector<const Decision*> tmp_actions2;

	return gowers_distance(tmp_actions1, tmp_actions2, count, o);
}


std::shared_ptr<ActionsContext>
ActionsContext::make_from_actions(Actions* actions)
{
	std::shared_ptr<ActionsContext> result(std::make_shared<ActionsContext>());

	if (!actions) {
		return result;
	}
	actions->for_each_bottomup([&] (auto action) {
		result->contexts.emplace_back(action->make_decision_context());
	});

	return result;
}



bool
DecisionConfigGetter::get_long(long& out, Actions* decision, QueryConfig& config,
	ConfigOption key)
{
	bool found = false;

	if (decision) {
		found = decision->get_config_long(out, config, key);
	}

	if (!found) {
		switch (key) {
		case ConfigOption::kVectorSize:
			out = config.vector_size();
			return true;

		case ConfigOption::kBitScoreDivisor:
			out = config.full_evaluation_bit_score_divisor();
			ASSERT(out > 0);
			return true;

		default:
			ASSERT(false && "Unhandled");
			break;
		}
	}

	return found;
}

long
DecisionConfigGetter::get_long_assert(Actions* decision, QueryConfig& config,
	ConfigOption key)
{
	long result;
	bool found = get_long(result, decision, config, key);
	ASSERT(found);

	return result;
}

bool
DecisionConfigGetter::get_bool(bool& out, Actions* decision, QueryConfig& config,
	ConfigOption key)
{
	bool found = false;

	if (decision) {
		found = decision->get_config_bool(out, config, key);
	}

	if (!found) {
		switch (key) {
		case kCanFullEval:
			out = config.has_full_evaluation();
			found = true;
			break;

		case kEnableSimdOpts:
			out = config.has_simd_opts();
			found = true;
			break;
		default:
			ASSERT(false && "Unhandled");
			break;
		}
	}

	return found;
}

bool
DecisionConfigGetter::get_bool_assert(Actions* decision, QueryConfig& config,
	ConfigOption key)
{
	bool result;
	bool found = get_bool(result, decision, config, key);
	ASSERT(found);

	return result;
}

#include "engine/voila/statement_identifier.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/adaptive/decisions.hpp"

using FlavorSpec = engine::voila::FlavorSpec;
using StatementRange = engine::voila::StatementRange;

size_t
DecisionState::gowers_distance_count()
{
	return 2
		+ FlavorSpec::gowers_distance_count()
		+ 1 + 1 + 1
		+ SetVectorKey::gowers_distance_count()
		+ StatementRange::gowers_distance_count();
}

double
DecisionState::gowers_distance_diff(const DecisionState& o) const
{
	double sum = 0;

	if (node_type != o.node_type) {
		sum += 2.0;
	}

	if (flavor_spec && o.flavor_spec) {
		sum += flavor_spec->gowers_distance_diff(*o.flavor_spec);
	} else if (!(!flavor_spec && !o.flavor_spec)) {
		sum += 1.0 * (double)FlavorSpec::gowers_distance_count();
	}

	if (destination_index != o.destination_index) {
		sum += 1.0;
	}

	if (source_index != o.source_index) {
		sum += 1.0;
	}

	if (stable_op_id != o.stable_op_id) {
		sum += 1.0;
	}

	if (set_vector_key && o.set_vector_key) {
		sum += set_vector_key->gowers_distance_diff(*o.set_vector_key);
	} else if (!(!set_vector_key && !o.set_vector_key)) {
		sum += 1.0 * (double)SetVectorKey::gowers_distance_count();
	}

	if (statement_range && o.statement_range) {
		sum += statement_range->gowers_distance_diff(*o.statement_range);
	} else if (!(!statement_range && !o.statement_range)) {
		sum += 1.0 * (double)StatementRange::gowers_distance_count();
	}

	return sum;
}
