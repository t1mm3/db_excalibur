#include "decisions.hpp"

#include "engine/stream.hpp"
#include "engine/query.hpp"
#include "engine/voila/voila.hpp"
#include "engine/voila/jit.hpp"
#include "engine/voila/statement_fragment_selection_pass.hpp"
#include "engine/voila/jit_prepare_pass.hpp"
#include "engine/loleplan_passes.hpp"
#include "engine/voila/compile_request.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/budget.hpp"

#include "engine/protoplan.hpp"

using namespace engine;
using namespace adaptive;

DataCentric::DataCentric(const std::shared_ptr<FlavorSpec>& flavor_spec)
 : Decision(DecisionType::kDataCentric, "DataCentric")
{
	_state.flavor_spec = flavor_spec;
}

void
DataCentric::apply_jit_internal(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<engine::voila::Stmt>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{
	ASSERT(_state.flavor_spec);

	auto& stream = s.block.stream;
	engine::voila::StatementFragmentSelectionPass jit_prepare_pass(
		stream.query.sys_context,
		stream.query.config.get(),
		*stream.execution_context.voila_context,
		s.budget_manager,
		out_fuse_groups,
		nullptr,
		operator_id);

	jit_prepare_pass.flavor_spec = _state.flavor_spec;

	jit_prepare_pass(stmts);
}

void
DataCentric::to_string(std::ostream& o) const
{
	ASSERT(_state.flavor_spec);

	o << get_dbg_name() << "(";
	o << "flavor=" << _state.flavor_spec->generate_signature();
	o << ")";
}




JitExpressions::JitExpressions(const std::shared_ptr<FlavorSpec>& flavor_spec)
 : Decision(DecisionType::kJitExpressions, "JitExpressions")
{
	_state.flavor_spec = flavor_spec;
}

void
JitExpressions::apply_jit_internal(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<engine::voila::Stmt>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{
	ASSERT(_state.flavor_spec);

	auto& stream = s.block.stream;

	// extract complex code fragments etc
	engine::voila::JitPrepareExpressionGraphPass jit_prepare_graph_pass;
	jit_prepare_graph_pass(stmts);

	engine::voila::JitPreparePass jit_prepare_pass(jit_prepare_graph_pass,
		stream.query.sys_context,
		stream.query.config.get(),
		*stream.execution_context.voila_context,
		s.budget_manager,
		out_fuse_groups);

	jit_prepare_pass.flavor_spec = _state.flavor_spec;
	jit_prepare_pass(stmts);
}

void
JitExpressions::to_string(std::ostream& o) const
{
	ASSERT(_state.flavor_spec);

	o << get_dbg_name() << "(";
	o << "flavor=" << _state.flavor_spec->generate_signature();
	o << ")";
}





void
Inline::apply_passes_internal(
	DecisionContext* ctx,
	Stream& stream,
	BudgetUser* budget_user)
{
	auto first_op = LoleplanPass::skip_first_non_voila_ops(
		stream.execution_context.root);

	PrinterPass printer;
	if (LOG_WILL_LOG(DEBUG)) {
		printer(first_op);
	}
	InlinePass inliner(*stream.execution_context.voila_context);
	inliner(first_op);

	if (LOG_WILL_LOG(DEBUG)) {
		printer(first_op);
	}
}


SwapOperators::SwapOperators(const SwapOperatorsValue& value)
 : Decision(DecisionType::kSwapOperators, "SwapOperators"),
	value(value)
{
	ASSERT(value.destination_index >= 0);
	ASSERT(value.source_index >= 0);
	ASSERT(value.source_index < value.destination_index);

	_state.destination_index = value.destination_index;
	_state.source_index = value.source_index;
}

static bool
find_op_with_id(
	std::shared_ptr<engine::protoplan::PlanOp>& out_parent_op,
	std::shared_ptr<engine::protoplan::PlanOp>& out_op,
	const std::shared_ptr<engine::protoplan::PlanOp>& op,
	int64_t seek_idx)
{
	out_parent_op.reset();
	out_op.reset();

	bool found = false;
	std::shared_ptr<engine::protoplan::PlanOp> prev;

	LoleplanPass::apply_source_to_sink_with_id(op, [&] (auto op, auto idx) {
		if (found) {
			if (!out_parent_op) {
				out_parent_op = op;
			}
			return;
		}
		if (op && op->stable_op_id == seek_idx) {
			found = true;
			out_op = op;
		}

		prev = op;
	});
	return found;
}

#include "engine/lolepops/scan.hpp"

void
SwapOperators::apply_protoplan_internal(
	DecisionContext* ctx,
	std::shared_ptr<engine::protoplan::PlanOp>& op)
{
	std::shared_ptr<engine::protoplan::PlanOp> src_parent;
	std::shared_ptr<engine::protoplan::PlanOp> src_op;
	std::shared_ptr<engine::protoplan::PlanOp> dst_parent;
	std::shared_ptr<engine::protoplan::PlanOp> dst_op;

	bool found_src = find_op_with_id(src_parent, src_op, op, value.source_index);
	ASSERT(found_src && src_op);
	if (src_parent) {
		ASSERT(src_parent->child == src_op);
	}
	bool found_dst = find_op_with_id(dst_parent, dst_op, op, value.destination_index);
	ASSERT(found_dst && dst_op);
	if (dst_parent) {
		ASSERT(dst_parent->child == dst_op);
	}

	ASSERT(src_op != dst_op);
	ASSERT(src_parent != dst_parent);
	ASSERT(src_parent && dst_parent && "TODO: makes life a bit easier, for now");

	std::swap(src_parent->child, dst_parent->child);
	std::swap(src_op->child, dst_op->child);
}

void
SwapOperators::to_string(std::ostream& o) const
{
	o << get_dbg_name() << "(src=" << value.source_index
		<< ", dest=" << value.destination_index << ")";
}



EnableBloomFilter::EnableBloomFilter(size_t stable_op_id, size_t bits)
 : Decision(DecisionType::kEnableBloomFilter,
 		"EnableBloomFilter(" + std::to_string(bits) + ")"),
	stable_op_id(stable_op_id), bits(bits)
{
	_state.stable_op_id = stable_op_id;
	ASSERT(bits == 1 || bits == 2);
	_state.source_index = bits;
}

void
EnableBloomFilter::apply_protoplan_internal(
	DecisionContext* ctx,
	std::shared_ptr<engine::protoplan::PlanOp>& op)
{
	bool found = false;

	std::shared_ptr<engine::protoplan::PlanOp> needle_op;

	LoleplanPass::apply_source_to_sink_with_id(op, [&] (auto op, auto idx) {
		if (found) {
			return;
		}

		ASSERT(op->rel_op);
		if (op->rel_op->get_stable_op_id() == stable_op_id) {
			if (auto probe_op = dynamic_cast<engine::protoplan::JoinProbeDriver*>(op.get())) {
				found = true;
				needle_op = op;
			}
		}
	});

	ASSERT(needle_op && "Must exist, why else would we have introduce this Decision");

	if (auto probe_op = dynamic_cast<engine::protoplan::JoinProbeDriver*>(needle_op.get())) {
		ASSERT(!probe_op->bloom_filter_bits);
		ASSERT(bits > 0);
		probe_op->bloom_filter_bits = bits;

		LOG_DEBUG("EnableBloomFilter: apply_protoplan_internal: enable");
	} else {
		ASSERT(false && "Invalid operator");
	}
}

void
EnableBloomFilter::to_string(std::ostream& o) const
{
	o << get_dbg_name() << "(op=" << stable_op_id << ")";
}




SetDefaultFlavor::SetDefaultFlavor(
	const std::shared_ptr<engine::voila::FlavorSpec>& flavor_spec)
 : Decision(DecisionType::kSetDefaultFlavor, "SetDefaultFlavor")
{
	_state.flavor_spec = flavor_spec;
}

void
SetDefaultFlavor::apply_fragment_internal(
	DecisionContext* ctx,
	engine::voila::CompileRequest& request,
	engine::voila::FuseGroup* fuse_group)
{
	ASSERT(_state.flavor_spec);

	LOG_DEBUG("SetDefaultFlavor: set request %p to '%s'",
		&request, _state.flavor_spec->generate_signature().c_str());
	request.flavor = _state.flavor_spec;
}

void
SetDefaultFlavor::to_string(std::ostream& o) const
{
	ASSERT(_state.flavor_spec);

	o << get_dbg_name() << "(" << _state.flavor_spec->generate_signature() << ")";
}



SetVectorKey::SetVectorKey(int64_t vector_size, bool full_eval,
	int bit_score_divisor, bool simd_opts)
 : vector_size(vector_size), full_eval(full_eval),
	bit_score_divisor(bit_score_divisor), simd_opts(simd_opts)
{
	ASSERT(vector_size >= kMinVectorSize);
	ASSERT(vector_size <= kMaxVectorSize);
	ASSERT(bit_score_divisor >= kMinBitScopeDivisor);
	ASSERT(bit_score_divisor <= kMaxBitScopeDivisor);
}

void
SetVectorKey::for_each_vector_size(const std::function<void(SetVectorKey&&)>& f)
{
	for (bool simd_opts : {true}) { // , false
		for (int bit_score_div : {24, 32, 64}) { //, 16
			for (bool full_eval : {true, false}) {
				for (size_t vsize : {1024, 512, 256}) {
					if (!full_eval && bit_score_div != 24) {
						continue;
					}
					f(SetVectorKey(vsize, full_eval, bit_score_div, simd_opts));
				}
			}
		}
	}
}


SetVectorSize::SetVectorSize(const std::shared_ptr<SetVectorKey>& values)
 : Decision(DecisionType::kSetVectorSize, "SetVectorSize")
{
	_state.set_vector_key = values;
}

bool
SetVectorSize::get_config_long_internal(
	long& out,
	QueryConfig& config,
	ConfigOption key) const
{
	ASSERT(_state.set_vector_key);
	const auto& values = *_state.set_vector_key;

	switch (key) {
	case ConfigOption::kVectorSize:
		out = values.vector_size;
		return true;

	case ConfigOption::kBitScoreDivisor:
		out = values.bit_score_divisor;
		return true;

	default:
		break;
	}

	return false;
}

bool
SetVectorSize::get_config_bool_internal(
	bool& out,
	QueryConfig& config,
	ConfigOption key) const
{
	ASSERT(_state.set_vector_key);
	const auto& values = *_state.set_vector_key;
	bool found = false;

	switch (key) {
	case ConfigOption::kCanFullEval:
		out = values.full_eval;
		found = true;
		break;

	case ConfigOption::kEnableSimdOpts:
		out = values.simd_opts;
		found = true;
		break;

	default:
		found = false;
		break;
	}

	return found;
}


void
SetVectorSize::to_string(std::ostream& o) const
{
	ASSERT(_state.set_vector_key);
	const auto& values = *_state.set_vector_key;

	o << get_dbg_name() << "(vsize=" << values.vector_size
		<< ", full=" << values.full_eval
		<< ", bit_div=" << values.bit_score_divisor
		<< ", simd_opts=" << values.simd_opts
		<< ")";
}





void
JitStatementFragmentOpts::to_string(std::ostream& o) const
{
	o << "sel=" << can_pass_sel << ", mem=" << can_pass_mem;
}



JitStatementFragment::JitStatementFragment(
	const std::shared_ptr<StatementRange>& statement_range,
	const std::shared_ptr<engine::voila::FlavorSpec>& flavor_spec)
 : Decision(DecisionType::kJitStatementFragment, "JitStatementFragment")
{
	_state.flavor_spec = flavor_spec;
	_state.statement_range = statement_range;
}

void
JitStatementFragment::apply_jit_internal(
	DecisionContext* ctx,
	engine::voila::FuseGroups* out_fuse_groups,
	engine::voila::CodeBlockBuilder& s,
	const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
	size_t operator_id,
	BudgetUser* budget_user)
{
	ASSERT(_state.statement_range);
	_state.statement_range->assert_valid();

	if (_state.statement_range->begin.operator_id != operator_id) {
		return;
	}

	ASSERT(_state.flavor_spec);
	JitStatementFragmentId given_fragment(_state.flavor_spec, _state.statement_range);

	auto& stream = s.block.stream;
	engine::voila::StatementFragmentSelectionPass jit_prepare_pass(
		stream.query.sys_context,
		stream.query.config.get(),
		*stream.execution_context.voila_context,
		s.budget_manager,
		out_fuse_groups,
		&given_fragment,
		operator_id);

	jit_prepare_pass.flavor_spec = _state.flavor_spec;

	jit_prepare_pass(stmts);

	ASSERT(jit_prepare_pass.found_given_fragment() == 1 && "Must exist");
}

void
JitStatementFragment::to_string(std::ostream& o) const
{
	ASSERT(_state.flavor_spec);
	ASSERT(_state.statement_range);

	o << get_dbg_name() << "(";
	o << "flavor=" << _state.flavor_spec->generate_signature();
	o << ", range=";
	_state.statement_range->to_string(o);
	o << ")";
}



SetScopeFlavor::SetScopeFlavor(
	const std::shared_ptr<StatementRange>& statement_range,
	const std::shared_ptr<engine::voila::FlavorSpec>& flavor_spec)
 : Decision(DecisionType::kSetScopeFlavor, "SetScopeFlavor")
{
	_state.flavor_spec = flavor_spec;
	_state.statement_range = statement_range;
}

void
SetScopeFlavor::apply_fragment_internal(
	DecisionContext* ctx,
	engine::voila::CompileRequest& request,
	engine::voila::FuseGroup* fuse_group)
{
	ASSERT(_state.statement_range);
	auto& statement_range = *_state.statement_range;
	size_t num_overlap = 0;
	auto& locations = request.context->locations;

#define CHECK(node) {\
		auto it = locations.find(node.get()); \
		if (it != locations.end()) { \
			auto& range = it->second.range; \
			if (range && statement_range.overlaps(*range)) { \
				num_overlap++; \
				break; \
			} \
		} \
	}

	for (auto& node : request.statements) {
		CHECK(node);
	}
	for (auto& node : request.expressions) {
		CHECK(node);
	}

	if (!num_overlap) {
		return;
	}

	request.flavor = _state.flavor_spec;
}

void
SetScopeFlavor::to_string(std::ostream& o) const
{
	ASSERT(_state.flavor_spec);
	ASSERT(_state.statement_range);

	o << get_dbg_name() << "(";
	o << "flavor=" << _state.flavor_spec->generate_signature();
	o << ", range=";
	_state.statement_range->to_string(o);
	o << ")";
}



Decision*
DecisionFactory::new_inline()
{
	return dec_inline;
}

Decision*
DecisionFactory::new_data_centric(
	const std::shared_ptr<FlavorSpec>& flavor)
{
	Decision* result = nullptr;
	auto it = set_DataCentric.find(*flavor);
	if (it == set_DataCentric.end()) {
		auto d = std::make_unique<DataCentric>(flavor);

		result = d.get();
		set_DataCentric.insert({ *flavor, result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}
	return result;
}

Decision*
DecisionFactory::new_jit_expressions(const std::shared_ptr<FlavorSpec>& flavor)
{
	Decision* result = nullptr;
	auto it = set_JitExpressions.find(*flavor);
	if (it == set_JitExpressions.end()) {
		auto d = std::make_unique<JitExpressions>(flavor);

		result = d.get();
		set_JitExpressions.insert({ *flavor, result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}
	return result;
}

Decision*
DecisionFactory::new_set_default_flavor(
	const std::shared_ptr<FlavorSpec>& flavor)
{
	Decision* result = nullptr;
	auto it = set_SetDefaultFlavor.find(*flavor);
	if (it == set_SetDefaultFlavor.end()) {
		auto d = std::make_unique<SetDefaultFlavor>(flavor);

		result = d.get();
		set_SetDefaultFlavor.insert({ *flavor, result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}
	return result;
}

Decision*
DecisionFactory::new_set_vector_size(
	const std::shared_ptr<SetVectorKey>& key)
{
	Decision* result = nullptr;
	auto it = set_SetVectorSize.find(*key);
	if (it == set_SetVectorSize.end()) {
		auto d = std::make_unique<SetVectorSize>(key);

		result = d.get();
		set_SetVectorSize.insert({ *key, result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}
	return result;
}

Decision*
DecisionFactory::new_jit_statement_fragment(
	const std::shared_ptr<StatementRange>& range,
	const std::shared_ptr<FlavorSpec>& spec)
{
	auto key = JitStatementFragmentId(spec, range);

	Decision* result = nullptr;
	auto it = set_JitStatementFragment.find(key);
	if (it == set_JitStatementFragment.end()) {
		auto d = std::make_unique<JitStatementFragment>(range, spec);

		result = d.get();
		set_JitStatementFragment.insert({ std::move(key), result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}

	return result;
}

Decision*
DecisionFactory::new_set_scope_flavor(
	const std::shared_ptr<StatementRange>& range,
	const std::shared_ptr<FlavorSpec>& spec)
{
	auto key = JitStatementFragmentId(spec, range);

	Decision* result = nullptr;
	auto it = set_SetScopeFlavor.find(key);
	if (it == set_SetScopeFlavor.end()) {
		auto d = std::make_unique<SetScopeFlavor>(range, spec);

		result = d.get();
		set_SetScopeFlavor.insert({ std::move(key), result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}

	return result;
}

Decision*
DecisionFactory::new_swap_operators(const SwapOperatorsValue& key)
{
	if (key.source_index > key.destination_index) {
		return new_swap_operators(SwapOperatorsValue { key.source_index,
			key.destination_index });
	}

	Decision* result = nullptr;
	auto it = set_SwapOperators.find(key);
	if (it == set_SwapOperators.end()) {
		auto d = std::make_unique<SwapOperators>(key);

		result = d.get();
		set_SwapOperators.insert({ std::move(key), result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}

	return result;
}

Decision*
DecisionFactory::new_enable_bloom_filter(size_t stable_op_id, size_t bits)
{
	Decision* result = nullptr;

	ASSERT(bits == 1 || bits == 2);

	auto& map = bits == 1 ? set_EnableBloomFilter1 : set_EnableBloomFilter2;

	auto it = map.find(stable_op_id);
	if (it == map.end()) {
		auto d = std::make_unique<EnableBloomFilter>(stable_op_id, bits);

		result = d.get();
		map.insert({stable_op_id, result });
		allocated.emplace_back(std::move(d));
	} else {
		result = it->second;
		ASSERT(result);
	}

	return result;
}

Decision*
DecisionFactory::new_from_state(const DecisionState& state)
{
	Decision* result = nullptr;

	switch (state.node_type) {
	case DecisionType::kInline:
		result = new_inline();
		break;

	case DecisionType::kSwapOperators:
		result = new_swap_operators(SwapOperatorsValue {
			state.source_index, state.destination_index});
		break;

	case DecisionType::kEnableBloomFilter:
		result = new_enable_bloom_filter(state.stable_op_id, state.source_index);
		break;

	case DecisionType::kSetDefaultFlavor:
		ASSERT(state.flavor_spec);
		result = new_set_default_flavor(state.flavor_spec);
		break;

	case DecisionType::kSetVectorSize:
		ASSERT(state.set_vector_key);
		result = new_set_vector_size(state.set_vector_key);
		break;

	case DecisionType::kJitStatementFragment:
		ASSERT(state.flavor_spec);
		ASSERT(state.statement_range);
		result = new_jit_statement_fragment(state.statement_range, state.flavor_spec);
		break;

	case DecisionType::kSetScopeFlavor:
		ASSERT(state.flavor_spec);
		ASSERT(state.statement_range);
		result = new_set_scope_flavor(state.statement_range, state.flavor_spec);
		break;

	case DecisionType::kDataCentric:
		ASSERT(state.flavor_spec);
		result = new_data_centric(state.flavor_spec);
		break;

	case DecisionType::kJitExpressions:
		ASSERT(state.flavor_spec);
		result = new_jit_expressions(state.flavor_spec);
		break;

	default:
		LOG_ERROR("DecisionFactory::new_from_state: Invalid node type %d (%p)",
			(int)state.node_type, (void*)state.node_type);
		ASSERT(false && "invalid node type");
		result = nullptr;
		break;
	}

	return result;
}

DecisionFactory::DecisionFactory()
{
	auto d = std::make_unique<Inline>();
	dec_inline = d.get();
	allocated.emplace_back(std::move(d));
}
