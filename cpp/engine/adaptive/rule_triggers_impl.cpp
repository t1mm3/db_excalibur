#include "rule_triggers_impl.hpp"
#include "brain.hpp"
#include "engine/protoplan.hpp"

using namespace engine::adaptive::triggers;

using Decision = engine::adaptive::Decision;

template<typename T>
static engine::lolepop::VoilaLolepop*
is_valid_op(const T& op)
{
	auto voila_op = dynamic_cast<engine::lolepop::VoilaLolepop*>(op.get());
	if (!op->is_dummy_op && voila_op) {
		return voila_op;
	}
	return nullptr;
}


bool
SetDefaultFlavor::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (input.class_type_counter[DecisionType::kSetDefaultFlavor]) {
		return false;
	}

	if (input.previous) {
		bool redundant = false;
		input.previous->for_each_bottomup([&] (auto& dec) {
			if (redundant) return;

			switch (dec->get_type()) {
			case DecisionType::kDataCentric:
				{
					auto dc = dynamic_cast<DataCentric*>(dec);
					ASSERT(dc);
					if (dc->get_flavor_spec() == *spec) {
						redundant = true;
					}
				}
				break;

			case DecisionType::kJitExpressions:
				{
					auto dc = dynamic_cast<JitExpressions*>(dec);
					ASSERT(dc);
					if (dc->get_flavor_spec() == *spec) {
						redundant = true;
					}
				}

			default:
				break;
			}
		});

		if (redundant) {
			return false;
		}
	}

	return true;
}

std::vector<Decision*>
SetDefaultFlavor::generate(Input& i, DecisionFactory& factory,
		std::unique_ptr<RuleTrigger::Context>& context)
{
	return { factory.new_set_default_flavor(spec) };
}

SetDefaultFlavor::SetDefaultFlavor(const FlavorSpec& spec)
 : RuleTrigger("SetDefaultFlavor"), spec(std::make_shared<FlavorSpec>(spec)) {

}




bool
SetVectorSize::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (input.class_type_counter[DecisionType::kSetVectorSize]) {
		return false;
	}

	return true;
}

std::vector<Decision*>
SetVectorSize::generate(Input& i, DecisionFactory& factory,
		std::unique_ptr<RuleTrigger::Context>& context)
{
	return { factory.new_set_vector_size(values) };
}

SetVectorSize::SetVectorSize(const SetVectorKey& values)
 : RuleTrigger("SetVectorSize"),
	values(std::make_shared<SetVectorKey>(values))
{

}



bool
DataCentricStrategy::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (input.num_decisions) {
		// must be first
		return false;
	}
	const auto& counts = input.class_type_counter;

	return !counts[DecisionType::kDummy] && !counts[DecisionType::kDataCentric] &&
		!counts[DecisionType::kJitExpressions];
}

std::vector<Decision*>
DataCentricStrategy::generate(Input& i, DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	std::vector<Decision*> r;
	r.reserve(2);
	if (inline_all) {
		r.push_back(factory.new_inline());
	}
	r.push_back(factory.new_data_centric(spec));
	return r;
}

DataCentricStrategy::DataCentricStrategy(const FlavorSpec& spec, bool inline_all)
 : RuleTrigger("DataCentricStrategy"), spec(std::make_shared<FlavorSpec>(spec)),
	inline_all(inline_all)
{
}




bool
JitExpressionsStrategy::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (input.num_decisions) {
		// must be first
		return false;
	}
	const auto& counts = input.class_type_counter;

	bool can = !counts[DecisionType::kDummy] &&
		!counts[DecisionType::kDataCentric] &&
		!counts[DecisionType::kJitStatementFragment] &&
		!counts[DecisionType::kJitExpressions] &&
		!counts[DecisionType::kInline];

	return can;
}

std::vector<Decision*>
JitExpressionsStrategy::generate(Input& i, DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	return {factory.new_jit_expressions(spec)};
}

JitExpressionsStrategy::JitExpressionsStrategy(const FlavorSpec& spec)
 : RuleTrigger("JitExpressionsStrategy"), spec(std::make_shared<FlavorSpec>(spec))
{
}



bool
FindJittableFragment::can_trigger(Input& input, std::unique_ptr<RuleTrigger::Context>& context)
{
	if (input.class_type_counter[DecisionType::kDataCentric]) {
		return false;
	}
	if (input.class_type_counter[DecisionType::kJitExpressions]) {
		return false;
	}
#if 1
	if (requires_inline && !input.class_type_counter[DecisionType::kInline]) {
		LOG_DEBUG("%s: no inline", get_name().c_str());
		return false;
	}
#endif
#if 1
	if (data_centric && input.class_type_counter[DecisionType::kJitStatementFragment]) {
		LOG_DEBUG("%s: DataCentric can only be applied once",
			get_name().c_str());
		// TODO: check op-ids of other fragments
		return false;
	}
#endif
	if (!input.example_plan || !input.voila_context) {
		LOG_DEBUG("%s: No example plan, or no voila::Context. plan=%p, ctx=%p",
			get_name().c_str(), input.example_plan.get(),
			input.voila_context.get());
		return false;
	}

	LocalContext ctx;

	ctx.has_inline = input.class_type_counter[DecisionType::kInline];

	engine::LoleplanPass::apply_source_to_sink(input.example_plan,
		[&] (auto op) {
			ctx.num_ops++;
			if (!is_valid_op(op)) {
				return;
			}

			ctx.num_voila_ops++;
		}
	);

	if (!ctx.num_voila_ops) {
		LOG_DEBUG("%s: No VoilaLolepops", get_name().c_str());
		return false;
	}

	// request prolfing info
	if (needs_profile && !input.get_input_plan_profile()) {
		return false;
	}

	context = std::make_unique<LocalContext>(ctx);

	return true;
}

std::vector<Decision*>
FindJittableFragment::generate(Input& input, DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& _context)
{
	ASSERT(input.example_plan && _context);
	auto& context = (LocalContext&)(*_context);
	ASSERT(context.num_voila_ops > 0);

	if (context.current_op >= context.num_voila_ops) {
		return {};
	}

	std::vector<Decision*> result;
	StatementFragmentSelection::ConstructCandidateOpts opts;

	size_t op_idx = 0;

	if (needs_profile) {
		opts.input_plan_profile = input.get_input_plan_profile();
		opts.jit_opts = &jit_opts;
	}

	// engine::lolepop::Lolepop::print_plan(input.example_plan, "FindJittableFragment");

	engine::LoleplanPass::apply_source_to_sink(input.example_plan,
		[&] (auto op) {
			if (op_idx++ != context.current_op) {
				return;
			}

			auto voila_op = is_valid_op(op);
			if (voila_op && !voila_op->is_dummy_op) {
				ASSERT(voila_op->voila_block);
				const auto& voila_block = *voila_op->voila_block;

				LOG_DEBUG("%s: Operator '%s'(%p), idx %llu, is_dummy_op %d, num_statements %llu",
					get_name().c_str(), op->name.c_str(), op.get(), op_idx, op->is_dummy_op,
					voila_block.statements.size());

				ASSERT(!voila_block.statements.empty());
				std::unique_ptr<FuseGroups> fuse_groups(std::make_unique<FuseGroups>());

				size_t num_iter = statements(result, input, factory,
					voila_block.statements, context, {}, fuse_groups.get(),
					opts);

				if (num_iter) {
					ASSERT(!result.empty());
				}
			}
			context.current_op++;
		}
	);

	return result;
}

size_t
FindJittableFragment::statements(std::vector<Decision*>& result,
	const Input& input,
	DecisionFactory& factory,
	const std::vector<Stmt>& stmts,
	LocalContext& context,
	std::vector<size_t> line_numbers,
	FuseGroups* fuse_groups,
	const StatementFragmentSelection::ConstructCandidateOpts& opts)
{
	if (!result.empty()) {
		return 0;
	}
	const int64_t num = stmts.size();
	ASSERT(num);

	LOG_DEBUG("TODO: need to create vector<Decision> global context, to share FuseGroups");

	StatementFragmentSelectionPass pass(input.brain.get_sys_context(),
		nullptr, *input.voila_context, nullptr,
		fuse_groups, nullptr, context.current_op);
	StatementFragmentSelection selection(pass);

	// try to get biggest fragments until, we cannot find such anymore.

	size_t iteration = 0;
	Candidate curr_candidate;
	Candidate best_candidate;

	std::vector<size_t> new_lines(line_numbers);
	new_lines.push_back(0);

	// TODO: maybe rethink and try to have some merge-style algorithm
	int64_t start_idx = 0;
	while (start_idx < num) {
		int64_t next_start_idx = start_idx+1;

		for (int64_t end_idx=num; end_idx>start_idx; end_idx--) {
			// does overlap with existing fragments?
			new_lines[line_numbers.size()] = start_idx;
			StatementIdentifier begin(context.current_op, new_lines);

			new_lines[line_numbers.size()] = end_idx;
			StatementIdentifier end(context.current_op, new_lines);

			auto range = StatementRange(std::move(begin), std::move(end));
			if (input.any_range_overlaps(range)) {
				LOG_DEBUG("%s: Range overlaps [%lld, %lld): %s",
					get_name().c_str(), start_idx, end_idx,
					range.to_string().c_str());
				continue;
			}

			// regular stuff: try to create fragment
			int64_t out_skip_first = 0;
			selection.construct_candidate(
				start_idx, end_idx, out_skip_first, curr_candidate,
				best_candidate, num, stmts, opts);
			if (out_skip_first) {
				// skip first n statements, if we cannot compile them anyway
				ASSERT(out_skip_first > 0);
				next_start_idx = std::max(next_start_idx, start_idx + out_skip_first);
				LOG_DEBUG("%s: Skipping %lld statements "
					"next index %lld",
					get_name().c_str(), out_skip_first, next_start_idx);
				break;
			}
		}

		ASSERT(next_start_idx > start_idx);
		start_idx = next_start_idx;
	}

	// construct FuseGroup from candidate
	if (!best_candidate.empty()) {
		// ASSERT(line_numbers.empty());
		new_lines[line_numbers.size()] = best_candidate.start_idx;
		StatementIdentifier begin(context.current_op, new_lines);

		new_lines[line_numbers.size()] = best_candidate.end_idx;
		StatementIdentifier end(context.current_op, new_lines);

		ASSERT(best_candidate.end_idx <= num);

		LOG_DEBUG("%s: start_idx %lld, end_idx %lld, depth %llu, num %llu",
			get_name().c_str(), best_candidate.start_idx,
			best_candidate.end_idx, line_numbers.size(), num);

		// create result
		ASSERT(result.empty());
#if 0
		if (requires_inline && !context.has_inline) {
			result.push_back(factory.new_inline());
		}
#endif
		result.push_back(factory.new_jit_statement_fragment(
			std::make_shared<StatementRange>(begin, end), spec));
		return 1;
	} else {
		LOG_DEBUG("%s: cannot construct new FuseGroup ... Abort (iteration %llu)",
			get_name().c_str(), iteration);
	}

	std::vector<size_t> new_line_numbers(line_numbers);
	new_line_numbers.push_back(0);

	size_t l = 0;
	for (auto& stmt : stmts) {
		new_line_numbers[line_numbers.size()] = l;
		if (auto block = dynamic_cast<engine::voila::Block*>(stmt.get())) {
			size_t r = statements(result, input, factory, block->statements,
				context, new_line_numbers, fuse_groups, opts);
			if (r) {
				return r;
			}
		} else {
			ASSERT(!dynamic_cast<engine::voila::Loop*>(stmt.get()));
		}

		l++;
	}

	return 0;
}

FindJittableFragment::FindJittableFragment(const FlavorSpec& spec,
	int profile_guided, bool requires_inline)
 : RuleTrigger(profile_guided ? "FindJittableFragmentProf" : "FindJittableFragmentPlain"),
	spec(std::make_shared<FlavorSpec>(spec)), profile_guided(profile_guided), requires_inline(requires_inline)
{
	needs_profile = profile_guided > 0;
	data_centric = false;

	switch (profile_guided) {
	case 1:
		jit_opts.can_pass_sel = -1; // depends on profile
		jit_opts.can_pass_mem = -1;
		jit_opts.can_pass_complex = 0;
		break;

	case -1:
		jit_opts.can_pass_sel = 0;
		jit_opts.can_pass_mem = 1;
		jit_opts.can_pass_complex = 0;
		break;

	case -2:
		jit_opts.can_pass_sel = 1;
		jit_opts.can_pass_mem = 0;
		jit_opts.can_pass_complex = 0;
		break;

	case -3:
		jit_opts.can_pass_sel = 0;
		jit_opts.can_pass_mem = 0;
		jit_opts.can_pass_complex = 0;
		break;

	case 0:
		jit_opts.can_pass_sel = 1;
		jit_opts.can_pass_mem = 1;
		jit_opts.can_pass_complex = 1;
		data_centric = true;
		break;

	default:
		ASSERT(false && "Invalid value for profile_guided");
		break;
	}
}




#include "engine/lolepops/scan.hpp"

static engine::lolepop::Filter*
to_filter(engine::lolepop::Lolepop* l)
{
	return dynamic_cast<engine::lolepop::Filter*>(l);
}

bool
PushDownMostSelectiveFilter::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (!input.given_plan_profile) {
		LOG_DEBUG("PushDownMostSelectiveFilter: No profile");
		return false;
	}

	if (!input.example_plan) {
		LOG_DEBUG("PushDownMostSelectiveFilter: No plan");
		return false;
	}
	if (input.class_type_counter[DecisionType::kSwapOperators]) {
		LOG_DEBUG("PushDownMostSelectiveFilter: Already introduced");
		return false;
	}

	size_t n = 0;
	LoleplanPass::apply_source_to_sink(input.example_plan, [&] (auto op) {
		if (to_filter(op.get())) {
			n++;
		}
	});

	if (n < 2) {
		LOG_DEBUG("PushDownMostSelectiveFilter: Not enough filters (n=%llu)", n);
		return false;
	}

	return true;
}

#include "plan_profile.hpp"

std::vector<Decision*>
PushDownMostSelectiveFilter::generate(Input& input, DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	ASSERT(input.given_plan_profile && input.example_plan);

	const auto& flat_plan_prof = input.given_plan_profile->flattend;

	struct Entry {
		double sel;
		size_t id;
	};
	std::vector<Entry> entries;
	int64_t first_id_offset = -1; 

	LoleplanPass::apply_source_to_sink_with_id(input.example_plan,
		[&] (auto op, auto id) {
			if (first_id_offset < 0) {
				ASSERT(op->plan_op);
				first_id_offset = op->plan_op->stable_op_id;
				ASSERT(first_id_offset >= 0);
			}
			if (auto filter = to_filter(op.get())) {
				ASSERT(flat_plan_prof[id]);
				ASSERT(op->plan_op);
				const auto& prof = flat_plan_prof[id]->prof_data;
				double sel = (double)prof.sum_output_tuples / (double)prof.sum_input_tuples;
				entries.emplace_back(Entry {sel, op->plan_op->stable_op_id});

				LOG_DEBUG("PushDownMostSelectiveFilter: init sel: %f with id=%lld",
					sel, (int64_t)op->plan_op->stable_op_id);
			}
		}
	);

	std::sort(entries.begin(), entries.end(),
		[] (const Entry& a, const Entry &b) {
			return a.sel < b.sel;
		}
	);

	if (LOG_WILL_LOG(DEBUG)) {
		LOG_DEBUG("PushDownMostSelectiveFilter: Selectivities");
		for (auto& e : entries) {
			LOG_DEBUG("PushDownMostSelectiveFilter: sel: %f", e.sel);
		}
	}

	ASSERT(first_id_offset >= 0);

	std::vector<Decision*> result;

	std::unordered_set<size_t> already_swapped_ids;

	size_t entry_id = 0;
	LoleplanPass::apply_source_to_sink_with_id(input.example_plan,
		[&] (auto op, auto seq_id) {
			size_t id = first_id_offset + seq_id;
			if (auto filter = to_filter(op.get())) {
				auto& entry = entries[entry_id];

				bool already_swapped =
					already_swapped_ids.find(id) != already_swapped_ids.end() ||
					already_swapped_ids.find(entry.id) != already_swapped_ids.end();

				if (!already_swapped && entry.id != op->plan_op->stable_op_id) {
					result.push_back(factory.new_swap_operators(SwapOperatorsValue {
						(int64_t)id, (int64_t)entry.id
					}));

					LOG_DEBUG("swap %llu and %llu", id, entry.id);

					already_swapped_ids.insert(id);
					already_swapped_ids.insert(entry.id);

					ASSERT(id > 0);
					ASSERT(entry.id > 0);
				}

				entry_id++;
			}
		}
	);

	return result;
}


#include "engine/lolepops/hash_join.hpp"

bool
EnableBloomFilterForHighestSelJoin::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (!input.given_plan_profile) {
		LOG_DEBUG("EnableBloomFilterForHighestSelJoin: No profile");
		return false;
	}

	if (!input.example_plan) {
		LOG_DEBUG("EnableBloomFilterForHighestSelJoin: No plan");
		return false;
	}

	std::unordered_set<size_t> other_stable_op_ids;

	if (input.previous) {
		input.previous->for_each_bottomup([&] (Decision* dec) {
			auto similar_op = dynamic_cast<EnableBloomFilter*>(dec);
			if (similar_op) {
				other_stable_op_ids.insert(similar_op->stable_op_id);
			}
		});
	}

	std::vector<LocalContext::Entry> qualifying_joins;
	std::unordered_set<void*> join_set;

	auto& flattend = input.given_plan_profile->flattend;

	// find higest sel join NOT IN other_stable_op_ids
	engine::LoleplanPass::apply_source_to_sink_with_id(
		input.example_plan,
		[&] (auto op, auto op_idx) {
			ASSERT(op_idx < flattend.size());

			if (!op->rel_op || other_stable_op_ids.find(
						op->rel_op->get_stable_op_id()) !=
					other_stable_op_ids.end()) {
				// ignore
				return;
			}

			if (join_set.find(op->rel_op.get()) != join_set.end()) {
				return;
			}

			if (auto join = std::dynamic_pointer_cast<engine::relop::HashJoin>(op->rel_op)) {
				if (auto probe_op = dynamic_cast<engine::lolepop::HashJoinProbe*>(op.get())) {
					auto sel = flattend[op_idx]->prof_data.get_selectivity();

					size_t ht_rows = 0.0;
					size_t ht_buckets = 0;
					auto ht_bytes = probe_op->estimate_hash_table_size(ht_rows, ht_buckets);
					size_t bucket_bytes = ht_buckets * sizeof(void*);

					double effective_wasted_mib = (1.0 - sel) * (double)bucket_bytes
						/ (1024.0*1024.0);

					bool sel_threshold = sel <= 0.75;
					bool size_threshold = effective_wasted_mib >= 32.0;

					LOG_TRACE("EnableBloomFilterForHighestSelJoin: "
						"Join op_idx=%llu, sel=%f, bucket_bytes=%llu, effective_wasted_mib=%f",
						op_idx, sel, bucket_bytes, effective_wasted_mib);

					if (sel_threshold && size_threshold && std::isfinite(sel)) {
						join_set.insert(op->rel_op.get());
						qualifying_joins.emplace_back(LocalContext::Entry { join, sel });
					}
				}
			}
		}
	);

	if (qualifying_joins.empty()) {
		LOG_DEBUG("EnableBloomFilterForHighestSelJoin: No qualifying joins");
		return false;
	}

	LOG_DEBUG("EnableBloomFilterForHighestSelJoin: Sort");

	std::sort(qualifying_joins.begin(), qualifying_joins.end(),
		[] (auto& a, auto& b) { return a.sel < b.sel; });

	if (LOG_WILL_LOG(DEBUG)) {
		for (auto& p : qualifying_joins) {
			LOG_DEBUG("EnableBloomFilterForHighestSelJoin: %f", p.sel);
		}
	}

	context = std::make_unique<LocalContext>(std::move(qualifying_joins));

	return true;
}

std::vector<Decision*>
EnableBloomFilterForHighestSelJoin::generate(Input& input,
	DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& _context)
{
	ASSERT(input.given_plan_profile && input.example_plan);
	auto* context = (LocalContext*)(_context.get());

	ASSERT(context);

	auto& qualifying_joins = context->qualifying_joins;
	ASSERT(!qualifying_joins.empty());
	if (context->curr_index >= qualifying_joins.size()) {
		return {};
	}


	const auto& first = qualifying_joins[context->curr_index++];
	ASSERT(first.rel_op);

	return { factory.new_enable_bloom_filter(
		first.rel_op->get_stable_op_id(), num_bits) };
}




struct SetScopeFlavorContext : engine::adaptive::RuleTrigger::Context {
	struct RangeInfo {
		double sum_cyc_tup = .0;
		size_t num_profiles = 0;
		std::vector<int64_t> tags;
	};

	using RangeMap = std::unordered_map<
		engine::voila::StatementRange, RangeInfo>;
	RangeMap range_map;

	size_t round = 0;

	SetScopeFlavorContext(RangeMap&& range_map)
	 : range_map(std::move(range_map)) {}
};

bool
SetScopeFlavor::can_trigger(Input& input,
	std::unique_ptr<RuleTrigger::Context>& context)
{
	if (!input.given_plan_profile) {
		LOG_DEBUG("SetScopeFlavor: No profile");
		return false;
	}

	if (!input.example_plan) {
		LOG_DEBUG("SetScopeFlavor: No plan");
		return false;
	}

	if (input.previous) {
		bool redundant = false;
		input.previous->for_each_bottomup([&] (auto& dec) {
			if (redundant) return;

			switch (dec->get_type()) {
			case DecisionType::kSetDefaultFlavor:
				{
					auto df = dynamic_cast<engine::adaptive::SetDefaultFlavor*>(dec);
					ASSERT(df);
					if (df->get_flavor_spec() == *flavor) {
						redundant = true;
					}
				}
				break;

			default:
				break;
			}
		});

		if (redundant) {
			return false;
		}
	}

	SetScopeFlavorContext::RangeMap range_profiles;

	for (auto op_prof : input.given_plan_profile->flattend) {
		ASSERT(op_prof);
		for (auto& prim : op_prof->primitives) {
			ASSERT(prim);

			if (!prim->statement_range) {
				continue;
			}
			const auto& range = *prim->statement_range;
			if (input.any_range_overlaps(range)) {
				continue;
			}
			double cyc_tup = prim->prof_data.get_cycles_per_tuple();
			if (!std::isfinite(cyc_tup)) {
				continue;
			}

			auto& info = range_profiles[range];
			info.sum_cyc_tup += cyc_tup;
			info.num_profiles++;
			info.tags.push_back(prim->voila_func_tag);
		}
	}

	if (range_profiles.empty()) {
		return false;
	}

	context = std::make_unique<SetScopeFlavorContext>(std::move(range_profiles));
	return true;
}

std::vector<Decision*>
SetScopeFlavor::generate(Input& input, DecisionFactory& factory,
	std::unique_ptr<RuleTrigger::Context>& _context)
{
	ASSERT(input.given_plan_profile && input.example_plan);
	auto* context = (SetScopeFlavorContext*)(_context.get());
	ASSERT(context);
	ASSERT(!context->range_map.empty());

	struct Info {
		double cyc_tup = .0;
		engine::voila::StatementRange range;
	};
	std::vector<Info> ranges_infos;
	ranges_infos.reserve(context->range_map.size());

	using Function = engine::voila::Function;

	uint64_t exists_flags[] = {
		Function::kCreatesPredicate,
		Function::kRandomAccess,
		Function::kALL,
	};

	const size_t max_rounds = sizeof(exists_flags)/sizeof(exists_flags[0]);

	while (context->round < max_rounds) {

		// materialize into internal shape
		for (auto& keyval : context->range_map) {
			bool has_complex = false;
			size_t has_exists = 0;
			auto& range_info = keyval.second;

			for (auto tag : range_info.tags) {
				auto flags = Function::get_flags((Function::Tag)tag);
				has_complex |= flags & Function::kComplexOperation;
				if (has_complex) {
					break;
				}

				ASSERT(exists_flags[context->round]);
				if (flags & exists_flags[context->round]) {
					has_exists++;
				}
			}

			if (has_complex) {
				continue;
			}
			if (!has_exists) {
				continue;
			}
			ranges_infos.emplace_back(Info {
				range_info.sum_cyc_tup, keyval.first
			});
		}

		context->round++;

#if 0
		// ORDER BY cyc_tup DESC
		std::sort(ranges_infos.begin(), ranges_infos.end(),
			[] (const Info& a, const Info &b) {
				return a.cyc_tup > b.cyc_tup;
			}
		);

		if (LOG_WILL_LOG(DEBUG)) {
			LOG_DEBUG("SetScopeFlavor:");
			for (auto& info : ranges_infos) {
				LOG_DEBUG("info: cyc_tup %f", info.cyc_tup);
			}
		}

		std::vector<Decision*> result;
		result.reserve(ranges_infos.size());
		for (auto& info : ranges_infos) {
			result.push_back(factory.new_set_scope_flavor(info.range, flavor));
		}
		return result;
#else
		int64_t max_index = -1;
		double max_cyc_tup = 0.0;

		for (size_t i=0; i<ranges_infos.size(); i++) {
			auto& inf = ranges_infos[i];
			if (max_index < 0 || inf.cyc_tup > max_cyc_tup) {
				max_index = i;
				max_cyc_tup = inf.cyc_tup;
			}
		}

		ASSERT(max_index < (int64_t)ranges_infos.size());

		if (max_index >= 0) {
			return { factory.new_set_scope_flavor(
				std::make_shared<engine::voila::StatementRange>(
					ranges_infos[max_index].range),
				flavor) };
		}
	}
#endif

	return {};
}
