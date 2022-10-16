#include "stream_reoptimizer.hpp"

#include "excalibur_context.hpp"
#include "engine/adaptive/decision.hpp"
#include "engine/adaptive/brain.hpp"

#include "engine/lolepops/lolepop.hpp"
#include "engine/lolepops/voila_lolepop.hpp"
#include "engine/voila/jit.hpp"
#include "engine/loleplan_passes.hpp"
#include "engine/stream.hpp"
#include "engine/budget.hpp"
#include "engine/query.hpp"
#include "engine/protoplan.hpp"

using namespace engine;
using namespace adaptive;

static constexpr bool kLimitExploration = false;
static constexpr double kLimitMaxPositionSize = 0.5;

static constexpr size_t kExploreMinExecuteCycles = 10*1024*1024;
static constexpr size_t kExploitMinExecuteCycles = 5*kExploreMinExecuteCycles;

static constexpr size_t kExploreMinExecuteTuples = 100*1000;
static constexpr size_t kExploitMinExecuteTuples = 5*kExploreMinExecuteTuples;

// #define REOPT_DEEP_PROFILE

namespace engine::adaptive {

struct CodeReoptGroup {
	using Lolepop = engine::lolepop::Lolepop;

	std::vector<Lolepop*> updates;

	Stream::ExecutionContext execution_context;

	CodeReoptGroup(bool bedrock)
	 : bedrock(bedrock) {

	}

	void install(Stream& stream) const {
		LOG_DEBUG("StreamReoptimizer: Install CodeReoptGroup");

		stream.execution_context = execution_context;

		// and patch operators up
		for (auto& op : updates) {
			ASSERT(op);
			op->reconnect_ios();
		}
	}

	const Metrics& get_metrics() const { return metrics; }
	bool is_bedrock() const { return bedrock; }

	void update_metrics(const Metrics& o) {
		metrics = o;
	}

private:
	Metrics metrics;
	const bool bedrock;
};

} /* engine::adaptive */

using namespace engine;
using namespace adaptive;

StreamReoptimizer::StreamReoptimizer(Stream& stream)
 : stream(stream),
	better_margin_plus1(1.0 + stream.query.config->adaptive_exploit_better_by_margin()),
	brain(stream.query.sys_context.get_brain())
{
	reset_current_tracking();

	std::ostringstream dbg_name_ss;
	dbg_name_ss << "ReOpt_" << stream.query_stage.get_stage_id() << "#" << stream.parallel_id;

	brain_session = brain.make_session(&stream.query_stage, dbg_name_ss.str(),
		stream.query.config->adaptive_exploit_test_random(), stream.execution_context.root,
		*stream.query.config);

	if (kLimitExploration) {
		exploration_budget_manager = std::make_shared<LimitedBudgetManager>(kLimitMaxPositionSize,
			stream.query_stage.stage_exploration_budget_manager.get());
	} else {
		exploration_budget_manager = stream.query_stage.stage_exploration_budget_manager;
	}

#ifdef REOPT_DEEP_PROFILE
	gen_code_prof = std::make_unique<StreamGenCodeProfData>();
#endif
}

void
StreamReoptimizer::set_bedrock(BudgetUser* budget_user)
{
	ASSERT(!m_bedrock_group);

	adaptive::Metrics m;
	auto new_grp = save_current_execution(budget_user, true, nullptr);
	m_bedrock_group = new_grp;
	m_curr_group = new_grp;
}

void
StreamReoptimizer::recover(const char* dbg_path)
{
	if (!m_prev_group && m_curr_group != m_bedrock_group) {
		return;
	}

	LOG_DEBUG("StreamReoptimizer: Recover: Recovering from %p to %p (%s)",
		m_curr_group.get(), m_bedrock_group.get(), dbg_path);
	ASSERT(m_bedrock_group);

	if (brain_session) {
		brain_session->feedback(BrainSession::kRecover,
			m_curr_group->execution_context.root,
			m_curr_group->execution_context.voila_context);
	}

	m_bedrock_group->install(stream);
	reset_current_tracking();

	LOG_DEBUG("StreamReoptimizer: Recover: Done");
}

void
StreamReoptimizer::update_current_metrics(const adaptive::Metrics& metrics)
{
	if (!m_curr_group) {
		return;
	}

	m_curr_group->update_metrics(metrics);
}

void
StreamReoptimizer::apply_actions(BudgetUser* _budget_user,
	adaptive::Actions* actions, const char* dbg_path, bool explore)
{
#if 0
	if (!actions) {
		ASSERT(m_bedrock_group && m_bedrock_group->is_bedrock());

		m_bedrock_group->install(stream);
		return;
	}
#endif

	bool use_budget = explore;
	if (!actions) {
		use_budget = false; // base flavor, shouldn't have overhead
	}

	BudgetUser* budget_user = use_budget ? _budget_user : nullptr;

	auto prof_start = profiling::rdtsc();

	auto cached_group_it = cached_groups.find(actions);
	if (cached_group_it != cached_groups.end()) {
		const auto& group = cached_group_it->second;
		ASSERT(group);
		group->install(stream);

		num_apply_actions_calls++;
		sum_apply_actions_cycles += profiling::rdtsc() - prof_start;
		return;
	}

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, dbg_path);
	}

	LOG_DEBUG("StreamReoptimizer: %s: new plan. "
		"m_num_explore_calls=%llu",
		dbg_path, (uint64_t)m_num_explore_calls);

	// set new Stream::ExecutionContext
	auto& ctx = stream.execution_context;
	ctx.reset();
	ctx.alloc(stream);
	ctx.adaptive_decision = actions;
	ctx.adaptive_decision_ctx = ActionsContext::make_from_actions(actions);


	auto task = scheduler::get_current_task();
	// make new plan
	ProtoplanTranslator proto_translate(stream,
		std::make_shared<memory::Context>(nullptr,
			"planops",
			scheduler::get_current_numa_node()));

	std::shared_ptr<engine::protoplan::PlanOp> new_protoplan(
		stream.query_stage.protoplan_root->clone());
	ASSERT(new_protoplan && "Must have protoplan");

	if (LOG_WILL_LOG(DEBUG)) {
		LOG_DEBUG("protoplan before: ");
		auto curr = new_protoplan;
		while (curr) {
			LOG_WARN("protoplan op  %p '%s'", curr.get(), curr->name.c_str());
			curr = curr->child;
		}
	}

	if (actions) {
		actions->apply_protoplan(ctx.adaptive_decision_ctx.get(),
			new_protoplan);
	}

	if (LOG_WILL_LOG(DEBUG)) {
		LOG_DEBUG("protoplan after: ");
		auto curr = new_protoplan;
		while (curr) {
			LOG_DEBUG("protoplan op %p '%s'", curr.get(), curr->name.c_str());
			curr = curr->child;
		}

	}
	ASSERT(new_protoplan && "Must have protoplan");
	proto_translate(new_protoplan);

	new_protoplan.reset();

	// generate code
	stream.stage_gen_code(task, false, use_budget, gen_code_prof.get(), budget_user);

	stream.stage_prepare(task, false);

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, dbg_path);
	}

	// save new group
	auto new_grp = save_current_execution(budget_user, false,
		stream.execution_context.adaptive_decision);
	m_curr_group = new_grp;
	LOG_DEBUG("StreamReoptimizer: %s: saved current execution. "
		"m_num_explore_calls=%llu",
		dbg_path, (uint64_t)m_num_explore_calls);

	num_apply_actions_calls++;
	sum_apply_actions_cycles += profiling::rdtsc() - prof_start;
}

std::string
StreamReoptimizer::get_current_action_name() const {
	auto actions = brain_session->get_current_actions();
	if (!actions) {
		return "";
	}
	return actions->to_string();
}

void
StreamReoptimizer::explore(BudgetUser& budget_user)
{
	with_metrics([&] () {
		const auto& metrics = m_curr_group->get_metrics();
		if (reopt_trigger_countdown >= 0 && !metrics.num_tuples) {
			LOG_DEBUG("StreamReoptimizer: %s: Not enough tuples (%lld)",
				(int64_t)metrics.num_tuples);
			return;
		}

		ASSERT(m_curr_group);
		m_prev_group = m_curr_group;

		// trigger profiling update when we ran bedrock group
		if (m_curr_group == m_bedrock_group) {
			m_bedrock_group->execution_context.root->do_update_profiling();
		}

		// make decision
		ASSERT(brain_session);
		ASSERT(m_bedrock_group);

		auto old_actions = brain_session->get_current_actions();

		BrainSession::ExploreArgs explore_args;
		explore_args.update_metrics = &metrics;
		explore_args.bedrock_root = m_bedrock_group->execution_context.root;
		explore_args.current_root = m_curr_group->execution_context.root;
		explore_args.current_voila_context = m_curr_group->execution_context.voila_context;
		explore_args.progress = progress;
		explore_args.budget_user = &budget_user;
		if (!brain_session->make_explore_decision(explore_args)) {
			reopt_trigger_countdown = brain_session->get_reopt_trigger_countdown();
			return;
		}

		auto actions = brain_session->get_current_actions();

		LOG_DEBUG("StreamReoptimizer: Explore: Last round %lld tuples, %f cyc/tup",
			(int64_t)metrics.num_tuples, metrics.cyc_per_tup);

		if (old_actions != actions) {
			apply_actions(&budget_user, actions, "apply_actions(Explore)", true);
		}

		reopt_trigger_countdown = brain_session->get_reopt_trigger_countdown();
	});

	m_num_explore_calls++;
}

StreamReoptimizer::ReoptGroupPtr
StreamReoptimizer::save_current_execution(BudgetUser* budget_user,
	bool first_call, Actions* actions)
{
	ReoptGroupPtr new_grp(std::make_shared<adaptive::CodeReoptGroup>(
		first_call));

	new_grp->execution_context = stream.execution_context;
	new_grp->execution_context.adaptive_decision = actions;

	LoleplanPass::apply_source_to_sink(stream.execution_context.root, [&] (auto& op) {
		new_grp->updates.push_back(op.get());
	});

	ASSERT(cached_groups.find(actions) == cached_groups.end());
	cached_groups[actions] = new_grp;

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "save_current_execution");
	}

	return new_grp;
}

void
StreamReoptimizer::exploit()
{
	with_metrics([&] () {
		const auto& metrics = m_curr_group->get_metrics();
		LOG_DEBUG("StreamReoptimizer: Exploit: Last round %lld tuples, %f cyc/tup",
			(int64_t)metrics.num_tuples, metrics.cyc_per_tup);

		ASSERT(brain_session);

		BrainSession::ExploitArgs exploit_args;
		exploit_args.update_metrics = &metrics;
		exploit_args.min_cyc_tup =
			m_curr_group->get_metrics().cyc_per_tup / better_margin_plus1;
		exploit_args.progress = progress;

		auto old_actions = brain_session->get_current_actions();
		if (!brain_session->make_exploit_decision(exploit_args)) {
			reopt_trigger_countdown = brain_session->get_reopt_trigger_countdown();
			return;
		}

		auto actions = brain_session->get_current_actions();
		if (old_actions != actions) {
			apply_actions(nullptr, actions, "apply_actions(Exploit)", false);
		}

		reopt_trigger_countdown = brain_session->get_reopt_trigger_countdown();
	});

	m_num_exploit_calls++;
}

void
StreamReoptimizer::print_profile(std::ostream& s) const
{
	if (!num_apply_actions_calls) {
		return;
	}

	int64_t t_gen = gen_code_prof ? gen_code_prof->t_total * 100 / sum_apply_actions_cycles : -1;

	s << "apply_actions: cyc_call " << ((double)sum_apply_actions_cycles / (double)num_apply_actions_calls)
		<< ", calls " << num_apply_actions_calls << ", gen_code " << t_gen << "%" << "\n";

	if (gen_code_prof) {
		s << "gen_code: ";
		gen_code_prof->to_string(s);
		s << std::endl;
	}

}

StreamReoptimizer::~StreamReoptimizer()
{
#ifdef REOPT_DEEP_PROFILE
	std::ostringstream ss;
	print_profile(ss);

	if (!ss.str().empty()) {
		LOG_ERROR("StreamReoptimizer: %s", ss.str().c_str());
	}
#endif

	cached_groups.clear();
	brain.destroy_session(brain_session);
}

void
StreamReoptimizer::execute()
{
	if (brain_session) {
		brain_session->feedback(BrainSession::FeedbackReason::kSuccessful,
			m_curr_group->execution_context.root,
			m_curr_group->execution_context.voila_context);
	}
}

void
StreamReoptimizer::scan_feedback(bool& do_reopt, bool& force_reopt,
	int64_t cycles_last_reopt)
{
	force_reopt = false;
	do_reopt = false;

	if (reopt_trigger_countdown < 0) {
		force_reopt = reopt_trigger_countdown < 0;

		return;
	}

	bool exploit = reopt_trigger_countdown > 0;
	if (reopt_trigger_countdown) {
		ASSERT((reopt_trigger_countdown == 1) || (reopt_trigger_countdown == -1));
	}

	const auto min_cycles = exploit ? kExploitMinExecuteCycles : kExploreMinExecuteCycles;
	const auto min_tuples = exploit ? kExploitMinExecuteTuples : kExploreMinExecuteTuples;

	do_reopt = m_track_sum_tuples > min_tuples;
	if (do_reopt) {
		return;
	}

	auto cycles_since_last_reopt = profiling::rdtsc() - cycles_last_reopt;
	do_reopt &= cycles_since_last_reopt > min_cycles;
}