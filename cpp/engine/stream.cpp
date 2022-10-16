#include "stream.hpp"

#include "budget.hpp"
#include "excalibur_context.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"

#include "lolepops/lolepop.hpp"
#include "lolepops/scan.hpp"
#include "lolepops/hash_join.hpp"
#include "lolepops/hash_group_by.hpp"
#include "lolepops/output.hpp"

#include "engine/voila/jit.hpp"
#include "engine/voila/voila.hpp"
#include "engine/voila/code_cache.hpp"

#include "engine/adaptive/decision.hpp"
#include "engine/adaptive/stream_reoptimizer.hpp"

#include "voila/voila.hpp"

#include "storage/scan_interface.hpp"

#include "query.hpp"
#include "type_stream.hpp"
#include "xvalue.hpp"
#include "string_heap.hpp"
#include "profile.hpp"

#include "loleplan_passes.hpp"
#include "var_life_time_pass.hpp"



using namespace engine;

static constexpr size_t kMinExecuteRuns = 2;
static constexpr size_t kExecutePeriod = 16;

static constexpr uint64_t kMinExecuteIntervalCycles = 1024*1024*10;
static constexpr uint64_t kMinExecuteCycles = kMinExecuteIntervalCycles;

static constexpr int64_t kMaxExploreThreads = 1;


static int64_t
compute_max_budget(double total_cycles, double parallelism, double max_risk_budget)
{
	double new_max_budget_dbl =
		total_cycles * max_risk_budget; // * parallelism;

	int64_t new_max_budget;
	if (new_max_budget_dbl <= 0.0 || !std::isfinite(new_max_budget_dbl)) {
		new_max_budget = 0;
	} else {
		new_max_budget = new_max_budget_dbl;
	}
	return new_max_budget;
}



void
Stream::ExecutionContext::reset()
{
	adaptive_decision = nullptr;
	root.reset();
	source_read_positions.clear();

	voila_global_context.reset();
	voila_context.reset();
}

void
Stream::ExecutionContext::alloc(Stream& stream)
{
	voila_global_context =
		std::make_shared<engine::voila::GlobalCodeContext>(stream.mem_context);
	voila_context =
		std::make_shared<voila::Context>();
}

Stream::ExecutionContext::ExecutionContext()
{
	reset();
}

Stream::ExecutionContext::~ExecutionContext()
{
	reset();
}



template<typename P, typename T>
auto wrap_stage(const T& fun, const P& profile,
	StreamProf::Phase phase)
{
	if (!profile) {
		return fun();
	}

	auto start = profiling::physical_rdtsc();

	auto r = fun();

	auto stop = profiling::physical_rdtsc();

	profile->sum_time_phase[phase] += stop-start;

	return r;
}

Stream::Stream(Query& query, size_t parallel_id, QueryStage& query_stage,
	memory::Context& mem_context)
 : parallel_id(parallel_id),
	query_stage(query_stage),
 	query(query),
 	mem_context(mem_context),
 	stage_profile(query_stage.profile),
 	has_runtime_feedback(query.config->runtime_feedback()),
 	can_reoptimize(query.config->enable_reoptimize() &&
 		(query.config->compilation_strategy() ==
 			QueryConfig::CompilationStrategy::kAdaptive)),
 	code_cache(query.sys_context.get_code_cache()),
	max_risk_budget(query.config->max_risk_budget()),
	max_execute_progress(query.config->max_execute_progress())
{
	execution_context.alloc(*this);
	// mem_context_op.simple = true;

	result = std::make_unique<Result>();

	global_strings = std::make_unique<StringHeap>(mem_context);

	if (stage_profile) {
		profile = std::make_shared<StreamProf>(parallel_id);
		stage_profile->register_stream(profile);
	}


	reopt_trigger_countdown = kMinExecuteRuns;

	const size_t kSafeBytes = 16*1024;
	invalid_access_sandbox_space = mem_context.aligned_alloc(kSafeBytes, 64,
		memory::Context::kZero | memory::Context::kNoRealloc);
	invalid_access_zero_sandbox_space = mem_context.aligned_alloc(kSafeBytes, 64,
		memory::Context::kZero | memory::Context::kNoRealloc);
}

Stream::~Stream()
{
	stream_reoptimizer.reset();
	execution_context.reset();

	global_strings.reset();

	mem_context.free(invalid_access_sandbox_space);
	mem_context.free(invalid_access_zero_sandbox_space);

	mem_context.dbg_validate("~Stream");
}

void
Stream::scan_feedback(voila::ReadPosition& read_pos, bool& fetch_next)
{
	fetch_next = true;

	if (has_runtime_feedback && (read_pos.num_calls % 16) == 15) {
		trigger_internal_profile_propagation();
	}

	LOG_DEBUG("scan_feedback");
	ASSERT(stage == Stage::kExecute);

	if (stream_reoptimizer && stage == Stage::kExecute) {
		ASSERT(can_reoptimize);

		bool do_reopt = false;
		bool force_reopt = false;

		stream_reoptimizer->scan_feedback(do_reopt, force_reopt,
			prof_last_reopt_stamp);

		if (do_reopt || force_reopt) {
			LOG_DEBUG("Stream::scan_feedback: Trigger reoptimize. do_reopt %d, force_reopt %d",
				do_reopt, force_reopt);

			stage = Stage::kReoptimize;
			fetch_next = false;
		}

		return;
	}

	reopt_trigger_countdown--;

	if (reopt_trigger_countdown <= 0 && can_reoptimize && stage == Stage::kExecute
			// && evalid >= kMinExecuteRuns
			// && prof_sum_time >= kMinExecuteCycles
			&& (profiling::rdtsc() - prof_last_reopt_stamp) >= kMinExecuteIntervalCycles) {

		const auto progress = get_progress();
		if (progress <= max_execute_progress) {
			LOG_DEBUG("Stream::scan_feedback: Trigger reoptimize");

			stage = Stage::kReoptimize;
			fetch_next = false;
		}
	}
}

void
Stream::scan_feedback_with_tuples(voila::ReadPosition& read_pos)
{
	if (stream_reoptimizer) {
		stream_reoptimizer->scan_feedback_with_tuples(read_pos.num_tuples);
	}
	last_reopt_num_tuples += read_pos.num_tuples;
}

void
Stream::trigger_internal_profile_propagation()
{
#if 0
	ParallelAccessLimiter::Access access(query_stage.max_parallel_runtime_feedbacks);
	access.with([&] () {
		double progress = get_progress();

		std::unique_lock lock(query_stage.runtime_feedback_mutex);
		if (progress >= query_stage.runtime_feedback_next_progress) {
			wrap_stage([&] () {
				LOG_DEBUG("Stream::trigger_internal_profile_propagation");
				LoleplanPass::apply_source_to_sink(root, [&] (auto& op) {
					op->propagate_internal_profiling();
				});

				return 0;
			}, profile, StreamProf::Phase::kDynamicProfile);

			query_stage.runtime_feedback_next_progress = progress + 0.05;
		}
	});
#endif
}

double
Stream::get_progress() const
{
	ASSERT(execution_context.source_read_positions.size() == 1);

	for (auto& read_pos : execution_context.source_read_positions) {
		return read_pos->get_progress();
	}

	return 0.0;
}

Stream::ExecResult
Stream::execute()
{
	auto task = scheduler::get_current_task();
	ASSERT(execution_context.root.get());

	int64_t numa_node = -1;
	int64_t worker_id = -1;

	while (1) {
		switch (stage) {
		case Stage::kInit:
			LOG_TRACE("Init");
			stage = Stage::kGenerateCode;
			ip = 0;
			break;

		case Stage::kGenerateCode:
			wrap_stage([&] () {
				stage_gen_code(task, true, false, nullptr, nullptr);
				return 0;
			}, profile, StreamProf::Phase::kGenerateCode);
			break;

		case Stage::kPrepare:
			wrap_stage([&] () {
				stage_prepare(task, true);
				return 0;
			}, profile, StreamProf::Phase::kPrepare);
			break;

		case Stage::kWaitCompilationDone:
			if (!stage_compilation_done(task)) {
				return ExecResult::kYield;
			}
			ip = 0;
			break;

		case Stage::kExecute:
			return wrap_stage([&] () {
				if (numa_node < 0) {
					numa_node = scheduler::get_current_numa_node();
					if (numa_node < 0) {
						numa_node = 0;
					}
				}

				if (worker_id < 0) {
					worker_id = scheduler::get_current_worker_id();
				}

				return stage_execute(task, worker_id, numa_node);
			}, profile, StreamProf::Phase::kExecute);

		case Stage::kProfile:
			wrap_stage([&] () {
				LOG_TRACE("Done");
				execution_context.root->do_update_profiling();
				return 0;
			}, profile, StreamProf::Phase::kProfile);

			stage = Stage::kDone;
			break;

		case Stage::kDone:
			LOG_DEBUG("Done");
			stage_done(task);
			return ExecResult::kEndOfStream;

		case Stage::kReoptimize:
			wrap_stage([&] () {
				stage_reoptimize(task);
				return 0;
			}, profile, StreamProf::Phase::kReoptimize);
			break;

		default:
			ASSERT(false && "Invalid stage");
			break;
		}

		// cooperate
		if (UNLIKELY(scheduler::should_yield(task))) {
			return ExecResult::kYield;
		}
	}

	return ExecResult::kEndOfStream;
}

void
Stream::stage_gen_code(scheduler::Task* task, bool internal, bool use_budget,
	StreamGenCodeProfData* prof, BudgetUser* budget_user)
{
	SCHEDULER_SCOPE(StreamCodeGen, task);

	uint64_t t_start = prof ? profiling::rdtsc() : 0;

	LOG_DEBUG("Stream %p: Producing code for root=%p, internal=%d, use_budget=%d",
		this, execution_context.root.get(), internal, use_budget);

	TypeStream typer(*this);

	using VoilaLolepop = lolepop::VoilaLolepop;

	auto first_op = LoleplanPass::skip_first_non_voila_ops(execution_context.root);

	VoilaCodegenContext ctx;

	// Produce and type code
	LoleplanPass::apply_source_to_sink(first_op, [&] (auto& op) {
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "stage_gen_code(codegen)");
		}
		auto op_name = op->name.c_str();
		auto voila = std::dynamic_pointer_cast<VoilaLolepop>(op);
		ASSERT(voila.get() && "Todo: Support mixes of VOILA and non-VOILA ops");

		LOG_DEBUG("Generating %s", op_name);
		ctx = voila->do_codegen_produce(std::move(ctx));

		ASSERT(voila->voila_block);
	});

	uint64_t t_produce = 0;
	if (prof) {
		t_produce = profiling::rdtsc();
		prof->t_produce += t_produce - t_start;
	}

	LoleplanPass::apply_source_to_sink(first_op, [&] (auto& op) {
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "stage_gen_code(type)");
		}
		auto op_name = op->name.c_str();
		LOG_DEBUG("Type code %s", op_name);
		typer.lolepop(op);
	});

	uint64_t t_type = 0;
	if (prof) {
		t_type = profiling::rdtsc();
		prof->t_type += t_type - t_produce;
	}

	LocationTrackingPass location_tracker(*execution_context.voila_context);
	location_tracker(first_op);

	uint64_t t_loctrack = 0;
	if (prof) {
		t_loctrack = profiling::rdtsc();
		prof->t_loctrack += t_loctrack - t_type;
	}

	if (execution_context.adaptive_decision) {
		execution_context.adaptive_decision->apply_passes(
			execution_context.adaptive_decision_ctx.get(), *this,
			budget_user);
	} else {
		// Inline
		if (first_op && query.config->compilation_strategy() ==
				QueryConfig::CompilationStrategy::kStatements) {
			PrinterPass printer;
			if (LOG_WILL_LOG(DEBUG)) {
				printer(first_op);
			}
			InlinePass inliner(*execution_context.voila_context);
			inliner(first_op);

			if (LOG_WILL_LOG(DEBUG)) {
				printer(first_op);
			}
		}
	}

	uint64_t t_other = 0;
	if (prof) {
		t_other = profiling::rdtsc();
		prof->t_other += t_other - t_loctrack;
	}

	LoleplanPass::apply_source_to_sink(first_op, [&] (auto& op) {
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "stage_gen_code(post_type)");
		}
		op->post_type_pass(budget_user);
	});

	uint64_t t_posttype = 0;
	if (prof) {
		t_posttype = profiling::rdtsc();
		prof->t_posttype += t_posttype - t_other;
	}

	// optimize
	VarLifeTimePass var_life_pass(*this);
	LoleplanPass::apply_source_to_sink(first_op, [&] (auto& op) {
		var_life_pass.lolepop(op);
	});

	uint64_t t_lifetime = 0;
	if (prof) {
		t_lifetime = profiling::rdtsc();
		prof->t_lifetime += t_lifetime - t_posttype;
	}

	// Translate into JIT representation
	size_t op_id=0;
	LoleplanPass::apply_source_to_sink(execution_context.root, [&] (auto& op) {
		if (auto voila_op = dynamic_cast<VoilaLolepop*>(op.get())) {
			op_gen_code_block(op, op_id, use_budget, prof, budget_user);
		}

		op_id++;
	});

	uint64_t t_code_block = 0;
	if (prof) {
		t_code_block = profiling::rdtsc();
		prof->t_code_block += t_code_block - t_lifetime;
	}

	if (prof) {
		prof->t_total += t_code_block - t_start;
		prof->calls++;
	}

	// ... start preparing
	stage = Stage::kPrepare;
}

void
Stream::stage_prepare(scheduler::Task* task, bool internal)
{
	SCHEDULER_SCOPE(StreamPrepare, task);

	LOG_TRACE("Preparing");
	execution_context.root->do_prepare();

	stage = Stage::kWaitCompilationDone;
}

bool
Stream::is_compilation_done() const
{
	bool done = true;
	LoleplanPass::apply_source_to_sink_pre_early_out(execution_context.root, [&] (auto& op) {
		auto voila = std::dynamic_pointer_cast<lolepop::VoilaLolepop>(op);
		if (voila) {
			done &= voila->code_block->is_compilation_done(false);
		}
		return done;
	});
	return done;
}

bool
Stream::stage_compilation_done(scheduler::Task* task)
{
	bool done = true;

	// make sure there were no errors (e.g. OutOfBudgetException or CompileException)
	try {
		LoleplanPass::apply_source_to_sink_pre_early_out(execution_context.root,
			[&] (auto& op) {
				auto voila = std::dynamic_pointer_cast<lolepop::VoilaLolepop>(op);
				if (voila && !voila->is_dummy_op) {
					ASSERT(voila->code_block);
					done &= voila->code_block->is_compilation_done(true);
				}
				return done;
			}
		);
	} catch (OutOfBudgetException& budget_exception) {
		ASSERT(stream_reoptimizer &&
			"Must have been re-optimized, otherwise we shouldn't hit this");

		LOG_DEBUG("stage_compilation_done: got OutOfBudgetException. Trying to recover ...");

		stream_reoptimizer->recover("stage_compilation_done");
		recovered = true;

		LoleplanPass::apply_source_to_sink(execution_context.root, [&] (auto& op) {
			op->restart();
		});

		// try again
		return false;
	} catch (...) {
		LOG_ERROR("stage_compilation_done: catched unknown exception");
		RETHROW();
	}

	if (done) {
		stage = Stage::kExecute;
		if (stream_reoptimizer) {
			stream_reoptimizer->reset_scan_tracking();
			stream_reoptimizer->execute();
		}

		reopt_exec_start_cycles = profiling::rdtsc();
#if 0
		// directly jump to exploration, when this is the 2. thread
		if (stream_reoptimizer && first_run_reoptimize &&
				!execution_context.adaptive_decision && parallel_id == 1) {
			stage = Stage::kReoptimize;
			first_run_reoptimize = false;
		}
#endif
	}
	return done;
}

Stream::ExecResult
Stream::stage_execute(scheduler::Task* task, int64_t worker_id, int64_t numa_node)
{
	SCHEDULER_SCOPE(StreamExecute, task);

	auto prof_start = profiling::physical_rdtsc();
	bool yield = false;

	mem_context.dbg_validate("stage_execute");

	using NextResult = lolepop::NextResult;

	NextResult next_result;
	lolepop::NextContext next_context { task, worker_id, numa_node };

	// Unroll loop, to avoid introducing our expensive
	// 'can_reoptimize' branch
	for (size_t i=0; i<kExecutePeriod; i++) {
		if (!ip) {
			evalid++;
		}


		next_result = execution_context.root->get_next(next_context);
		if (UNLIKELY(next_result == NextResult::kEndOfStream)) {
			ASSERT(stage == kReoptimize || stage == kExecute);
			if (stage == kExecute) {
				execution_done = true;
				stage = Stage::kProfile;
			}
			break;
		}
		mem_context.dbg_validate("stage_execute2");

		ip = 0;
	}

	// cooperate
	if (UNLIKELY(scheduler::should_yield(task))) {
		yield = true;
	}

	prof_sum_time += profiling::physical_rdtsc() - prof_start;

	if (yield) {
		SCHEDULER_SCOPE_YIELD(StreamExecute, task);
		return ExecResult::kYield;
	}
	return ExecResult::kData;
}

#include <sstream>

bool
Stream::op_gen_code_block(std::shared_ptr<lolepop::Lolepop>& op, size_t op_id,
	bool use_budget, StreamGenCodeProfData* prof, BudgetUser* budget_user)
{
	using CodeBlockBuilder = engine::voila::CodeBlockBuilder;

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "op_gen_code_block");
	}

	auto op_name = op->name.c_str();
	auto voila = std::dynamic_pointer_cast<lolepop::VoilaLolepop>(op);
	ASSERT(voila.get() && "Todo: Support mixes of VOILA and non-VOILA ops");

	if (voila->is_dummy_op) {
		voila->output = voila->child->output;
		return true;
	}

	auto& voila_context = voila->voila_context;

	LOG_DEBUG("Generating bytecode for operator '%s' (%p)",
		op_name, op.get());
	auto& code_block = voila->code_block;
	code_block = std::make_unique<CodeBlock>(
		execution_context.adaptive_decision, *op);

	ASSERT(voila->code_block);
	voila->reconnect_ios();

	if (use_budget) {
		ASSERT(stream_reoptimizer);
		ASSERT(stream_reoptimizer->exploration_budget_manager);
	}

	ASSERT(execution_context.voila_global_context);
	CodeBlockBuilder builder(
		code_cache,
		*code_block,
		voila_context,
		*execution_context.voila_global_context,
		op->name,
		use_budget ?
			stream_reoptimizer->exploration_budget_manager :
			nullptr,
		op_id,
		prof ? prof->code_block_builder_prof.get() : nullptr,
		budget_user);
	builder(voila->voila_block->statements);

	return true;
}

static constexpr double kMinProgress = 0.00001;

struct ProgressWindow {
	ProgressWindow(size_t size) : m_max_size(size) {
		m_array.resize(m_max_size);
		reset();
	}

	void push(int64_t p, int64_t cyc, int64_t rows) {
		auto old_index = m_index;
		m_index++;
		if (m_index >= m_max_size) {
			m_index = 0;
		} else {
			m_count++;
		}

		ASSERT(old_index < m_max_size);
		ASSERT(m_index < m_max_size);

		auto& old = m_array[old_index];

		m_sum_cycles -= old.cycles;
		m_sum_progress -= old.progress;
		m_sum_rows -= old.rows;

		auto& ent = m_array[m_index];
		ent.progress = p;
		ent.cycles = cyc;
		ent.rows = rows;

		m_sum_cycles += ent.cycles;
		m_sum_progress += ent.progress;
		m_sum_rows += ent.rows;

		ASSERT(m_sum_cycles >= ent.cycles);
		ASSERT(m_sum_progress >= ent.progress);
	}

	void reset() {
		m_index = 0;
		m_sum_cycles = 0;
		m_sum_progress = 0;
		m_sum_rows = 0;
		for (size_t i=0; i<m_max_size; i++) {
			auto& entry = m_array[i];
			entry.progress = 0;
			entry.cycles = 0;
		}
	}

	int64_t get_sum_progress() const {
		return m_sum_progress;
	}

	int64_t get_sum_cycles() const {
		return m_sum_cycles;
	}

	int64_t get_sum_rows() const {
		return m_sum_rows;
	}


private:
	struct Entry {
		int64_t progress = 0;
		int64_t cycles = 0;
		int64_t rows = 0;
	};
	std::vector<Entry> m_array;
	size_t m_index = 0;
	size_t m_count = 0;

	int64_t m_sum_cycles = 0;
	int64_t m_sum_progress = .0;
	int64_t m_sum_rows = 0;
	const size_t m_max_size;
};


static const double kProgressMul =
	1000.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0;


struct StreamData : QueryStageStreamData {
	std::unordered_map<adaptive::Actions*, std::unique_ptr<ProgressWindow>>
		actions;
};


void
Stream::reoptimize_update_progress(
	double& out_min_cyc_p, double& out_max_cyc_p, int64_t& sum_p, int64_t& sum_cyc,
	double& out_curr_cyc_p, double& out_min_cyc_tup, int64_t progress_diff, int64_t cycles_diff, int64_t rows_diff)
{
	std::unique_lock lock(query_stage.mutex);

	if (!query_stage.stream_data) {
		query_stage.stream_data = std::make_unique<StreamData>();
	}

	auto& stream_data = (StreamData&)(*query_stage.stream_data);

	{
		auto& ins_prog_win = stream_data.actions[execution_context.adaptive_decision];
		if (!ins_prog_win) {
			ins_prog_win = std::make_unique<ProgressWindow>(32);
		}
		ins_prog_win->push(progress_diff, cycles_diff, rows_diff);

		auto sc = ins_prog_win->get_sum_cycles();
		auto sp = ins_prog_win->get_sum_progress();
		double t = (double)sc / ((double)sp / kProgressMul);

		out_curr_cyc_p = t;
	}


	int64_t min_sum_p = -1;
	int64_t min_sum_cyc = -1;
	double min_total_cycles = -1.0;

	int64_t max_sum_p = -1;
	int64_t max_sum_cyc = -1;
	double max_total_cycles = -1.0;

	out_min_cyc_tup = -1.0;

	for (auto& keyval : stream_data.actions) {
		auto& win = keyval.second;
		ASSERT(win);

		auto sc = win->get_sum_cycles();
		auto sp = win->get_sum_progress();
		auto sr = win->get_sum_rows();
		double t = (double)sc / ((double)sp / kProgressMul);
		double cyc_tup = (double)sc / (double)sr;

		if (min_total_cycles < 0.0 || t < min_total_cycles) {
			min_total_cycles = t;
			min_sum_p = sp;
			min_sum_cyc = sc;
		}

		if (max_total_cycles < 0.0 || t > max_total_cycles) {
			max_total_cycles = t;
			max_sum_p = sp;
			max_sum_cyc = sc;
		}

		if (out_min_cyc_tup < 0.0 || cyc_tup < out_min_cyc_tup) {
			out_min_cyc_tup = cyc_tup;
		}
	}

	sum_p = min_sum_p;
	sum_cyc = min_sum_cyc;
	out_min_cyc_p = min_total_cycles;
	out_max_cyc_p = max_total_cycles;
}

int64_t
Stream::reoptimize_try_explore_compute_budget(double progress,
	double& cyc_prog_min, double& cyc_prog_curr, int64_t& cyc_diff,
	int64_t& p_diff, int64_t& sum_cyc, int64_t& sum_p)
{
	sum_cyc = 0;
	sum_p = .0;

	cyc_diff = profiling::rdtsc() - reopt_exec_start_cycles;
	double p_diff_dbl = (progress - reopt_last_progress) * kProgressMul;
	p_diff = p_diff_dbl;
	ASSERT(p_diff >= 0);
	ASSERT(cyc_diff >= 0);

	int64_t row_diff = last_reopt_num_tuples;

	reopt_last_progress = progress;

	cyc_prog_min = .0;
	cyc_prog_curr = .0;
	double cyc_prog_max = .0;
	double min_cyc_tup = .0;

	reoptimize_update_progress(cyc_prog_min, cyc_prog_max, sum_p, sum_cyc,
		cyc_prog_curr, min_cyc_tup, p_diff, cyc_diff, row_diff);


	double p_todo = 1.0 - progress;
	p_todo = std::max(p_todo, .0);

	double cyc_until_end = cyc_prog_min * p_todo;

	ASSERT(sum_p >= 0);
	ASSERT(sum_cyc >= 0);

	int64_t new_max_budget = compute_max_budget(cyc_until_end,
		query_stage.max_parallelism, max_risk_budget);

	LOG_DEBUG("reoptimize_try_explore: parallelism %lld, pred_total %f, new_max_budget %lld, "
		"p_diff %f, cyc_diff %llu, sum_p %f, sum_cyc %llu",
		query_stage.max_parallelism, cyc_prog_min, new_max_budget,
		(double)p_diff/kProgressMul, cyc_diff, (double)sum_p/kProgressMul, sum_cyc);

	if (query.profile && query.profile->trace_recorder) {
		double measured_speed = p_diff > .0 ? (double)cyc_diff / ((double)p_diff_dbl / kProgressMul) : .0;
		double curr_cyc_tup = row_diff ? (double)cyc_diff / (double)row_diff : 0;

		std::string actions(stream_reoptimizer ?
			stream_reoptimizer->get_current_action_name() : "");

		query.profile->trace_recorder->add_progress_info(parallel_id, query_stage.get_stage_id(),
			progress, cyc_prog_curr, cyc_prog_min, cyc_prog_max,
			measured_speed, new_max_budget,
			p_diff, progress - reopt_last_progress,
			cyc_diff, cyc_until_end, std::move(actions), curr_cyc_tup, min_cyc_tup, row_diff);
	}

	return new_max_budget;
}

bool
Stream::reoptimize_try_explore(double progress)
{
	if (progress > max_execute_progress) {
		LOG_DEBUG("reoptimize_try_explore: Cannot explore anymore %f", progress);

		double cyc_prog_min;
		double cyc_prog_curr;
		int64_t cyc_diff;
		int64_t p_diff;
		int64_t sum_cyc;
		int64_t sum_p;
		reoptimize_try_explore_compute_budget(progress,
			cyc_prog_min, cyc_prog_curr, cyc_diff, p_diff,
			sum_cyc, sum_p);
		return false;
	}
	if (!within_limit_number_threads(query_stage.explore_threads,
			kMaxExploreThreads)) {
		return false;
	}

	bool need_recovery = false;
	bool did_exploration = false;

	try {
		double cyc_prog_min;
		double cyc_prog_curr;
		int64_t cyc_diff;
		int64_t p_diff;
		int64_t sum_cyc;
		int64_t sum_p;
		auto new_max_budget = reoptimize_try_explore_compute_budget(progress,
			cyc_prog_min, cyc_prog_curr, cyc_diff, p_diff,
			sum_cyc, sum_p);

		int64_t old_max_budget = -1;
		int64_t cur_used_budget = -1;

		const auto& budget_manager = stream_reoptimizer->exploration_budget_manager;

		if (budget_manager) {
			budget_manager->update_max_budget(new_max_budget,
				old_max_budget, cur_used_budget);
		}

		ASSERT(budget_manager);
		BudgetUser budget_user(budget_manager.get());

		if (reoptimize_run < 0 && cyc_prog_curr >= cyc_prog_min * 1.05) {
			// explored before, allocate the excess time
			double p = (double)p_diff/kProgressMul;
			int64_t min_time = p * cyc_prog_min;

			ASSERT(cyc_diff > min_time);
			int64_t excess_time = cyc_diff - min_time;

			if (!budget_manager->try_alloc_budget(excess_time, 0)) {
				LOG_WARN("reoptimize_try_explore: out of budget excess time %lld, "
					"cyc_prog_curr %f, cyc_prog_min %f, p_diff %f,"
					"min_time %lld",
					excess_time, cyc_prog_curr, cyc_prog_min, p,
					min_time);
			}
		}

		did_exploration = budget_user.with(
			[&] () {
				LOG_DEBUG("Reoptimize Explore at progress %d, pred_total %f, "
					"new_max_budget %lld, old_max_budget %lld, cur_used_budget %lld,"
					"p_diff %f, cyc_diff %llu, sum_p %f, sum_cyc %llu",
					(int)(100.0 * progress), cyc_prog_min,
					new_max_budget, old_max_budget, cur_used_budget,
					p_diff, cyc_diff, sum_p, sum_cyc);

				budget_user.ensure_sufficient(kDefaultBudget, "stage_reoptimize");

				need_recovery = true;
				stream_reoptimizer->explore(budget_user);
			},
			"explore");

		if (!did_exploration) {
			auto old = query_stage.explore_threads.fetch_sub(1);
			ASSERT(old >= 0);
		}
	} catch (...) {
		auto old = query_stage.explore_threads.fetch_sub(1);
		ASSERT(old >= 0);
		throw;
	};

	if (!did_exploration && need_recovery) {
		LOG_TRACE("Reoptimize: Explore is out of budget. Need to recover");
		stream_reoptimizer->recover("reoptimize_try_explore");
	}

	return did_exploration;
}

void
Stream::stage_reoptimize(scheduler::Task* task)
{
	SCHEDULER_SCOPE(StreamReoptimize, task);

	ASSERT(can_reoptimize);
	ASSERT(!execution_done);

	const auto prof_start = profiling::physical_rdtsc();
	const double progress = get_progress();

	if (!stream_reoptimizer && progress < max_execute_progress) {
		stream_reoptimizer = std::make_unique<adaptive::StreamReoptimizer>(*this);
		stream_reoptimizer->set_bedrock(nullptr);
	}

	if (stream_reoptimizer) {
		stream_reoptimizer->update_progress(progress);
	}

	// when explored before, release one thread
	bool explored_before = reoptimize_run < 0;
	if (explored_before) {
		ASSERT(stream_reoptimizer);

		auto old = query_stage.explore_threads.fetch_sub(1);
		ASSERT(old >= 0);
	}

	if (stream_reoptimizer) {
		if (reoptimize_try_explore(progress)) {
			reoptimize_run = -1;
		} else {
			LOG_DEBUG("Try exploit at progress %d", (int)(100.0 * progress));
			stream_reoptimizer->exploit();

			reoptimize_run = 1;
		}
	}

	// reset from kEndOfStream
	LoleplanPass::apply_source_to_sink(execution_context.root, [&] (auto& op) {
		op->restart();
	});


	// reset counters
	auto clock = profiling::physical_rdtsc();
	prof_last_reopt_stamp = clock;
	prof_sum_reoptimize_time += clock - prof_start;
	prof_num_reoptimize_calls++;
	last_reopt_num_tuples = 0;

	reopt_trigger_countdown = kMinExecuteRuns;
	stage = Stage::kWaitCompilationDone;
}

void
Stream::stage_done(scheduler::Task* task)
{
	check_budget(true);
}

bool
Stream::check_budget(bool print) const
{
	if (!stream_reoptimizer) {
		return true;
	}

	// check budget
	int64_t new_max_budget = compute_max_budget(prof_sum_time,
		query_stage.max_parallelism, max_risk_budget);

	const auto& budget_manager = stream_reoptimizer->exploration_budget_manager;
	if (!budget_manager) {
		return true;
	}

	int64_t old_max = -1;
	int64_t curr_used = -1;
	budget_manager->update_max_budget(-1, old_max, curr_used);

	if (!curr_used) {
		return true;
	}

	int64_t difference = new_max_budget - curr_used;
	double multiple = (double)new_max_budget / (double)curr_used;

	const double margin = 0.1;

	bool used_too_much = (difference + 10*kDefaultBudget > 0) &&
		(multiple < (1.0 - margin));

	if (used_too_much && print) {
		LOG_ERROR("check_budget: Used too much exploration budget. diff=%lld, mul=%f",
			difference, multiple);
	}

	return !used_too_much;
}

std::unique_ptr<Result>
Stream::move_result()
{
	return std::move(result);
}




StreamGenCodeProfData::StreamGenCodeProfData()
{
	code_block_builder_prof = std::make_unique<engine::voila::CodeBlockBuilderProfData>();
}

void
StreamGenCodeProfData::aggregate(const StreamGenCodeProfData& o)
{
	t_produce += o.t_produce;
	t_type += o.t_type;
	t_loctrack += o.t_loctrack;
	t_other += o.t_other;
	t_posttype += o.t_posttype;
	t_lifetime += o.t_lifetime;
	t_code_block += o.t_code_block;
	t_total += o.t_total;
	calls += o.calls;
}

void
StreamGenCodeProfData::to_string(std::ostream& s) const
{
	if (!calls) {
		return;
	}

	s << "produce " << (t_produce * 100 / t_total) << "%, ";
	s << "type " << (t_type * 100 / t_total) << "%, ";
	s << "loctrack " << (t_loctrack * 100 / t_total) << "%, ";
	s << "other " << (t_other * 100 / t_total) << "%, ";
	s << "posttype " << (t_posttype * 100 / t_total) << "%, ";
	s << "lifetime " << (t_lifetime * 100 / t_total) << "%, ";
	s << "code_block " << (t_code_block * 100 / t_total) << "%, ";
	s << "#calls " << calls;
	s << "\n";

	if (code_block_builder_prof) {
		s << "CodeBlockBuilder: ";
		code_block_builder_prof->to_string(s);
	}
}
