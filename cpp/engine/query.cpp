#include "query.hpp"
#include "plan.hpp"
#include "profile.hpp"
#include "budget.hpp"

#include "engine/stream.hpp"
#include "engine/blocked_space.hpp"
#include "engine/voila/flavor.hpp"
#include "system/system.hpp"
#include "system/configuration.hpp"
#include "lolepops/lolepop.hpp"

#include "protoplan.hpp"

#include <sstream>
#include <iostream>

using engine::Stream;
using engine::StreamProf;

Query::Query(excalibur::Context& context, size_t id,
	engine::catalog::Catalog& catalog,
	std::unique_ptr<plan::RelPlan>&& plan,
	const std::shared_ptr<QueryConfig>& given_config)
 : id(id), plan(std::move(plan)), mem_context(nullptr, "query", -1),
	catalog(catalog), sys_context(context), config(given_config)
{
	LOG_DEBUG("Query::new");

	if (!given_config) {
		LOG_TRACE("Query: No config, create default config");
		config = std::make_shared<QueryConfig>();
	}

	if (config->enable_profiling()) {
		profile = std::make_shared<QueryProfile>();

		if (config->enable_trace_profiling()) {
			LOG_WARN("Using trace recorder query=%p", this);
			profile->trace_recorder = std::make_unique<QueryTraceRecorder>();
		}
	}

	ASSERT(config);
	default_flavor_spec = std::make_shared<FlavorSpec>(config.get());
}

void
Query::operator()()
{
	start_clock = std::chrono::high_resolution_clock::now();

	ASSERT(!first_stage);

	gen_loleplan();

	ASSERT(first_stage);
	first_stage->start();
}

void
Query::add_deallocatable(std::unique_ptr<QueryDeallocatableObject>&& n)
{
	ASSERT(!n->next);

	std::unique_lock lock(mutex);
	LOG_DEBUG("Query::add_deallocatable(%p)", n.get());

	n->next = std::move(dealloc_head);
	dealloc_head = std::move(n);
}

#include "engine/stream.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/lolepops/scan.hpp"
#include "engine/lolepops/hash_join.hpp"
#include "engine/lolepops/hash_group_by.hpp"
#include "engine/lolepops/output.hpp"

struct DeallocatableRelOp : QueryDeallocatableObject {
	void dealloc() final {
		op->dealloc_resources();
	}

	DeallocatableRelOp(const engine::RelOpPtr& op)
	 : op(op)
	{
	}

private:
	engine::RelOpPtr op;
};

struct LoleplanGen : plan::NodeVisitor {
	Query& query;
	engine::RelOpPtr root_relop;
	size_t id_counter = 0;

	template<typename T, typename... Args>
	std::shared_ptr<T> mkRelOp(Args&&... args)
	{
		std::shared_ptr<T> r(std::make_shared<T>(std::forward<Args>(args)...));
		query.add_deallocatable(std::make_unique<DeallocatableRelOp>(r));
		return r;
	}

	LoleplanGen(plan::RelPlan& p, Query& query) : query(query) {
		auto root = p.root;
		plan::NodeVisitor::operator()(root);
		finalize();
	}

	void add_to_stage(const std::function<std::shared_ptr<engine::protoplan::PlanOp>(std::shared_ptr<engine::protoplan::PlanOp>&&)>& fun) {
		if (!query.last_stage) {
			new_stream();
		}
		ASSERT(query.last_stage);
		auto& stage = *query.last_stage;

		stage.protoplan_root = fun(std::move(stage.protoplan_root));
		stage.protoplan_root->stable_op_id = ++id_counter;
	}

	void new_stream() const {
		auto stage = std::make_shared<QueryStage>(query);
		if (query.last_stage) {
			query.last_stage->next = stage;
		}
		query.last_stage = stage;
		if (!query.first_stage) {
			query.first_stage = stage;
		}
	}

	void new_pipeline() const {
		new_stream();
	}

	void visit(const plan::Scan& op) override {
		LOG_TRACE("LoleplanGen::Scan '%s'",
			op.table.c_str());

		root_relop = mkRelOp<engine::relop::Scan>(query, op.table,
			op.cols);

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::Scan>(&op, root_relop);
		});
	}

	void visit(const plan::Select& op) override {
		LOG_TRACE("LoleplanGen::Select");

		(*this)(op.child);

		root_relop = mkRelOp<engine::relop::Filter>(query, root_relop);

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::Select>(&op, root_relop,
				std::move(prev_root));
		});
	}

	void visit(const plan::Project& op) override {
		LOG_TRACE("LoleplanGen::Project");

		(*this)(op.child);

		root_relop = mkRelOp<engine::relop::Project>(query, root_relop);

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::Project>(&op, root_relop,
				std::move(prev_root));
		});
	}

	void visit(const plan::HashJoin& op) override {
		LOG_TRACE("LoleplanGen::HashJoin build");

		(*this)(op.build_op);

		auto relop = mkRelOp<engine::relop::HashJoin>(query, root_relop);
		relop->build_relop = root_relop;
		relop->fk1 = op.fk1;

		root_relop = relop;

		{
			std::vector<engine::lolepop::Expr> build_keys;
			for (auto& expr : op.build_keys) {
				ExprTranslator translate;
				build_keys.emplace_back(translate(expr));
			}

			std::vector<engine::lolepop::Expr> build_payloads;
			for (auto& expr : op.build_payloads) {
				ExprTranslator translate;
				build_payloads.emplace_back(translate(expr));
			}

			relop->build_keys = std::move(build_keys);
			relop->build_payloads = std::move(build_payloads);
		}

		// write outputs
		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::JoinWrite>(&op, root_relop,
				std::move(prev_root));
		});

		// build hash table
		new_pipeline();

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::JoinBuild>(&op, root_relop);
		});

		// probe hash table
		new_pipeline();
		LOG_TRACE("LoleplanGen::HashJoin probe");
		(*this)(op.probe_op);

		relop->probe_relop = root_relop;
		root_relop = relop;

		{
			std::vector<engine::lolepop::Expr> probe_keys;
			for (auto& expr : op.probe_keys) {
				ExprTranslator translate;
				probe_keys.emplace_back(translate(expr));
			}

			std::vector<engine::lolepop::Expr> probe_payloads;
			for (auto& expr : op.probe_payloads) {
				ExprTranslator translate;
				probe_payloads.emplace_back(translate(expr));
			}

			relop->probe_keys = std::move(probe_keys);
			relop->probe_payloads = std::move(probe_payloads);
		}

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::JoinProbeDriver>(&op, root_relop,
				std::move(prev_root));
		});

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::JoinProbeCheck>(&op, root_relop,
				std::move(prev_root));
		});

		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::JoinProbeGather>(&op, root_relop,
				std::move(prev_root));
		});
	}

	struct Aggregate {
		const std::string in_name;
		const std::string aggr_func;
		const std::string out_name;
		const std::string tmp_name;

		plan::ExprPtr aggr_into_temp() const {
			return std::make_shared<plan::Assign>(tmp_name,
				std::make_shared<plan::Function>(aggr_func,
					std::vector<plan::ExprPtr> {
						std::make_shared<plan::ColumnId>(in_name)
					}));
		}

		plan::ExprPtr aggr_temp_into_final() const {
			std::string fun(!aggr_func.compare("count") ?
				std::string("sum") : aggr_func);

			return std::make_shared<plan::Assign>(out_name,
				std::make_shared<plan::Function>(fun,
					std::vector<plan::ExprPtr> {
						std::make_shared<plan::ColumnId>(tmp_name)
					}));
		}
	};

	void visit(const plan::HashGroupBy& op) override {
		LOG_TRACE("LoleplanGen::HashGroupBy build");

		(*this)(op.child);

		HashAggrRewriteExprTranslator backward_translate;

		backward_translate.map_function["count"] = "sum";
		size_t stage = 0;

		// analzye aggregates
		std::vector<Aggregate> op_aggregates;
		op_aggregates.reserve(op.aggregates.size());

		for (auto& expr : op.aggregates) {
			std::string col_id("$tmp" + std::to_string(op_aggregates.size()));
			auto assign = std::dynamic_pointer_cast<plan::Assign>(expr);
			ASSERT(assign && assign->children.size() == 1);


			auto aggregate_func = std::dynamic_pointer_cast<plan::Function>(assign->children[0]);
			ASSERT(aggregate_func && aggregate_func->children.size() == 1);

			auto col_ref = std::dynamic_pointer_cast<plan::ColumnId>(aggregate_func->children[0]);
			ASSERT(col_ref);

			op_aggregates.emplace_back(Aggregate { col_ref->name, aggregate_func->name,
				assign->name, col_id });
		}


		auto generate_groupby = [&] (bool local, bool parallel) {
			bool first_stage = stage == 0;
			bool is_global = op.keys.empty();
			auto relop = mkRelOp<engine::relop::HashGroupBy>(query,
				root_relop, local, !first_stage, is_global);
			root_relop = relop;

			bool has_aggregates;

			{
				ExprTranslator translate;

				std::vector<engine::lolepop::Expr> keys;
				keys.reserve(op.keys.size());
				for (auto& expr : op.keys) {
					keys.emplace_back(translate(expr));
				}


				std::vector<engine::lolepop::Expr> payloads;
				payloads.reserve(op.payloads.size());
				for (auto& expr : op.payloads) {
					payloads.emplace_back(translate(expr));
				}


				std::vector<engine::lolepop::Expr> aggregates;
				aggregates.reserve(op_aggregates.size());
				if (parallel && first_stage) {
					for (auto& ag : op_aggregates) {
						aggregates.emplace_back(translate(ag.aggr_into_temp()));
					}
				} else if (parallel && !first_stage) {
					for (auto& ag : op_aggregates) {
						aggregates.emplace_back(translate(ag.aggr_temp_into_final()));
					}
				} else {
					ASSERT(!parallel);
					for (auto& expr : op.aggregates) {
						aggregates.emplace_back(translate(expr));
					}
				}

				has_aggregates = aggregates.size() > 0;


				relop->probe_keys = std::move(keys);
				relop->payloads = std::move(payloads);
				relop->aggregates = std::move(aggregates);
			}

			add_to_stage([&] (auto&& prev_root) {
				return std::make_shared<engine::protoplan::GroupByBuild>(
					&op, root_relop, std::move(prev_root), is_global, has_aggregates);
			});

			if (!local) {
				// partition hash table
				new_pipeline();
				add_to_stage([&] (auto&& prev_root) {
					return std::make_shared<engine::protoplan::GroupByPartition>(&op, root_relop);
				});
			}

			// build hash table
			new_pipeline();
			add_to_stage([&] (auto&& prev_root) {
				return std::make_shared<engine::protoplan::GroupByScan>(&op, root_relop);
			});

			stage++;
		};

		if (false && query.config->parallelism() == 1) {
			generate_groupby(true, false);
		} else {
			// thread local pre-aggregation
			generate_groupby(false, true);

			// final global aggregation
			generate_groupby(true, true);
		}

	}

	void finalize() {
		add_to_stage([&] (auto&& prev_root) {
			return std::make_shared<engine::protoplan::Output>(std::move(prev_root));
		});
	}
};

void
Query::gen_loleplan()
{
	ASSERT(plan);
	LoleplanGen gen(*plan, *this);

	LOG_TRACE("LolePlan generated");
}

void Query::mark_as_done()
{
	auto stop_clock = std::chrono::high_resolution_clock::now();
	auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
		stop_clock - start_clock).count();

	if (profile) {
		profile->query_runtime_us += duration_us;
	}

	{
		std::lock_guard lock(mutex);
		ASSERT(!is_done);
		is_done = true;
		cv_done.notify_all();
	}

	// deallocate resources
	LOG_DEBUG("Query::dealloc_head");
	std::lock_guard lock(mutex);

	auto curr = std::move(dealloc_head);
	while (curr) {
		LOG_DEBUG("dealloc(%p)", curr.get());
		curr->dealloc();
		curr = std::move(curr->next);
	}
}

Query::~Query()
{
	LOG_DEBUG("Query::dealloc_stages");

	// todo: move into another thread, but make sure every stage_dealloc() is already exeuted
	first_stage.reset();
	last_stage.reset();
}

void
Result::dump()
{
	// print result
	int64_t line = 0;
	for (auto& row : rows) {
		LOG_DEBUG("Result[%lld]: %s", line, row.c_str());
		line++;
	}
}

QueryStage::QueryStage(Query& query)
 : query(query),
	max_parallelism(query.config->parallelism()),
	max_parallel_reoptimize(1)
{
	task_list.resize(max_parallelism);
	num_started = 0;
	num_done = 0;

	stage_id = query.num_stages++;

	profile = nullptr;

	stage_exploration_budget_manager = std::make_shared<GlobalBudgetManager>();

	if (query.profile) {
		size_t id = query.profile->stages.size();

		profile = std::make_shared<engine::QueryStageProf>(id, max_parallelism);
		query.profile->stages.push_back(profile);
	}
}

QueryStage::~QueryStage()
{

}

enum ExecStage {
	kInit = 0,
	kExecuteStream,
	kDone,
	kDead
};

struct StreamExecutor : scheduler::FsmTask<ExecStage> {
	QueryStage& stage;
	const size_t parallel_id;
	engine::Stream* stream = nullptr;
	std::unique_ptr<memory::Context> mem_context;

	StreamExecutor(QueryStage& stage)
	 : scheduler::FsmTask<ExecStage>(ExecStage::kInit),
		stage(stage), parallel_id(stage.num_started.fetch_add(1))
	{

	}

	uint32_t dispatch(ExecStage& state) final;

	void stage_done();
	void stage_init();

	void stage_dealloc();

	~StreamExecutor() {
		stage_dealloc();
	}
};

uint32_t
StreamExecutor::dispatch(ExecStage& state)
{
	uint32_t result;
	Stream::ExecResult stream_res;

	while (1) {
		switch (state) {
		case ExecStage::kExecuteStream:
			ASSERT(stream);

			result = scheduler::FsmTaskFlags::kRepeat;
			stream_res = stream->execute();

			// consider_increasing_parallelism();

			if (stream_res == Stream::ExecResult::kEndOfStream) {
				state = ExecStage::kDone;
				LOG_TRACE("StreamExecutor: done");
			} else if (stream_res == Stream::ExecResult::kYield) {
				result |= scheduler::FsmTaskFlags::kYield;
			}
			break;

		case ExecStage::kDone:
			stage_done();
			state = ExecStage::kDead;
			result = scheduler::FsmTaskFlags::kDone;
			break;

		case ExecStage::kInit:
			stage_init();
			state = ExecStage::kExecuteStream;
			result = scheduler::FsmTaskFlags::kRepeat;
			break;

		default:
			ASSERT(false);
			result = scheduler::FsmTaskFlags::kDone;
			break;
		}

		if (result != scheduler::FsmTaskFlags::kRepeat) {
			return result;
		}
	}

}

void
StreamExecutor::stage_done()
{
	LOG_DEBUG("StreamExecutor: stage_done");
	uint64_t prof_move_result = 0;
	uint64_t prof_result_rows = 0;
	uint64_t prof_dealloc = 0;
	uint64_t prof_trigger_next = 0;

	ASSERT(stream);

	if (stage.next) {
		// prepare next task, locally allocated using same affinity
		auto& next = *stage.next;
		int64_t size = next.task_list.size();
		int64_t ins = next.task_list_ins.fetch_add(1);
		ASSERT(size == next.max_parallelism);
		ASSERT(ins <= size);

		auto& entry = next.task_list[ins];
		entry = std::make_shared<StreamExecutor>(next);

		// assign to same queue
		int64_t my_home_queue = this->home_queue_id;
		entry->home_queue_id = my_home_queue;
	} else {
		// move result into query
		auto&& stream_result = stream->move_result();
		auto& query = stage.query;
		auto num_rows = stream_result->size();

		prof_result_rows = num_rows;

		if (num_rows > 0) {
			auto prof_start = profiling::rdtsc();

			std::unique_lock lock(query.mutex);

			query.result.prepare_more(num_rows);
			for (auto&& r : stream_result->rows) {
				query.result.rows.emplace_back(r);
			}

			lock.unlock();

			prof_move_result = profiling::rdtsc() - prof_start;
		}
	}

	// trigger next stage
	{
		bool last_stream;
		{
			auto old_done = stage.num_done.fetch_add(1)+1;
			last_stream = old_done == stage.max_parallelism;
			ASSERT(old_done <= stage.max_parallelism);

			LOG_TRACE("StreamExecutor: old_done %lld, max_parallelism %lld",
				old_done, stage.max_parallelism);

			ASSERT(old_done <= stage.max_parallelism);
		}

		if (UNLIKELY(last_stream)) {
			auto prof_start = profiling::rdtsc();

			if (stage.profile) {
				auto stop_clock = std::chrono::high_resolution_clock::now();
				auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
					stop_clock - stage.profile->clock_start).count();

				stage.profile->duration_us = duration_us;
			}
			if (stage.next) {
				LOG_TRACE("StreamExecutor: trigger next QueryStage");
				stage.next->start();
			} else {
				LOG_TRACE("StreamExecutor: query done");
				stage.query.mark_as_done();
			}

			prof_trigger_next = profiling::rdtsc() - prof_start;
		}
	}

	auto profile = stream->profile;

	// dealloc Stream
	{
		auto prof_start = profiling::rdtsc();
		// stage_dealloc();
		prof_dealloc = profiling::rdtsc() - prof_start;
	}

	if (profile) {
		profile->sum_time_phase[StreamProf::Phase::kMoveResult] += prof_move_result;
		profile->sum_time_phase[StreamProf::Phase::kResultRows] += prof_result_rows;
		profile->sum_time_phase[StreamProf::Phase::kTriggerNextStage] += prof_trigger_next;
		profile->sum_time_phase[StreamProf::Phase::kDealloc] += prof_dealloc;
	}
}

void
StreamExecutor::stage_dealloc()
{
	if (!mem_context) {
		return;
	}

	mem_context->deleteObj(stream);
	mem_context.reset();

	// memory::GlobalAllocator::get().trim();
}

void
StreamExecutor::stage_init()
{
	uint64_t prof_start = profiling::rdtsc();

	auto& query = stage.query;

	auto numa_node = scheduler::get_current_numa_node();
	mem_context = std::make_unique<memory::Context>(&query.mem_context,
		"stream", numa_node);
	// mem_context->simple = true;

	ASSERT(!stream);
	stream = mem_context->newObj<engine::Stream>(query, parallel_id, stage,
		*mem_context);
	ASSERT(stream);

	// Build plan lazily & locally
	ASSERT(stage.protoplan_root);
	ProtoplanTranslator proto_translate(*stream,
		std::make_shared<memory::Context>(nullptr, "stream_planops", numa_node));
	proto_translate(stage.protoplan_root);

	auto& profile = stream->profile;
	if (profile) {
		profile->sum_time_phase[StreamProf::Phase::kAlloc] +=
			profiling::rdtsc() - prof_start;
	}
}

void
QueryStage::start()
{
	LOG_DEBUG("QueryStage::start");
	if (profile) {
		profile->clock_start = std::chrono::high_resolution_clock::now();
	}

	// start remaining tasks
	while (1) {
		size_t ins = task_list_ins.fetch_add(1);
		if (ins >= task_list.size()) {
			break;
		}

		task_list[ins] = std::make_shared<StreamExecutor>(*this);
	}

	// submit batch
	g_scheduler.submit_n(task_list);
}



QueryConfig::QueryConfig()
{
	_updated = true;
	_config = std::make_unique<Configuration>();

	_config->add("parallelism", Configuration::kLong,
		std::to_string(std::thread::hardware_concurrency())
		// "4"
		// "8"
		// "1"
	);

#ifdef IS_DEBUG_BUILD
	_config->add("vector_size", Configuration::kLong, "128");
	_config->add("morsel_size", Configuration::kLong, std::to_string(4*1024));
#else
	_config->add("vector_size", Configuration::kLong, "1024");
	_config->add("morsel_size", Configuration::kLong, std::to_string(16*1024));
#endif
	_config->add("enable_optimizations", Configuration::kBool, "true");

	_config->add("verify_code_fragments", Configuration::kBool,
#ifdef IS_DEBUG_BUILD
		"true"
#else
		"false"
#endif
	);

	_config->add("asynchronous_compilation", Configuration::kBool, "true");
	_config->add("enable_cache", Configuration::kBool, "true");
	_config->add("enable_profiling", Configuration::kBool, "true");
	_config->add("enable_trace_profiling", Configuration::kBool, "false");
	_config->add("dynamic_minmax", Configuration::kBool, "true");
	_config->add("enable_reoptimize", Configuration::kBool, "true");
	_config->add("runtime_feedback", Configuration::kBool, "true");

	_config->add("paranoid_mode", Configuration::kBool,
#ifdef IS_DEBUG_BUILD
		"true"
#else
		"false"
#endif
	);

	_config->add("inline_operators", Configuration::kBool, "true");
	_config->add("enable_learning", Configuration::kBool, "true");
	_config->add("enable_output", Configuration::kBool, "true");
	_config->add("compilation_strategy", Configuration::kString, "adaptive");

	_config->add("compilation_flavor", Configuration::kString, "max_fragment");
	_config->add("compilation_strategy_seed", Configuration::kLong, "13");

	_config->add("has_full_evaluation", Configuration::kBool, "true");
	_config->add("full_evaluation_bit_score_divisor", Configuration::kLong, "64");
	_config->add("adaptive_exploit_test_random", Configuration::kBool,
#ifdef IS_DEBUG_BUILD
		"true"
#else
		"false"
#endif
	);

	_config->add("adaptive_exploit_better_by_margin", Configuration::kDouble, "0.2");
	_config->add("adaptive_random_seed", Configuration::kLong, "23");
	_config->add("adaptive_random_noseed", Configuration::kBool, "true");
	_config->add("adaptive_exploration_strategy", Configuration::kString, "mcts");

	_config->add("max_tuples", Configuration::kDouble,
		std::to_string((double)(1ull << 40ull)));

	_config->add("table_cardinality_multiplier", Configuration::kDouble,
		std::to_string(1.0));

	_config->add("max_risk_budget", Configuration::kDouble,
		std::to_string(0.15));

	_config->add("max_execute_progress", Configuration::kDouble,
		std::to_string(0.4));


	_config->add("default_flavor", Configuration::kString, "");
	_config->add("table_load_factor", Configuration::kDouble, "0.5");
	_updated = true;
}

void
QueryConfig::overwrite(const std::string& s)
{
	std::istringstream is_file(s);
	std::string line;
	while (std::getline(is_file, line, ';')) {
		std::istringstream is_line(line);
		std::string key;
		if (std::getline(is_line, key, '=')) {
			std::string value;
			if (std::getline(is_line, value)) {
				set(key, value);
			}
		}
	}
}

void
QueryConfig::set(const std::string& key, const std::string& val)
{
	LOG_DEBUG("QueryConfig: set '%s' := '%s'",
		key.c_str(), val.c_str());
	_updated = true;
	_config->set(key, val);
}

bool
QueryConfig::get(std::string& out, const std::string& key) const
{
	return _config->get(out, key);
}

void
QueryConfig::refresh() const
{
	_parallelism = _config->get_long("parallelism");
	_vector_size = _config->get_long("vector_size");
	_morsel_size = _config->get_long("morsel_size");
	_max_tuples = _config->get_double("max_tuples");
	_table_cardinality_multiplier = _config->get_double("table_cardinality_multiplier");
	_max_risk_budget = _config->get_double("max_risk_budget");
	_max_execute_progress = _config->get_double("max_execute_progress");

	_default_flavor = _config->get_string("default_flavor");

	_enable_optimizations = _config->get_bool("enable_optimizations");
	_verify_code_fragments = _config->get_bool("verify_code_fragments");

	_asynchronous_compilation = _config->get_bool("asynchronous_compilation");
	_enable_cache = _config->get_bool("enable_cache");
	_enable_profiling = _config->get_bool("enable_profiling");
	_enable_trace_profiling = _config->get_bool("enable_trace_profiling");
	_dynamic_minmax = _config->get_bool("dynamic_minmax");
	_enable_reoptimize = _config->get_bool("enable_reoptimize");
	_runtime_feedback = _config->get_bool("runtime_feedback");
	_paranoid_mode = _config->get_bool("paranoid_mode");
	_inline_operators = _config->get_bool("inline_operators");
	_enable_learning = _config->get_bool("enable_learning");
	_enable_output = _config->get_bool("enable_output");

	std::string compilation_strategy(_config->get_string("compilation_strategy"));
	if (compilation_strategy.empty() || !strcasecmp(compilation_strategy.c_str(), "none")) {
		_compilation_strategy = CompilationStrategy::kNone;
	} else if (!strcasecmp(compilation_strategy.c_str(), "adaptive")) {
		_compilation_strategy = CompilationStrategy::kAdaptive;
	} else if (!strcasecmp(compilation_strategy.c_str(), "expr")) {
		_compilation_strategy = CompilationStrategy::kExpressions;
	} else if (!strcasecmp(compilation_strategy.c_str(), "stmt")) {
		_compilation_strategy = CompilationStrategy::kStatements;
	} else {
		ASSERT(false);
	}

	_has_full_evaluation = _config->get_bool("has_full_evaluation");
	_full_evaluation_bit_score_divisor = _config->get_long("full_evaluation_bit_score_divisor");
	_adaptive_exploit_test_random = _config->get_bool("adaptive_exploit_test_random");
	_adaptive_exploit_better_by_margin = _config->get_double("adaptive_exploit_better_by_margin");
	_adaptive_random_seed = _config->get_long("adaptive_random_seed");
	_adaptive_random_noseed = _config->get_bool("adaptive_random_noseed");

	std::string adaptive_exploration_strategy(_config->get_string("adaptive_exploration_strategy"));
	if (adaptive_exploration_strategy.empty() ||
			!strcasecmp(adaptive_exploration_strategy.c_str(), "heuristic")) {
		_adaptive_exploration_strategy = ExplorationStrategy::kExploreHeuristic;
	} else if (!strcasecmp(adaptive_exploration_strategy.c_str(), "random")) {
		_adaptive_exploration_strategy = ExplorationStrategy::kExploreRandom;
	} else if (!strcasecmp(adaptive_exploration_strategy.c_str(), "random_depth2")) {
		_adaptive_exploration_strategy = ExplorationStrategy::kExploreRandomDepth2;
	} else if (!strcasecmp(adaptive_exploration_strategy.c_str(), "random_max")) {
		_adaptive_exploration_strategy = ExplorationStrategy::kExploreRandomMaxDist;
	} else if (!strcasecmp(adaptive_exploration_strategy.c_str(), "mcts")) {
		_adaptive_exploration_strategy = ExplorationStrategy::kExploreMCTS;
	} else {
		ASSERT(false);
	}

	std::string compilation_flavor(_config->get_string("compilation_flavor"));
	if (compilation_flavor.empty() ||
			!strcasecmp(compilation_flavor.c_str(), "min") ||
			!strcasecmp(compilation_flavor.c_str(), "min_fragment")) {
		_compilation_flavor = CompilationFlavor::kDefault;
	} else if (!strcasecmp(compilation_flavor.c_str(), "max") ||
			!strcasecmp(compilation_flavor.c_str(), "max_fragment")) {
		_compilation_flavor = CompilationFlavor::kMaxFragment;
	} else if (!strcasecmp(compilation_flavor.c_str(), "rand") ||
			!strcasecmp(compilation_flavor.c_str(), "random")) {
		_compilation_flavor = CompilationFlavor::kRandom;
	} else {
		ASSERT(false);
	}

	_compilation_strategy_seed = _config->get_long("compilation_strategy_seed");
	_table_load_factor = _config->get_double("table_load_factor");

	_updated = false;
}

QueryConfig::~QueryConfig()
{

}

void
QueryConfig::with_config(QueryConfig& config,
	const KeyValuePairs& kv_pairs,
	const std::function<void(QueryConfig& config)>& fun,
	bool force_inplace)
{
	auto set_values = [] (auto& config, const KeyValuePairs& values) {
		for (const auto& kv : values) {
			auto& key = std::get<0>(kv);
			auto& val = std::get<1>(kv);

			config.set(key, val);
		}

		config.force_refresh();
	};

	KeyValuePairs old_values;
	old_values.reserve(kv_pairs.size());

	for (auto& kv : kv_pairs) {
		auto& key = std::get<0>(kv);

		std::string old_value;
		bool exists = config.get(old_value, key);
		ASSERT(exists);

		old_values.emplace_back(KeyValuePair {key, old_value});
	}

	// set new values
	set_values(config, kv_pairs);

	// run
	fun(config);

	// reset
	set_values(config, old_values);
}


QueryProfile::QueryProfile()
{
	total_compilation_time_us = 0;
	total_compilation_time_cy = 0;

	total_compilation_wait_time_us = 0;
	total_compilation_wait_time_cy = 0;

	total_code_cache_gen_signature_cy = 0;
	total_code_cache_get_cy = 0;

	query_runtime_us = 0;
}

void
QueryProfile::aggregate(const QueryProfile& o)
{
	total_compilation_time_us += o.total_compilation_time_us.load();
	total_compilation_time_cy += o.total_compilation_time_cy.load();

	total_compilation_wait_time_us += o.total_compilation_wait_time_us.load();
	total_compilation_wait_time_cy += o.total_compilation_wait_time_cy.load();

	total_code_cache_gen_signature_cy += o.total_code_cache_gen_signature_cy.load();
	total_code_cache_get_cy += o.total_code_cache_get_cy.load();

	query_runtime_us += o.query_runtime_us.load();

	if (o.trace_recorder) {
		if (!trace_recorder) {
			trace_recorder = std::make_unique<QueryTraceRecorder>();
		}

		trace_recorder->aggregate(*o.trace_recorder);
	}
}

QueryProfile::~QueryProfile()
{

}

void
QueryProfile::dump()
{
	to_json(std::cout);
}

void
QueryProfile::to_json(std::ostream& o)
{
	o
		<< "{\n"
		<< "\"Header\": \"1\",\n"
		<< "  \"Stats\": {\n"
		<< "    \"RuntimeMs\": " << ((double)query_runtime_us / 1000.0) << ",\n"
		<< "    \"Num_stages\": " << stages.size() << ",\n"
		;

	o << "    \"Stages\" : {";
	for (auto& stage : stages) {
		if (!stage) {
			LOG_WARN("QueryProfile::to_json: Stage has nullptr");
			continue;
		}
		o << "      \"" << stage->get_id() << "\": {";
		stage->to_json(o);
		o << "      },\n";
	}
	o << "\"__Done\" : \"\"\n";
	o << "    } \n";
	o << "  },\n";


	o
		<< "\"Trailer\": \"1\"\n"
		<< "}\n"
		;
}


QueryTraceRecorder::Timestamp
QueryTraceRecorder::get_timestamp()
{
	uint64_t event = event_counter++;
	return Timestamp { event, profiling::rdtsc() };
}

void
QueryTraceRecorder::aggregate(const QueryTraceRecorder& other)
{
	progress_infos.reserve(progress_infos.size() + other.progress_infos.size());
	for (auto& o : other.progress_infos) {
		progress_infos.push_back(o);
	}
}