#pragma once

#include <vector>
#include <memory>
#include <unordered_set>

#include "system/memory.hpp"

struct Query;
struct QueryStage;
struct Result;
struct BudgetUser;

namespace scheduler {
struct Task;
} /* scheduler */

namespace engine {
struct StringHeap;
struct QueryStageProf;
struct StreamProf;
} /* engine */

namespace engine::adaptive {
struct StreamReoptimizer;
struct Actions;
struct ActionsContext;
} /* engine::adaptive */

namespace engine::lolepop {
struct Lolepop;
struct Output;
} /* engine::lolepop */

namespace engine::voila {
struct Context;
struct GlobalCodeContext;
struct ReadPosition;
struct CodeBlock;
struct CodeCache;
struct CodeBlockBuilderProfData;
} /* engine::voila */

namespace engine {

struct StreamGenCodeProfData {
	uint64_t t_produce = 0;
	uint64_t t_type = 0;
	uint64_t t_loctrack = 0;
	uint64_t t_other = 0;

	uint64_t t_posttype = 0;
	uint64_t t_lifetime = 0;

	uint64_t t_code_block = 0;

	uint64_t t_total = 0;
	uint64_t calls = 0;

	std::unique_ptr<engine::voila::CodeBlockBuilderProfData> code_block_builder_prof;

	void aggregate(const StreamGenCodeProfData& o);

	void to_string(std::ostream& s) const;

	StreamGenCodeProfData();
};

struct Stream {
	const size_t parallel_id;
	QueryStage& query_stage;
	Query& query;

	struct ExecutionContext {
		adaptive::Actions* adaptive_decision;
		std::shared_ptr<adaptive::ActionsContext> adaptive_decision_ctx;

		std::shared_ptr<lolepop::Lolepop> root;

		// Set of all voila::ReadPosition iterators
		std::unordered_set<voila::ReadPosition*> source_read_positions;
		std::shared_ptr<engine::voila::GlobalCodeContext> voila_global_context;

		std::shared_ptr<voila::Context> voila_context;

		void reset();
		void alloc(Stream& stream);

		ExecutionContext();
		~ExecutionContext();
	};

	ExecutionContext execution_context;

	memory::Context& mem_context;

	std::unique_ptr<StringHeap> global_strings;

	std::shared_ptr<StreamProf> profile;
	std::shared_ptr<QueryStageProf> stage_profile;

	const bool has_runtime_feedback;
	const bool can_reoptimize;

	bool recovered = false;


private:
	engine::voila::CodeCache& code_cache;

	friend struct lolepop::Output;
	friend struct adaptive::StreamReoptimizer;

	std::unique_ptr<Result> result;

	size_t evalid = 0;
	size_t ip = 0;

	bool first_run_reoptimize = true;
	int reoptimize_run = 0;

	enum Stage {
		kInit = 1,
		kGenerateCode = 2,
		kPrepare = 3,
		kWaitCompilationDone = 4,
		kExecute = 5,
		kProfile = 6,
		kReoptimize = 7,
		kDone = 8
	};
	Stage stage = Stage::kInit;
	std::unique_ptr<adaptive::StreamReoptimizer> stream_reoptimizer;

public:
	double get_progress() const;

	enum ExecResult {
		kEndOfStream,
		kYield,
		kData,
	};
	ExecResult execute();
	Stream(Query& query, size_t parallel_id, QueryStage& stage, memory::Context& mem_context);
	~Stream();

	std::unique_ptr<Result> move_result();

	size_t get_parallel_id() const { return parallel_id; }

	bool is_compilation_done() const;

	bool is_execution_done() const { return execution_done; }

	// called from VOILA byte-code
	void scan_feedback(voila::ReadPosition& read_pos, bool& fetch_next);
	void scan_feedback_with_tuples(voila::ReadPosition& read_pos);

private:
	using CodeBlock = engine::voila::CodeBlock;

	void stage_gen_code(scheduler::Task* task, bool internal, bool use_budget,
		StreamGenCodeProfData* prof, BudgetUser* budget_user);
	void stage_prepare(scheduler::Task* task, bool internal);
	bool stage_compilation_done(scheduler::Task* task);
	ExecResult stage_execute(scheduler::Task* task, int64_t worker_id, int64_t numa_node);
	void stage_reoptimize(scheduler::Task* task);
	bool reoptimize_try_explore(double progress);
	int64_t reoptimize_try_explore_compute_budget(double progress,
		double& cyc_prog_min, double& cyc_prog_curr, int64_t& cyc_diff,
		int64_t& p_diff, int64_t& sum_cyc, int64_t& sum_p);
	void reoptimize_update_progress(
		double& out_min_cyc_p, double& out_max_cyc_p, int64_t& sum_p, int64_t& sum_cyc,
		double& out_curr_cyc_p, double& min_cyc_tup, int64_t progress, int64_t cycles, int64_t rows);

	bool op_gen_code_block(std::shared_ptr<lolepop::Lolepop>& op, size_t op_id,
		bool use_budget, StreamGenCodeProfData* prof, BudgetUser* budget_user);

	void stage_done(scheduler::Task* task);
	bool check_budget(bool print) const;

	void trigger_internal_profile_propagation();

	uint64_t prof_sum_time = 0;
	uint64_t prof_last_reopt_stamp = 0;

	uint64_t prof_sum_reoptimize_time = 0;
	uint64_t prof_num_reoptimize_calls = 0;

	bool execution_done = false;

	//! Counts down, if <= 0, when can trigger reoptimization
	int64_t reopt_trigger_countdown;
	uint64_t reopt_num_calls = 0;

	const double max_risk_budget;
	const double max_execute_progress;

	void* invalid_access_sandbox_space = nullptr;
	void* invalid_access_zero_sandbox_space = nullptr;

	double reopt_last_progress = .0;
	uint64_t reopt_last_cycles = 0;
	uint64_t reopt_exec_start_cycles = 0;
	uint64_t last_reopt_num_tuples = 0;

public:
	void** get_pointer_to_invalid_access_sandbox_space() {
		return &invalid_access_sandbox_space;
	}

	void** get_pointer_to_invalid_access_zero_sandbox_space() {
		return &invalid_access_zero_sandbox_space;
	}
};

struct StreamBuilderException {
	StreamBuilderException(const std::string& msg) {}
};

} /* engine */
