#pragma once

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>

#include "system/memory.hpp"
#include "system/scheduler.hpp"

namespace excalibur {
struct Context;
}

struct Configuration;
struct StreamExecutor;

namespace plan {
struct RelPlan;
} /* plan */

namespace engine {
struct Stream;
struct QueryStageProf;
struct Ussr;
struct RelOp;

namespace lolepop {
struct Output;
}

namespace adaptive {
struct StreamReoptimizer;
}

namespace protoplan {
struct PlanOp;
}

namespace catalog {
struct Catalog;
}

namespace voila {
struct FlavorSpec;
}

} /* engine */


struct IBudgetManager;

struct Result {
	std::vector<std::string> rows;

	void dump();

	void prepare_more(size_t n) {
		rows.reserve(rows.size() + n);
	}

	size_t size() const {
		return rows.size();
	}
};

struct Query;
struct LoleplanGen;

struct ParallelAccessLimiter {
	struct Access {
		Access(ParallelAccessLimiter& limiter) : limiter(limiter) {
			allowed = limiter.curr_parallel.fetch_add(1) < limiter.max_parallel;
		}

		bool is_allowed() const { return allowed; }

		template<typename T>
		bool with(const T& f) {
			if (is_allowed()) {
				f();
			}
			return is_allowed();
		}

		~Access() {
			limiter.curr_parallel.fetch_sub(1);
		}
	private:
		ParallelAccessLimiter& limiter;
		bool allowed;
	};

	ParallelAccessLimiter(int64_t max_parallel = 1)
	 : max_parallel(max_parallel)
	{
		curr_parallel = 0;
	}
private:
	friend struct Access;
	std::atomic<int64_t> curr_parallel;
	const int64_t max_parallel;
};

struct QueryStageStreamData {
	virtual ~QueryStageStreamData() = default;
};

struct QueryStage {
	void start();

	QueryStage(Query& query);
	~QueryStage();

	size_t get_stage_id() const { return stage_id; }

	Query& get_query() const { return query; }

	std::shared_ptr<IBudgetManager> stage_exploration_budget_manager;

	const int64_t get_max_parallelism() const {
		return max_parallelism;
	}
private:
	friend struct LoleplanGen;
	friend struct StreamExecutor;
	friend struct engine::Stream;
	friend struct engine::adaptive::StreamReoptimizer;

	std::atomic<size_t> num_started;
	std::shared_ptr<QueryStage> next;
	Query& query;
	std::shared_ptr<engine::QueryStageProf> profile;
	std::shared_ptr<engine::protoplan::PlanOp> protoplan_root;

	std::atomic<int64_t> num_done;
	int64_t max_parallelism;

	size_t stage_id;

	std::mutex mutex;

	ParallelAccessLimiter max_parallel_runtime_feedbacks;
	ParallelAccessLimiter max_parallel_reoptimize;

	double runtime_feedback_next_progress = 0.0;
	std::mutex runtime_feedback_mutex;

	std::atomic<int64_t> explore_threads = {0};

	std::atomic<int64_t> task_list_ins = {0};
	std::vector<std::shared_ptr<scheduler::Task>> task_list;

	std::unique_ptr<QueryStageStreamData> stream_data;
};

template<typename T>
inline static bool
within_limit_number_threads(std::atomic<T>& atomic, const T& max)
{
	while (1) {
		auto expected = atomic.load();
		if (expected > max) {
			return false;
		}

		if (atomic.compare_exchange_strong(expected, expected+1)) {
			return true;
		}
	}
}

struct QueryTraceRecorder {
	struct Timestamp {
		uint64_t event_id;
		uint64_t rdtsc;
	};

	struct ProgressInfo {
		Timestamp timestamp;
		size_t stream;
		size_t stage_id;
		double progress;

		double win_curr_cyc_prog;
		double win_min_cyc_prog;
		double win_max_cyc_prog;
		double measured_cyc_prog;
		int64_t new_budget;

		int64_t p_diff;
		double d_diff_dbl;
		int64_t cyc_diff;
		double cyc_until_end;

		double curr_cyc_tup;
		double min_cyc_tup;
		int64_t rows;
		std::string actions;
	};
	std::vector<ProgressInfo> progress_infos;


	void add_progress_info(size_t stream_par_id, size_t stage_id, double progress,
			double cyc_prog_curr, double cyc_prog_min, double cyc_prog_max,
			double cyc_prog_measured, int64_t new_budget, int64_t p_diff, double d_diff_dbl,
			int64_t cyc_diff, double cyc_until_end, const std::string& actions, double cur_cyc_tup, double min_cyc_tup, int64_t rows) {
		std::lock_guard<std::mutex> lock(mutex);
		if (progress_infos.capacity() <= progress_infos.size()+1) {
			progress_infos.reserve(progress_infos.size() + 4*1024);
		}
		progress_infos.push_back(ProgressInfo {get_timestamp(), stream_par_id, stage_id,
			progress, cyc_prog_curr, cyc_prog_min, cyc_prog_max, cyc_prog_measured,
			new_budget, p_diff, d_diff_dbl, cyc_diff, cyc_until_end, cur_cyc_tup, min_cyc_tup, rows, actions});
	}

	void aggregate(const QueryTraceRecorder& other);

private:
	std::mutex mutex;
	std::atomic<uint64_t> event_counter;

	Timestamp get_timestamp();
};

struct QueryProfile {
	std::atomic<uint64_t> total_compilation_time_us;
	std::atomic<uint64_t> total_compilation_time_cy;

	std::atomic<uint64_t> total_compilation_wait_time_us;
	std::atomic<uint64_t> total_compilation_wait_time_cy;
	std::atomic<uint64_t> total_code_cache_gen_signature_cy;
	std::atomic<uint64_t> total_code_cache_get_cy;

	std::atomic<uint64_t> query_runtime_us;

	std::unique_ptr<QueryTraceRecorder> trace_recorder;

	QueryProfile();

	void aggregate(const QueryProfile& o);

	~QueryProfile();

	void dump();
	void to_json(std::ostream& out);

	std::vector<std::shared_ptr<engine::QueryStageProf>> stages;
};

struct QueryConfig {
	using FlavorSpec = engine::voila::FlavorSpec;

	bool asynchronous_compilation() const {
		refresh_if_updated();
		return _asynchronous_compilation;
	}
	bool enable_cache() const {
		refresh_if_updated();
		return _enable_cache;
	}

	bool enable_optimizations() const {
		refresh_if_updated();
		return _enable_optimizations;
	}

	bool enable_profiling() const {
		refresh_if_updated();
		return _enable_profiling;
	}

	bool enable_trace_profiling() const {
		refresh_if_updated();
		return _enable_trace_profiling;
	}

	bool enable_reoptimize() const {
		refresh_if_updated();
		return _enable_reoptimize;
	}

	bool runtime_feedback() const {
		refresh_if_updated();
		return _runtime_feedback;
	}

	bool verify_code_fragments() const {
		refresh_if_updated();
		return _verify_code_fragments;
	}

	bool dynamic_minmax() const {
		refresh_if_updated();
		return _dynamic_minmax;
	}

	size_t parallelism() const {
		refresh_if_updated();
		return _parallelism;
	}

	size_t vector_size() const {
		refresh_if_updated();
		return _vector_size;
	}

	size_t morsel_size() const {
		refresh_if_updated();
		return _morsel_size;
	}

	double max_tuples() const {
		refresh_if_updated();
		return _max_tuples;
	}

	double table_cardinality_multiplier() const {
		refresh_if_updated();
		return _table_cardinality_multiplier;
	}

	double max_risk_budget() const {
		refresh_if_updated();
		return _max_risk_budget;
	}

	double max_execute_progress() const {
		refresh_if_updated();
		return _max_execute_progress;
	}

	std::string default_flavor() const {
		refresh_if_updated();
		return _default_flavor;
	}

	enum CompilationFlavor {
		kDefault = 0,
		kMaxFragment = 1,
		kRandom = 2,
	};

	CompilationFlavor compilation_flavor() const {
		refresh_if_updated();
		return _compilation_flavor;
	}

	uint64_t compilation_strategy_seed() const {
		refresh_if_updated();
		return _compilation_strategy_seed;
	}

	enum CompilationStrategy {
		kExpressions = 1,
		kStatements = 2,
		kNone = 0,
		kAdaptive = 3,
	};

	CompilationStrategy compilation_strategy() const {
		refresh_if_updated();
		return _compilation_strategy;
	}

	bool paranoid_mode() const {
		refresh_if_updated();
		return _paranoid_mode;
	}

	bool inline_operators() const {
		refresh_if_updated();
		return _inline_operators;
	}

	bool enable_learning() const {
		refresh_if_updated();
		return _enable_learning;
	}

	bool enable_output() const {
		refresh_if_updated();
		return _enable_output;
	}

	bool has_simd_opts() const {
		refresh_if_updated();
		return true;
	}

	bool has_full_evaluation() const {
		refresh_if_updated();
		return _has_full_evaluation;
	}

	int full_evaluation_bit_score_divisor() const {
		refresh_if_updated();
		return _full_evaluation_bit_score_divisor;
	}

	double adaptive_exploit_better_by_margin() const {
		refresh_if_updated();
		return _adaptive_exploit_better_by_margin;
	}

	bool adaptive_exploit_test_random() const {
		refresh_if_updated();
		return _adaptive_exploit_test_random;
	}

	int64_t adaptive_random_seed() const {
		refresh_if_updated();
		return _adaptive_random_seed;
	}

	bool adaptive_random_noseed() const {
		refresh_if_updated();
		return _adaptive_random_noseed;
	}

	double table_load_factor() const {
		refresh_if_updated();
		return _table_load_factor;
	}

	enum ExplorationStrategy {
		kExploreRandom = 1,
		kExploreRandomDepth2,
		kExploreHeuristic,
		kExploreRandomMaxDist,
		kExploreMCTS,
	};

	ExplorationStrategy adaptive_exploration_strategy() const {
		refresh_if_updated();
		return _adaptive_exploration_strategy;
	}

	QueryConfig();
	~QueryConfig();

	void overwrite(const std::string& s);
	void set(const std::string& key, const std::string& val);

	bool get(std::string& out, const std::string& key) const;

	void force_refresh() {
		refresh();
	}

	using KeyValuePair = std::pair<std::string, std::string>;
	using KeyValuePairs = std::vector<KeyValuePair>;

	static void with_config(QueryConfig& config,
		const KeyValuePairs& kv_pairs,
		const std::function<void(QueryConfig&)>& fun,
		bool force_inplace = false);

	template<typename T>
	void with_config(const KeyValuePairs& kv_pairs,
		const T& fun)
	{
		with_config(*this, kv_pairs,
			[&] (auto& new_config) {
				// we overwrite this configuration, i.e. no
				// special reference needed
				fun();
			},
			true
		);
	}
private:
	void refresh_if_updated() const {
		if (!_updated) {
			return;
		}

		refresh();
	}

	void refresh() const;

	std::unique_ptr<Configuration> _config;

	mutable bool _enable_optimizations;
	mutable bool _verify_code_fragments;
	mutable bool _asynchronous_compilation;
	mutable bool _enable_cache;
	mutable bool _enable_profiling;
	mutable bool _enable_trace_profiling;
	mutable bool _dynamic_minmax;
	mutable bool _runtime_feedback;
	mutable bool _enable_reoptimize;
	mutable bool _paranoid_mode;
	mutable bool _inline_operators;
	mutable bool _enable_learning;
	mutable bool _enable_output;

	mutable size_t _parallelism;
	mutable size_t _vector_size;
	mutable size_t _morsel_size;

	mutable double _max_tuples;
	mutable double _table_cardinality_multiplier;
	mutable double _max_risk_budget;
	mutable double _max_execute_progress;
	mutable std::string _default_flavor;
	mutable CompilationFlavor _compilation_flavor;
	mutable uint64_t _compilation_strategy_seed;

	mutable CompilationStrategy _compilation_strategy;
	mutable bool _has_full_evaluation;
	mutable long _full_evaluation_bit_score_divisor;
	mutable double _adaptive_exploit_better_by_margin;
	mutable bool _adaptive_exploit_test_random;
	mutable uint64_t _adaptive_random_seed;
	mutable bool _adaptive_random_noseed;
	mutable ExplorationStrategy _adaptive_exploration_strategy;

	mutable double _table_load_factor;

	mutable bool _updated;
};

struct QueryDeallocatableObject {
	virtual void dealloc() = 0;

	virtual ~QueryDeallocatableObject() = default;

private:
	friend struct Query;

	std::unique_ptr<QueryDeallocatableObject> next;
};

struct Query : scheduler::Task {
private:
	friend struct LoleplanGen;
	friend struct StreamExecutor;
	friend struct QueryStage;
	friend struct engine::Stream;
	friend struct engine::RelOp;
	friend struct engine::protoplan::PlanOp;
	friend struct engine::lolepop::Output;

	size_t id;
	std::string query_text;
	std::unique_ptr<plan::RelPlan> plan;

	std::shared_ptr<QueryStage> first_stage;
	std::shared_ptr<QueryStage> last_stage;
	std::mutex mutex;
	memory::Context mem_context;

	bool is_done = false;
	std::condition_variable cv_done;
	std::chrono::time_point<std::chrono::high_resolution_clock> start_clock;

	size_t num_stages = 0;

	void gen_loleplan();
	void mark_as_done();

	size_t stable_op_id_counter = 0;

	std::unique_ptr<QueryDeallocatableObject> dealloc_head;

public:
	Result result;

	engine::catalog::Catalog& catalog;
	excalibur::Context& sys_context;

	Query(excalibur::Context& context, size_t id,
		engine::catalog::Catalog& catalog,
		std::unique_ptr<plan::RelPlan>&& plan,
		const std::shared_ptr<QueryConfig>& config = nullptr);
	~Query();

	void operator()() final;

	static std::shared_ptr<Query>
	from_rel_plan(std::unique_ptr<plan::RelPlan>&& plan);

	void wait() {
		std::unique_lock lock(mutex);
		cv_done.wait(lock, [&] { return is_done; });
	}

	size_t get_query_id() const { return id; }

	std::shared_ptr<QueryConfig> config;

	// Profiling information, can be NULL
	// Needs to be shared_ptr, because some code fragments can be still compiled
	// when the query already terminated
	std::shared_ptr<QueryProfile> profile;

	using FlavorSpec = engine::voila::FlavorSpec;
	std::shared_ptr<FlavorSpec> default_flavor_spec;

	void add_deallocatable(std::unique_ptr<QueryDeallocatableObject>&& n);
};
