#include "excalibur_context.hpp"

#include "system/build.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"

#include "engine/voila/code_cache.hpp"
#include "engine/query.hpp"
#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "engine/types.hpp"
#include "engine/storage/csv_loader.hpp"
#include "engine/adaptive/brain.hpp"
#include "tpch_test_utils.hpp"

#include "test_utils.hpp"

#include "third_party/cxxopts.hpp"

#include <memory>
#include <iostream>
#include <fstream>
#include <thread>

using namespace std;


// #define TRACE_AND_DEBUG

static bool g_output;
static std::string g_output_string;
static bool g_append;
static int g_threads;
static int g_workers;
static std::string g_host;
static std::string g_mode;
static int g_scale_factor;
static bool g_compact_data_types = true;

static const std::string kEnableCodeCache("enable_cache");
static const std::string kCompilationStrategy("compilation_strategy");
static const std::string kInlineOperators("inline_operators");
static const std::string kDefaultFlavor("default_flavor");
static const std::string kEnableReoptimize("enable_reoptimize");
static const std::string kAdaptiveExplorationStrategy("adaptive_exploration_strategy");
static const std::string kParallelism("parallelism");

static const std::string kMaxRiskBudget("max_risk_budget");
static const std::string kEnableLearning("enable_learning");
static const std::string kEnableOutput("enable_output");
static const std::string kMaxExecuteProgress("max_execute_progress");
static const std::string kEnableProfiling("enable_profiling");
static const std::string kEnableTraceProfiling("enable_trace_profiling");




using RelOpPtr = plan::RelOpPtr;
using Builder = plan::Builder;
using ExprPtr = plan::ExprPtr;

typedef std::vector<ExprPtr> ExprPtrVec;



void print_profile(ostream& f, QueryProfile& prof, size_t runs)
{
	f	<< prof.total_compilation_time_us.load()
		<< "|" << prof.total_compilation_time_cy
		<< "|" << prof.total_compilation_wait_time_us.load()
		<< "|" << prof.total_compilation_wait_time_cy.load()
		<< "|" << prof.total_code_cache_gen_signature_cy.load()
		<< "|" << prof.total_code_cache_get_cy.load()
		<< "|" << prof.query_runtime_us.load();
}
struct Experiment {
	virtual ~Experiment() = default;
	Experiment() {

	}

	virtual std::string get_name() const = 0;

	virtual void operator()(QueryProfile& profile) = 0;

	virtual void prepare() {}

	virtual void check_results() const {}
};

static std::unordered_map<std::string,
	std::shared_ptr<engine::catalog::Catalog>> tpch_catalogs;

struct TpchExperiment : Experiment {
	const std::string scale_factor;
	const int query = 9;

	void prepare() override {
		const bool compact = g_compact_data_types;
		if (!m_catalog) {
			auto& catalog = tpch_catalogs[scale_factor];

			if (!catalog) {
				LOG_WARN("Load sf %s", scale_factor.c_str());
				const std::string postfix(compact ? "compact" : "");
				catalog = std::make_shared<engine::catalog::Catalog>();
				const auto tpch_csv_files = TpchUtils::get_files(scale_factor);
				if (!catalog->get_num_tables()) {
					TpchUtils::create_schema(*catalog, compact, scale_factor);

					for (auto& file_mapping : tpch_csv_files) {
						auto& table_name = file_mapping.first;
						auto& file_name = file_mapping.second;

						LOG_WARN("Load table %s", table_name.c_str());

						auto table = catalog->get_table(table_name);
						ASSERT(table);

						engine::storage::CsvLoader csv(*table);

						csv.from_file(file_name, postfix);
						csv.flush();

						ASSERT(table->get_num_rows() > 0);
					}
				}
			}

			m_catalog = catalog.get();
		}
		root_op = TpchUtils::get_query(query);
	}

	TpchExperiment(int query, const std::string& scale_factor = "0.01")
	 : scale_factor(scale_factor), query(query) {

	}

	std::string get_name() const override {
		return "q" + std::to_string(query);
	}

	void operator()(QueryProfile& profile) override {
		result = TestUtils::run_query(*m_catalog, root_op, &profile,
			"/tmp/exp_qperf_q" + std::to_string(query) + "_sf" + scale_factor + ".json");
	}

	void check_results() const override {
		if (g_output) {
			auto expected_rows = TpchUtils::get_results(scale_factor, query);

			TestUtils::compare_results_assert(result, expected_rows);
		}
	}

	std::vector<std::string> result;

	engine::catalog::Catalog* m_catalog = nullptr;
	plan::RelOpPtr root_op;
};


struct TpchQ6Experiment : TpchExperiment {
	TpchQ6Experiment(int year, int discount, const std::string& quantity,
		int variant, const std::string& scale_factor = "0.01")
	 : TpchExperiment(6, scale_factor), year(year), discount(discount),
		quantity(quantity), variant(variant) {
	}

	void prepare() override {
		TpchExperiment::prepare();

		root_op = TpchUtils::get_q6(variant, year, discount, quantity);
	}

	void check_results() const override {
	}

	const int year;
	const int discount;
	const std::string quantity;
	const int variant;
};



static long g_repetitions;

void
run_compilation_time(ostream& f)
{
	std::vector<std::shared_ptr<Experiment>> queries;

	for (auto& query_id : TpchUtils::get_all_queries()) {
		queries.emplace_back(std::make_shared<TpchExperiment>(query_id));
	}

	auto& cache = TestUtils::get_excalibur_context().get_code_cache();

	for (auto& query : queries) {
		query->prepare();

		cache.clear();

		// warm up
		for (size_t i=0; i<2; i++) {
			QueryProfile profile;
			(*query)(profile);
		}

		size_t runs = g_repetitions;
		// cold run
		{

			QueryProfile profile;

			for (size_t i=0; i<runs; i++) {
				cache.clear();

				(*query)(profile);
			}

			f << g_host << "|"
				<< build::GitMeta::commit_sha() << "|"
				<< query->get_name() << "|false|" << g_threads << "|";
			print_profile(f, profile, runs);
			f << endl;
		}

		// hot run
		{
			QueryProfile profile;

			for (size_t i=0; i<runs; i++) {
				(*query)(profile);
			}

			f << g_host << "|"
				<< build::GitMeta::commit_sha() << "|"
				<< query->get_name() << "|true|" << g_threads << "|";
			print_profile(f, profile, runs);
			f << endl;
		}

	}
}


void
run_code_cache(ostream& f, std::string sf)
{
	// runs default small scale factor
	std::vector<std::shared_ptr<Experiment>> queries;

	for (auto& query_id : TpchUtils::get_all_queries()) {
		if (query_id == 1 || query_id == 9 || query_id == 18) {
			queries.emplace_back(std::make_shared<TpchExperiment>(query_id));
		}
	}

	auto& cache = TestUtils::get_excalibur_context().get_code_cache();
	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	for (auto& query : queries) {
		query->prepare();

		// warm up
		for (size_t i=0; i<2; i++) {
			QueryProfile profile;
			(*query)(profile);
		}

		size_t runs = g_repetitions;

		for (bool enable_reoptimize : {false}) {
			for (auto cache_size : std::vector<int64_t>({
					0, 1, 8, 16, 32, 64, 128, 256, 512, 1024, 16*1024, 128*1024, 1024*1024})) {
				QueryProfile profile;

				bool enable_cache = cache_size > 0;
				config->with_config({
					QueryConfig::KeyValuePair { kEnableCodeCache,
						enable_cache ? "true" : "false" },
					QueryConfig::KeyValuePair { kEnableReoptimize,
						enable_reoptimize ? "true" : "false" },
					QueryConfig::KeyValuePair { kParallelism, std::to_string(1) },
					QueryConfig::KeyValuePair { kEnableLearning, "false" },
					
				}, [&] () {
					TestUtils::get_excalibur_context().get_brain().clear();
					cache.clear(enable_cache ? cache_size : 0);
					for (size_t i=0; i<runs; i++) {
						cache.wait_until_quiet();
						
						(*query)(profile);
					}

					f << g_host << "|"
						<< build::GitMeta::commit_sha() << "|"
						<< query->get_name() << "|" << cache_size << "|" << g_threads << "|"
						<< enable_reoptimize << "|" << g_threads << "|";
					print_profile(f, profile, runs);
					f << endl;
				});
			}
		}
	}
}
struct RunQueryArgs {
	std::string prefix = "";
	std::string prefix2 = "";
	bool print = true;

	size_t warmup_runs = 3;
	bool print_every_run = false;
	bool clear_memory_after_warmup = false;
	bool clear_cache_after_warmup = false;
};

static void run_query(ostream& f,
	Experiment& query,
	RunQueryArgs* args = nullptr)
{
	RunQueryArgs default_args;
	if (!args) {
		args = &default_args;
	}

	query.prepare();

	struct Entry {
		size_t run;
		double profile_t_ms;
		double outside_t_ms;
	};
	std::vector<Entry> times;

	auto& exc_ctx = TestUtils::get_excalibur_context();
	exc_ctx.get_code_cache().wait_until_quiet();
	exc_ctx.get_brain().clear();

	// warm up
	for (size_t i=0; i<args->warmup_runs; i++) {
		LOG_ERROR("Query %s", query.get_name().c_str());

		auto& config = TestUtils::get_config().query;
		ASSERT(config);

		config->with_config({
			QueryConfig::KeyValuePair { kMaxRiskBudget, "0.9"},
			QueryConfig::KeyValuePair { kMaxExecuteProgress, "0.99"},
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		}, [&] () {
			QueryProfile profile;
			(query)(profile);
			if (g_scale_factor <= 10) {
				query.check_results();
			}
		});
	}

	if (args->clear_memory_after_warmup) {
		exc_ctx.get_brain().clear();
	}
	if (args->clear_cache_after_warmup) {
		exc_ctx.get_code_cache().clear();
	}

	size_t runs = g_repetitions;
	size_t runs_completed = 0;

	// hot run
	QueryProfile profile;

	double total_ms = 0.0;

	auto print_times = [&] (int64_t run_id, const auto& times) {
		ASSERT(times.size() > 0);
		double t_min = times[0].profile_t_ms;
		double t_max = times[times.size()-1].profile_t_ms;
		double t_median;
		if ((times.size() % 2) == 0) {
			size_t half = times.size() / 2;

			ASSERT(half > 0);
			ASSERT(half-1 < times.size());
			ASSERT(half < times.size());
			t_median = 0.5 * (times[half-1].profile_t_ms + times[half].profile_t_ms);
		} else {
			size_t half = times.size() / 2;

			ASSERT(half < times.size());
			t_median = times[half].profile_t_ms;
		}

		if (args->print) {
			f
				<< args->prefix
				<< g_host << "|"
				<< build::GitMeta::commit_sha() << "|"
				<< run_id << "|"
				<< args->prefix2
				<< query.get_name() << "|" << g_threads << "|"
				<< (total_ms / (double)runs_completed) << "|"
				<< (double)profile.query_runtime_us / (runs_completed * 1000.0) << "|"
				<< t_min << "|"
				<< t_max << "|"
				<< t_median
				<< endl;
		}
	};

	for (size_t i=0; i<runs; i++) {
		LOG_ERROR("Query %s", query.get_name().c_str());
		auto start_clock = std::chrono::high_resolution_clock::now();

		(query)(profile);

		auto stop_clock = std::chrono::high_resolution_clock::now();
		auto duration_ms = (double)std::chrono::duration_cast<std::chrono::microseconds>(
			stop_clock - start_clock).count() / 1000.0;

		if (g_scale_factor <= 10) {
			query.check_results();
		}

		runs_completed++;
		total_ms += duration_ms;

		times.emplace_back(Entry {
			i, duration_ms, (double)profile.query_runtime_us/1000.0
		});

		if (args->print_every_run) {
			print_times(i, times);
			times.clear();
		}
	}

	if (!args->print_every_run) {
		// sort for correct median
		std::sort(times.begin(), times.end(),
			[] (const auto& a, const auto& b) {
				return a.profile_t_ms < b.profile_t_ms;
			});
		print_times(0, times);
	}

	TestUtils::get_excalibur_context().get_code_cache().wait_until_quiet();
	TestUtils::get_excalibur_context().get_brain().clear();
}



struct QperfSetting {
	int id;
	std::string name;
	std::string compilation_strategy_name;
	std::string enable_reoptimize;
	std::string adaptive_exploration_strategy;
};

void
run_qperf(ostream& f, std::string scale_factor)
{
	std::vector<std::shared_ptr<Experiment>> queries;
	for (auto& query_id : TpchUtils::get_all_queries()) {
		queries.emplace_back(std::make_shared<TpchExperiment>(query_id,
			scale_factor
		));
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		QperfSetting {5, "adaptive-mcts", "adaptive", "true", "mcts"},
		QperfSetting {1, "adaptive-h", "adaptive", "true", "heuristic"},
		QperfSetting {2, "adaptive-r", "adaptive", "true", "random"},
		// QperfSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2"},
		// QperfSetting {4, "adaptive-rm", "adaptive", "true", "random_max"},
		QperfSetting {6, "statements", "stmt", "false", ""},
		QperfSetting {7, "interpret", "none", "false", ""},
		// QperfSetting {8, "expressions", "expr", "false"},
	};

#if 0
	// pre-populate code cache
	for (auto& strat : {"heuristic", "random", "random_depth2"}) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, "adaptive" },
			QueryConfig::KeyValuePair { kEnableReoptimize, "true" },
			QueryConfig::KeyValuePair { kMaxRiskBudget, "0.99"},
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, strat },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query : queries) {
				query->prepare();

				for (size_t i=0; i<2; i++) {
					QueryProfile profile;
					(*query)(profile);
				}
			}
		});
	}
#endif
	auto& exc_ctx = TestUtils::get_excalibur_context();
	exc_ctx.get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query : queries) {
				RunQueryArgs args;
				args.prefix2 = setting.name + std::string("|");
				run_query(f, *query, &args);
			}
		});
	}
}

void
run_q6(ostream& f, std::string scale_factor)
{
	struct Q6Info {
		std::shared_ptr<TpchExperiment> query;
		int variant, year, discount;
		std::string quantity;
	};

	std::vector<Q6Info> queries;

	std::vector<std::string> quantities = {"1", "5", "10", "15"};

	for (int variant : {-1, 0}) {
		for (auto& quantity : quantities) {
			for (int discount : {1, 4, 9}) {
				for (int year : {1992, /*1993, 1994,*/ 1995, /*1996, 1997,*/ 1999}) {
					queries.emplace_back(Q6Info {
						std::make_shared<TpchQ6Experiment>(year, discount, quantity,
							variant, scale_factor),
						variant, year, discount, quantity
					});
				}
			}
		}
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		QperfSetting {1, "adaptive-h", "adaptive", "true", "heuristic"},
#if 0
		QperfSetting {2, "adaptive-r", "adaptive", "true", "random"},
		QperfSetting {5, "adaptive-mcts", "adaptive", "true", "mcts"},
		QperfSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2"},
		QperfSetting {4, "adaptive-rm", "adaptive", "true", "random_max"},
#endif
		QperfSetting {6, "statements", "stmt", "false", ""},
		QperfSetting {7, "interpret", "none", "false", ""},
		//QperfSetting {7, "expressions", "expr", "false"},
	};

	auto& exc_ctx = TestUtils::get_excalibur_context();
	exc_ctx.get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query_tuple : queries) {
				auto& query = query_tuple.query;
				RunQueryArgs args;
				std::stringstream ss;
				ss << setting.name << "|"
					<< query_tuple.variant << "|"
					<< query_tuple.discount << "|"
					<< query_tuple.year << "|"
					<< query_tuple.quantity << "|";
				args.prefix2 = ss.str();
				run_query(f, *query, &args);
			}
		});
	}
}

struct QriskSetting {
	int id;
	std::string name;
	std::string compilation_strategy_name;
	std::string enable_reoptimize;
	std::string adaptive_exploration_strategy;
	std::string enable_learning;
};

void
run_qrisk(ostream& f, std::string scale_factor)
{
	std::vector<std::shared_ptr<Experiment>> queries;
	for (auto& query_id : TpchUtils::get_all_queries()) {
		if (query_id == 1 /* || query_id == 18 */) {
			queries.emplace_back(std::make_shared<TpchExperiment>(query_id,
				scale_factor
			));
		}
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
#if 1
		  QriskSetting {1, "adaptive-h", "adaptive", "true", "heuristic", "false"},
		  QriskSetting {2, "adaptive-r", "adaptive", "true", "random", "false"},
		  QriskSetting {6, "adaptive-h-learn", "adaptive", "true", "heuristic", "true"},
		  QriskSetting {7, "adaptive-r-learn", "adaptive", "true", "random", "true"},
		  QriskSetting {11, "statements", "stmt", "false", "", "false"},
		  QriskSetting {12, "interpret", "none", "false", "", "false"},
#endif

		// QriskSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2", "false"},
		  QriskSetting {4, "adaptive-rm", "adaptive", "true", "random_max", "false"},
		  QriskSetting {5, "adaptive-mcts", "adaptive", "true", "mcts", "false"},
		  
		// QriskSetting {8, "adaptive-r2-learn", "adaptive", "true", "random_depth2", "true"},
		  QriskSetting {9, "adaptive-rm-learn", "adaptive", "true", "random_max", "true"},
		  QriskSetting {10, "adaptive-mcts-learn", "adaptive", "true", "mcts", "true"},
		// QriskSetting {13, "expressions", "expr", "false", "", "false"},
	};

	TestUtils::get_excalibur_context().get_code_cache().wait_until_quiet();

	for (auto max_risk_budget : {"0.0", "0.05", "0.1", "0.2", "0.3", "0.5"}) {
		for (auto& setting : settings) {
			config->with_config({
				QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
				QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
				QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
				QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
				QueryConfig::KeyValuePair { kMaxRiskBudget, max_risk_budget},
				QueryConfig::KeyValuePair { kEnableLearning, setting.enable_learning },
				QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
			},
			[&] () {
				for (auto& query : queries) {
					RunQueryArgs args;
					std::stringstream ss;
					ss << setting.name << std::string("|") << max_risk_budget << "|" ;
					args.prefix2 = ss.str();
					if (setting.id < 11) {
						args.warmup_runs = 10;
					}
					run_query(f, *query, &args);
				}
			});
		}
	}
}

void
run_adapt(ostream& f, std::string scale_factor)
{
	std::vector<std::shared_ptr<Experiment>> queries;
	for (auto& query_id : TpchUtils::get_all_queries()) {
		if (query_id == 1 || query_id == 9) {
			queries.emplace_back(std::make_shared<TpchExperiment>(query_id,
				scale_factor
			));
		}
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		QriskSetting {1, "adaptive-h", "adaptive", "true", "heuristic", "false"},
		QriskSetting {2, "adaptive-r", "adaptive", "true", "random", "false"},
		QriskSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2", "false"},
		QriskSetting {4, "adaptive-rm", "adaptive", "true", "random_max", "false"},
		QriskSetting {5, "adaptive-mcts", "adaptive", "true", "mcts", "false"},
		QriskSetting {6, "adaptive-h-learn", "adaptive", "true", "heuristic", "true"},
		QriskSetting {7, "adaptive-r-learn", "adaptive", "true", "random", "true"},
		QriskSetting {8, "adaptive-r2-learn", "adaptive", "true", "random_depth2", "true"},
		QriskSetting {9, "adaptive-rm-learn", "adaptive", "true", "random_max", "true"},
		QriskSetting {10, "adaptive-mcts-learn", "adaptive", "true", "mcts", "true"},
		QriskSetting {11, "statements", "stmt", "false", "", "false"},
		QriskSetting {12, "interpret", "none", "false", "", "false"},
		QriskSetting {13, "expressions", "expr", "false", "", "false"},
	};

	TestUtils::get_excalibur_context().get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableLearning, setting.enable_learning },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query : queries) {
				RunQueryArgs args;
				std::stringstream ss;
				ss << setting.name << "|" ;
				args.prefix2 = ss.str();
				args.clear_memory_after_warmup = true;
				args.clear_cache_after_warmup = true;
				args.warmup_runs = 0;
				args.print_every_run = true;
				run_query(f, *query, &args);
			}
		});
	}
}

void
run_speed_plot(ostream& f, std::string scale_factor)
{
	std::vector<std::shared_ptr<TpchExperiment>> queries;
	for (auto& query_id : TpchUtils::get_all_queries()) {
		if (query_id == 1 || query_id == 12) {
			queries.emplace_back(std::make_shared<TpchExperiment>(query_id,
				scale_factor
			));
		}
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		QperfSetting {5, "adaptive-mcts", "adaptive", "true", "mcts"},
		QperfSetting {1, "adaptive-h", "adaptive", "true", "heuristic"},
		//QperfSetting {2, "adaptive-r", "adaptive", "true", "random"},
		//QperfSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2"},
		// QperfSetting {4, "adaptive-rm", "adaptive", "true", "random_max"},
		// QperfSetting {6, "statements", "stmt", "false", ""},
		// QperfSetting {7, "interpret", "none", "false", ""},
	};

	auto& exc_ctx = TestUtils::get_excalibur_context();
	exc_ctx.get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string },
			QueryConfig::KeyValuePair { kEnableProfiling, "true"},
			QueryConfig::KeyValuePair { kEnableTraceProfiling, "true"},
			QueryConfig::KeyValuePair { kEnableLearning, "false"},
		},
		[&] () {
			auto print_run = [&] (auto& query, auto& profile) {
				const auto query_id = query.query;
				ASSERT(profile.trace_recorder);
				for (auto& info : profile.trace_recorder->progress_infos) {
					f << setting.id << "|" << g_host << "|" << query_id << "|"
						<< info.stream << "|" << info.stage_id << "|" << info.progress << "|"
						<< info.win_min_cyc_prog << "|" << info.win_curr_cyc_prog << "|"
						<< info.measured_cyc_prog << "|" << info.new_budget << "|"
						<< info.actions << "|" << info.d_diff_dbl << "|"
						<< info.timestamp.rdtsc
						<< "|" << info.curr_cyc_tup << "|" << info.min_cyc_tup << "|" << info.rows
						<< "\n";
				}
			};

			for (auto& query : queries) {
				query->prepare();

				// warm up
				for (size_t i=0; i<5; i++) {
					QueryProfile profile;
					(*query)(profile);
				}
				
				// run
				QueryProfile profile;

				(*query)(profile);

				print_run(*query, profile);
			}
		});
	}
}

struct JoinDataExperiment : Experiment {
	static std::vector<std::string> get_all_probe_table_names() {
		return { "probe1", "probe2", "probe3" };
	}

	static std::vector<std::string> get_all_build_table_names() {
		return { "build1", "build2", "build3", "build4"};
	}

	static std::vector<std::string> get_all_group_table_names() {
		return {"group1", "group2", "group3"};
	}
private:
	void create_schema(engine::catalog::Catalog& catalog) {
		for (auto& build_name : get_all_build_table_names()) {
			auto table = std::make_unique<engine::catalog::FlatArrayTable>();

			for (size_t i=0; i<4; i++) {
				table->add_column(
					std::make_shared<engine::catalog::AppendableFlatArrayColumn>(
						engine::SqlType::from_string("i64")),
					std::string("col") + std::to_string(i));
			}

			catalog.add_table(build_name, std::move(table));
		}

		for (auto& probe_name : get_all_probe_table_names()) {
			auto table = std::make_unique<engine::catalog::FlatArrayTable>();

			for (size_t i=0; i<2; i++) {
				table->add_column(
					std::make_shared<engine::catalog::AppendableFlatArrayColumn>(
						engine::SqlType::from_string("i64")),
					std::string("col") + std::to_string(i));
			}

			catalog.add_table(probe_name, std::move(table));
		}

		for (auto& probe_name : get_all_group_table_names()) {
			auto table = std::make_unique<engine::catalog::FlatArrayTable>();

			for (size_t i=0; i<2; i++) {
				table->add_column(
					std::make_shared<engine::catalog::AppendableFlatArrayColumn>(
						engine::SqlType::from_string("i64")),
					std::string("col") + std::to_string(i));
			}

			catalog.add_table(probe_name, std::move(table));
		}
	}

	void load_data(engine::catalog::Catalog& catalog, std::string path) {
		std::unordered_map<std::string, std::string> map {
			{"build1", "join/build_10000000.csv"},
			{"build2", "join/build_10000000_off9900000.csv"},
			{"build3", "join/build_10000000_off990000.csv"},
			{"build4", "join/build_10000000_off5000000.csv"},
			{"probe1", "join/probe_4000000_200000000.csv"},
			{"probe2", "join/probe_2000000000_4000000.csv"},
			{"probe3", "join/probe_12000000_120000000.csv"},

			{"group1", "group/group_2000000_100000000.csv"},
			{"group2", "group/group_100000000_2000000.csv"},
			{"group3", "group/group_6000000_60000000.csv"},
		};
		for (auto& file_mapping : map) {
			auto& table_name = file_mapping.first;
			auto file_name = path + std::string("/") + file_mapping.second;

			LOG_WARN("Load table %s", table_name.c_str());

			auto table = catalog.get_table(table_name);
			ASSERT(table);

			engine::storage::CsvLoader csv(*table);

			csv.from_file(file_name);
			csv.flush();

			ASSERT(table->get_num_rows() > 0);
		}
	}

public:
	void prepare() override {
		if (!m_catalog) {
			auto& catalog = tpch_catalogs["join"];

			if (!catalog) {
				std::string file(TpchUtils::get_prefix_directory() + "/generate_exp.py");
				int r = system(file.c_str());
				if (r) {
					LOG_ERROR("cannot run '%s'. ret=%d", file.c_str(), r);
					ASSERT(!r);
				}

				catalog = std::make_shared<engine::catalog::Catalog>();
				if (!catalog->get_num_tables()) {
					create_schema(*catalog);
					load_data(*catalog, TpchUtils::get_prefix_directory() + "/");
				}
			}

			m_catalog = catalog.get();
		}
	}

	JoinDataExperiment(const std::string& query)
	 : query(query) {}

	std::string get_name() const override {
		return query;
	}

	void operator()(QueryProfile& profile) override {
		result = TestUtils::run_query(*m_catalog, root_op, &profile);
	}

	void check_results() const override {
		// TODO:
	}

	std::vector<std::string> result;

	engine::catalog::Catalog* m_catalog = nullptr;
	plan::RelOpPtr root_op;
	const std::string query;
};

struct JoinExperiment : JoinDataExperiment {
	JoinExperiment(const std::string& query, const std::string& probe_table,
		const std::string& build_table)
	 : JoinDataExperiment(query), probe_table(probe_table), build_table(build_table) {}

	void prepare() override {
		JoinDataExperiment::prepare();

		root_op = get_query(probe_table, build_table);
	}


	static RelOpPtr
	get_join(const std::string& probe_table, const std::string& build_table)
	{
		Builder builder;

		RelOpPtr probe = builder.scan(probe_table, {
			"col0", "col1"
		});

		RelOpPtr build = builder.scan(build_table, {
			"col0", "col1", "col2", "col3"
		});

		build = builder.project(build,
			ExprPtrVec {
				builder.assign("colA", builder.column("col0")),
				builder.assign("colB", builder.column("col1")),
				builder.assign("colC", builder.column("col2")),
				builder.assign("colD", builder.column("col3")),
			});

		probe = builder.hash_join(
			build,
			ExprPtrVec { builder.column("colA") },
			ExprPtrVec { builder.column("colB"), builder.column("colC"), builder.column("colD")},

			probe,
			ExprPtrVec { builder.column("col0") },
			ExprPtrVec { builder.column("col1") }
		);
		return probe;
	}

	static RelOpPtr
	get_query(const std::string& probe_table, const std::string& build_table)
	{
		Builder builder;
		auto probe = get_join(probe_table, build_table);

		RelOpPtr aggr = builder.hash_group_by(probe,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("sum", builder.function("sum", builder.column("col1")))
			});

		return aggr;
	}

	const std::string probe_table;
	const std::string build_table;
};

struct GroupExperiment : JoinExperiment {
	GroupExperiment(const std::string& query, const std::string& probe_table)
	 : JoinExperiment(query, probe_table, "") {}

	void prepare() override {
		JoinDataExperiment::prepare();

		root_op = get_query(probe_table);
	}

	static RelOpPtr
	get_query(const std::string& probe_table)
	{
		Builder builder;

		RelOpPtr probe = builder.scan(probe_table, {
			"col0", "col1"
		});

		RelOpPtr aggr = builder.hash_group_by(probe,
			ExprPtrVec {
				builder.column("col0"),
			},
			ExprPtrVec {
				builder.assign("sum1", builder.function("sum", builder.column("col1")))
			});

		return aggr;
	}
};

struct JoinGroupExperiment : JoinExperiment {
	JoinGroupExperiment(const std::string& query, const std::string& probe_table, const std::string& build_table)
	 : JoinExperiment(query, probe_table, build_table) {}

	void prepare() override {
		JoinDataExperiment::prepare();

		root_op = get_query(probe_table, build_table);
	}

	RelOpPtr
	get_query(const std::string& probe_table, const std::string& build_table)
	{
		RelOpPtr probe = get_join(probe_table, build_table);
		Builder builder;

		RelOpPtr aggr = builder.hash_group_by(probe,
			ExprPtrVec {
				builder.column("col0"),
			},
			ExprPtrVec {
				builder.assign("sum1", builder.function("sum", builder.column("colA"))),
				builder.assign("sum2", builder.function("sum", builder.column("colB")))
			});

		return aggr;
	}
};

void
run_join(ostream& f)
{
	std::vector<std::shared_ptr<Experiment>> queries;

	size_t i=0;
	for (auto& build : JoinDataExperiment::get_all_build_table_names()) {
		for (auto& probe : JoinDataExperiment::get_all_probe_table_names()) {
			queries.emplace_back(std::make_shared<JoinExperiment>(
				std::string("join") + std::to_string(i), probe, build));
#if 0
			// takes waaay to long
			queries.emplace_back(std::make_shared<JoinGroupExperiment>(
				std::string("jgroup") + std::to_string(i), probe, build));
#endif
			i++;
		}
	}

#if 0
	i = 0;
	for (auto& probe : JoinDataExperiment::get_all_group_table_names()) {
		// takes too long too
		queries.emplace_back(std::make_shared<GroupExperiment>(
			std::string("group") + std::to_string(i), probe));
		i++;
	}
#endif
	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		QperfSetting {1, "adaptive-h", "adaptive", "true", "heuristic"},
		QperfSetting {2, "adaptive-r", "adaptive", "true", "random"},
		// QperfSetting {3, "adaptive-r2", "adaptive", "true", "random_depth2"},
		// QperfSetting {4, "adaptive-rm", "adaptive", "true", "random_max"},
		QperfSetting {5, "adaptive-mcts", "adaptive", "true", "mcts"},
		QperfSetting {6, "statements", "stmt", "false", ""},
		QperfSetting {7, "interpret", "none", "false", ""},
		// QperfSetting {8, "expressions", "expr", "false"},
	};

	auto& exc_ctx = TestUtils::get_excalibur_context();
	exc_ctx.get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query : queries) {
				RunQueryArgs args;
				args.prefix2 = setting.name + std::string("|");
				run_query(f, *query, &args);
			}
		});
	}
}

#include <sys/time.h>
#include <sys/resource.h>

void
run_code_frag_footprint(ostream& f)
{
	std::vector<std::shared_ptr<Experiment>> queries;
	for (auto& query_id : TpchUtils::get_all_queries()) {
		queries.emplace_back(std::make_shared<TpchExperiment>(query_id, "1"));
	}

	auto& query = queries[0];
	query->prepare();

	auto& cache = TestUtils::get_excalibur_context().get_code_cache();
	cache.set_cleanup(false);
	cache.set_all_unique(true);

	size_t run_id = 0;
	cache.clear();
	cache.reset_stats();

	while (run_id < 1000) {
		auto& cache = TestUtils::get_excalibur_context().get_code_cache();

		QueryProfile profile;

		for (size_t i=0; i<5; i++) {
			(*query)(profile);

			run_id++;
		}

		rusage usage;
		int r = getrusage(RUSAGE_SELF, &usage);
		ASSERT(!r);

		auto stats = cache.get_stats();

		f << g_host << "|"
			<< build::GitMeta::commit_sha() << "|"
			<< run_id << "|"
			<< stats.num_cached_requests << "|"
			<< usage.ru_maxrss << "|"
			<< (double)((double)usage.ru_maxrss / stats.num_cached_requests)
			<< endl;

	}

	cache.set_cleanup(true);
	cache.set_all_unique(false);
}


struct TpchQ1Experiment : TpchExperiment {
	TpchQ1Experiment(const std::string& shipdate, const std::string& scale_factor = "0.01")
	 : TpchExperiment(1, scale_factor), shipdate(shipdate) {
	}

	void prepare() override {
		TpchExperiment::prepare();

		root_op = TpchUtils::get_q1(shipdate);
	}

	void check_results() const override {
	}

	const std::string shipdate;
};


struct Q1SelTuple {
	std::string shipdate;
	int select;
};

static std::vector<Q1SelTuple>
get_q1_seltuples()
{
	return {
		Q1SelTuple { "1999-09-02", 100 },
		Q1SelTuple { "1998-09-20", 99 },
		Q1SelTuple { "1998-09-02", 98 },
		Q1SelTuple { "1997-02-10", 75 },

		Q1SelTuple { "1995-10-17", 50 },
		Q1SelTuple { "1993-10-25", 25 },
		Q1SelTuple { "1992-12-16", 12 },
		Q1SelTuple { "1992-7-25", 6 },
		Q1SelTuple { "1991-12-01", 0 },

		// on sf10
	};
}

void run_q1selectivity(ostream& f, std::string sf)
{
	struct Q1SelQuery {
		std::shared_ptr<TpchExperiment> query;
		int select;
	};

	struct Q1SelSetting {
		int id;
		std::string name;
		std::string compilation_strategy_name;
		std::string inline_operators;
		std::string flavor;
		std::string enable_reoptimize;
		std::string adaptive_exploration_strategy;
	};

	std::vector<Q1SelTuple> selconfig(get_q1_seltuples());

	std::vector<Q1SelQuery> queries;

	for (auto& cfg : selconfig) {
		queries.emplace_back(Q1SelQuery {
			std::make_shared<TpchQ1Experiment>(cfg.shipdate, sf),
			cfg.select
		});
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		Q1SelSetting {1, "interpret", 		"none", "false", "pred=0", 	"false", ""},
		Q1SelSetting {2, "interpret pred1", 	"none", "false", "pred=1", 	"false", ""},
		Q1SelSetting {2, "interpret pred2", 	"none", "false", "pred=2", 	"false", ""},
		Q1SelSetting {3, "statements",		"stmt", "true", "pred=0", 	"false", ""},
		Q1SelSetting {4, "statements pred1",	"stmt", "true", "pred=1", 	"false", ""},
		Q1SelSetting {4, "statements pred2",	"stmt", "true", "pred=2", 	"false", ""},
		Q1SelSetting {5, "adaptive-h",	"adaptive", "false", "", 			"true", "heuristic"},
		Q1SelSetting {6, "adaptive-r",	"adaptive", "false", "", 			"true", "random"},
		Q1SelSetting {7, "adaptive-r2",	"adaptive", "false", "", 			"true", "random_depth2"},
	};

	// pre-populate code cache
	for (auto& strat : {"heuristic", "random", "random_depth2"}) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, "adaptive" },
			QueryConfig::KeyValuePair { kEnableReoptimize, "true" },
			QueryConfig::KeyValuePair { kMaxRiskBudget, "0.99"},
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, strat },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query_tuple : queries) {
				auto& query = query_tuple.query;
				query->prepare();

				for (size_t i=0; i<2; i++) {
					QueryProfile profile;
					(*query)(profile);
				}
			}
		});
	}

	TestUtils::get_excalibur_context().get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kDefaultFlavor, setting.flavor },
			QueryConfig::KeyValuePair { kInlineOperators, setting.inline_operators },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query_tuple : queries) {
				auto& query = query_tuple.query;

				RunQueryArgs args;
				std::stringstream ss;
				ss << setting.name << std::string("|") << query_tuple.select << "|" ;
				args.prefix2 = ss.str();

				run_query(f, *query, &args);
			}
		});
	}
}



static plan::RelOpPtr
make_sum_query(const std::string& shipdate)
{
	Builder builder;

	auto sql_date = SqlType::from_string("date");

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_shipdate", "l_extendedprice", "l_quantity"
	});

	auto lt_shipdate = builder.function("<=",
		builder.column("l_shipdate"), builder.constant(shipdate));


	lineitem = builder.select(lineitem, lt_shipdate);

#if 0
	lt_shipdate = builder.function(">",
		builder.column("l_shipdate"), builder.constant("1990-09-02"));
	lineitem = builder.select(lineitem, lt_shipdate);
#else
	lt_shipdate = builder.function("=",
		builder.column("l_shipdate"), builder.constant("1996-03-13"));
	lineitem = builder.select(lineitem, lt_shipdate);
#endif

	RelOpPtr project = builder.project(lineitem, ExprPtrVec {
		builder.assign("mul",
			builder.function("*",
				builder.column("l_extendedprice"), builder.column("l_quantity")
			)
		)
	});

	RelOpPtr aggr = builder.hash_group_by(project,
		ExprPtrVec {},
		ExprPtrVec {
			builder.assign("sum", builder.function("sum", builder.column("mul"))),
		});

	return aggr;
}

struct SumExperiment : TpchExperiment {
	SumExperiment(const std::string& shipdate, const std::string& scale_factor = "0.01")
	 : TpchExperiment(1, scale_factor), shipdate(shipdate) {
	}

	void prepare() override {
		TpchExperiment::prepare();

		root_op = make_sum_query(shipdate);
	}

	const std::string shipdate;
};


void run_sumselectivity(ostream& f, std::string sf)
{
	struct Q1SelQuery {
		std::shared_ptr<TpchExperiment> query;
		int select;
	};

	struct Q1SelSetting {
		int id;
		std::string name;
		std::string compilation_strategy_name;
		std::string inline_operators;
		std::string flavor;
		std::string enable_reoptimize;
		std::string adaptive_exploration_strategy;
	};

	std::vector<Q1SelTuple> selconfig(get_q1_seltuples());

	std::vector<Q1SelQuery> queries;

	for (auto& cfg : selconfig) {
		queries.emplace_back(Q1SelQuery {
			std::make_shared<SumExperiment>(cfg.shipdate, sf),
			cfg.select
		});
	}

	auto& config = TestUtils::get_config().query;
	config = std::make_shared<QueryConfig>();

	std::vector settings = {
		Q1SelSetting {1, "interpret", 		"none", "false", "pred=0", 	"false", ""},
		Q1SelSetting {2, "interpret pred1", 	"none", "false", "pred=1", 	"false", ""},
		Q1SelSetting {3, "interpret pred2", 	"none", "false", "pred=2", 	"false", ""},
		Q1SelSetting {4, "statements",		"stmt", "true", "pred=0", 	"false", ""},
		Q1SelSetting {5, "statements pred1",	"stmt", "true", "pred=1", 	"false", ""},
		Q1SelSetting {6, "statements pred2",	"stmt", "true", "pred=2", 	"false", ""},
		Q1SelSetting {7, "adaptive-h",	"adaptive", "false", "", 			"true", "heuristic"},
		Q1SelSetting {8, "adaptive-r",	"adaptive", "false", "", 			"true", "random"},
		Q1SelSetting {9, "adaptive-r2",	"adaptive", "false", "", 			"true", "random_depth2"},
	};

	// pre-populate code cache
	for (auto& strat : {"heuristic", "random", "random_depth2"}) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, "adaptive" },
			QueryConfig::KeyValuePair { kEnableReoptimize, "true" },
			QueryConfig::KeyValuePair { kMaxRiskBudget, "0.99"},
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, strat },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query_tuple : queries) {
				auto& query = query_tuple.query;
				query->prepare();

				for (size_t i=0; i<2; i++) {
					QueryProfile profile;
					(*query)(profile);
				}
			}
		});
	}

	TestUtils::get_excalibur_context().get_code_cache().wait_until_quiet();

	for (auto& setting : settings) {
		config->with_config({
			QueryConfig::KeyValuePair { kCompilationStrategy, setting.compilation_strategy_name },
			QueryConfig::KeyValuePair { kDefaultFlavor, setting.flavor },
			QueryConfig::KeyValuePair { kInlineOperators, setting.inline_operators },
			QueryConfig::KeyValuePair { kEnableReoptimize, setting.enable_reoptimize },
			QueryConfig::KeyValuePair { kAdaptiveExplorationStrategy, setting.adaptive_exploration_strategy },
			QueryConfig::KeyValuePair { kParallelism, std::to_string(g_threads) },
			QueryConfig::KeyValuePair { kEnableOutput, g_output_string }
		},
		[&] () {
			for (auto& query_tuple : queries) {
				auto& query = query_tuple.query;

				RunQueryArgs args;
				std::stringstream ss;
				ss << setting.name << std::string("|") << query_tuple.select << "|" ;
				args.prefix2 = ss.str();

				run_query(f, *query, &args);
			}
		});
	}
}




void run(ostream& f)
{
	cerr << "Scheduler: Using " << g_workers << " worker threads" << endl;
	cerr << "Query: Using " << g_threads << " as parallelism" << endl;

	g_scheduler.init(g_workers);

	const std::string sf(std::to_string(g_scale_factor));

	if (!g_mode.compare("qperf")) {
		run_qperf(f, sf);
	} else if (!g_mode.compare("qrisk")) {
		run_qrisk(f, sf);
	} else if (!g_mode.compare("adapt")) {
		run_adapt(f, sf);
	} else if (!g_mode.compare("q6")) {
		run_q6(f, sf);
	} else if (!g_mode.compare("q1sel")) {
		run_q1selectivity(f, sf);
	} else if (!g_mode.compare("join")) {
		run_join(f);
	} else if (!g_mode.compare("sumsel")) {
		run_sumselectivity(f, sf);
	} else if (!g_mode.compare("compile")) {
		run_compilation_time(f);
	} else if (!g_mode.compare("code_cache")) {
		run_code_cache(f, sf);
	} else if (!g_mode.compare("fragment_footprint")) {
		run_code_frag_footprint(f);
	} else if (!g_mode.compare("speed_plot")) {
		run_speed_plot(f, sf);
	} else {
		cerr << "Invalid mode " << g_mode << endl;
	}
}

int
main(int argc, char** argv)
{
	cxxopts::Options options("Run experiment", "");

	options.add_options()
		("mode", "Mode. One of 'qperf', 'compile', 'fragment_footprint', 'q1sel', 'sumsel'",
			cxxopts::value<string>()->default_value("qperf"))
		("o,out", "Output file name", cxxopts::value<string>()->default_value(""))
		("host", "Host name", cxxopts::value<string>()->default_value("local"))
		("append", "Append")
		("compact", "Use compact data types")
		("no-compact", "Do not use compact data types. Overwrite --compact")
		("output", "Generate outputs")
		("s,scale-factor", "Scale factor", cxxopts::value<int>()->default_value("10"))
		("j,threads", "Number of threads", cxxopts::value<int>()->default_value(
#if 0
			"1"
#else
			to_string(thread::hardware_concurrency())
#endif
			))
		("workers", "Number of worker threads", cxxopts::value<int>()->default_value("0"))
		("r,reps", "Repetitions", cxxopts::value<long>()->default_value("21"))
		("h,help", "Print usage")
		;

	auto args = options.parse(argc, argv);

	if (args.count("help")) {
		cout << options.help() << endl;
		exit(0);
	}

	g_output = args["output"].count();
	g_output_string = g_output ? "true" : "false";
	g_mode = args["mode"].as<string>();
	g_host = args["host"].as<string>();
	g_append = args["append"].count();


	if (args["compact"].count()) {
		g_compact_data_types = true;
	}

	// overwrite --compact
	if (args["no-compact"].count()) {
		g_compact_data_types = false;
	}

	g_threads = args["threads"].as<int>();
	ASSERT(g_threads > 0);
	g_workers = g_threads;

	{
		int w = args["workers"].as<int>();
		if (w > 0) {
			g_workers = w;
		}
	}

	g_scale_factor = args["scale-factor"].as<int>();
	ASSERT(g_scale_factor >= 1);
	g_repetitions = args["reps"].as<long>();
	ASSERT(g_repetitions >= 1);

	const auto& file_name = args["out"].as<string>();

	if (file_name.empty()) {
		run(cout);
	} else {
		fstream f;
		f.open(file_name, g_append ? ios::out | ios::app : ios::out);
		if (!f) {
			cerr << "Cannot create file '" << file_name << "'" << endl;
			return 1;
		}

		try {
			run(f);
			f.close();
		} catch (...) {
			f.close(),
			throw;
		}
	}

	return 0;
}
