#include "brain.hpp"

#include "decisions.hpp"
#include "metrics.hpp"
#include "rule_triggers.hpp"
#include "plan_profile.hpp"
#include "engine/query.hpp"
#include "utils/cache.hpp"

#include <unordered_set>
#include <map>
#include <random>
#include <sstream>
#include <shared_mutex>

#include "engine/query.hpp"
#include "engine/budget.hpp"
#include "engine/protoplan.hpp"
#include "system/system.hpp"
#include "system/profiling.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/lolepops/voila_lolepop.hpp"
#include "rule_triggers_impl.hpp"

static const size_t kMinTuples = 50* 1000;
static const size_t kMinCycles = 100*1000;
static const double kStabilityMargin = 2.0;

static const size_t kRunBaseExploreModulo = 10;
static const size_t kRunBaseExploitModulo = 10;

static const double kImproveMargin = 0.1;

using namespace engine;
using namespace adaptive;

using FlavorSpec = engine::voila::FlavorSpec;

template<typename T>
struct ReservoirSampleBuilder {
	ReservoirSampleBuilder(size_t size, std::mt19937& gen, std::vector<T>& arr)
	 : arr(arr), max_size(size), gen(gen) {
		arr.reserve(max_size);
	}

	struct LazyAppend {
		LazyAppend(ReservoirSampleBuilder& sample) : sample(sample) {
			can_push = sample.can_add(idx, back);
		}

		bool can_append() const { return can_push; }

		void push(const T& a) {
			if (!can_append()) {
				return;
			}
			if (back) {
				sample.arr.push_back(a);
				sample.size++;
			} else {
				sample.arr[idx] = a;
			}
			sample.i++;
		}

		void emplace(T&& a) {
			if (!can_append()) {
				return;
			}
			if (back) {
				sample.arr.emplace_back(std::move(a));
				sample.size++;
			} else {
				sample.arr[idx] = std::move(a);
			}
			sample.i++;
		}

	private:
		ReservoirSampleBuilder& sample;
		bool can_push;
		bool back;
		size_t idx;
	};


	void push(const T& a) {
		LazyAppend append(*this);
		append.push(a);
	}

	void emplace(T&& a) {
		LazyAppend append(*this);
		append.emplace(a);
	}

private:
	std::vector<T>& arr;
	const size_t max_size;
	size_t size = 0;
	size_t i = 0;
	std::mt19937& gen;

	friend struct LazyAppend;

	bool can_add(size_t& out_idx, bool& out_back) {
		out_back = false;
		out_idx = 0;
		if (size < max_size) {
			out_back = true;
			return true;
		}
		// replace random index
		std::uniform_int_distribution<size_t> distrib(0, i+1);

		size_t j = distrib(gen);
		if (j < max_size) {
			out_idx = j;
			return true;
		}
		return false;
	}
};


namespace engine::adaptive {
struct PipelineInfo;
}


namespace engine::adaptive {

struct DecisionPerfInfo {
	uint64_t tot_cycles;
	uint64_t tot_tuples;

	size_t buf_insert_pos;
	size_t size;

	static const size_t kWindowSize = 32;

	uint64_t buf_cycles[kWindowSize];
	uint64_t buf_tuples[kWindowSize];


	void reset() {
		for (size_t i=0; i<kWindowSize; i++) {
			buf_tuples[i] = 0;
			buf_cycles[i] = 0;
		}

		tot_cycles = 0;
		tot_tuples = 0;
		buf_insert_pos = 0;
		size = 0;
	}

	DecisionPerfInfo() {
		 reset();
	}

	void push(const Metrics& metrics) {
		size_t i = buf_insert_pos;

		tot_cycles -= buf_cycles[i];
		buf_cycles[i] = metrics.num_cycles;
		tot_cycles += buf_cycles[i];

		tot_tuples -= buf_tuples[i];
		buf_tuples[i] = metrics.num_tuples;
		tot_tuples += buf_tuples[i];		

		buf_insert_pos = buf_insert_pos+1;
		if (buf_insert_pos == kWindowSize) {
			buf_insert_pos = 0;
		}
		if (size < kWindowSize) {
			size++;
		}
		ASSERT(buf_insert_pos < kWindowSize);
	}

	double get_cycles_per_tuple() const {
		return (double)tot_cycles / (double)tot_tuples;
	}

	size_t count() const { return size; }

	bool is_full() const { return count() == kWindowSize; }

	size_t get_sum_tuples() const { return tot_tuples; }
	size_t get_sum_cycles() const { return tot_cycles; }

	template<typename T>
	void for_each_index(const T& f) const {
		size_t n = is_full() ? kWindowSize : buf_insert_pos;
		for (size_t i=0; i<n; i++) {
			f(i);
		}
	}

	double get_cyc_tup_std_dev() const {
		ASSERT(count() > 1);

		const double avg = get_cycles_per_tuple();
		const double bessel_adjust = count() - 1;

		double sum = 0.0;
		for_each_index([&] (auto i) {
			if (buf_tuples[i] < 100) {
				return;
			}
			double cyc_tup = (double)buf_cycles[i] / (double)buf_tuples[i];
			double diff = avg - cyc_tup;
			sum += diff*diff;
		});

		return sqrt((1/bessel_adjust) * sum);
	}
};

struct ChainInfo;

struct DecisionInfo {
	DecisionPerfInfo perf;

	double progress_when_discovered = 1.0;
	double progress_when_use_perf = 1.0;

	mutable std::mutex mutex;

	enum Status : int {
		kUnexplored,

		kPending,

		kFailed,
		kSucceeded,

		kHasProfiling
	};
	std::atomic<Status> status = { Status::kPending };

	bool can_use_perf() const {
		return status.load() == kHasProfiling;
	}

	std::shared_ptr<engine::lolepop::Lolepop> example_plan;
	std::shared_ptr<engine::voila::Context> example_voila_context;

	size_t explore_used = 0;
	std::atomic<size_t> exploit_used = { 0 };

	ChainInfo* memory_info = nullptr;
};

struct ActionsHasher {
	size_t operator()(Actions* a) const {
		if (!a) {
			return 42;
		}
		return a->hash();
	}
};

struct ActionsEqual {
	bool operator()(Actions* const a, Actions const* b) const {
		if (!a || !b) {
			return !a == !b;
		}
		return a->equals(*b);
	}
};

using ExploredActionsSet = std::unordered_set<Actions*, ActionsHasher, ActionsEqual>;

template<typename T>
static void
with_temp_actions(std::vector<Actions>& tmp_actions,
		Actions* previous,
		const std::vector<Decision*>& decisions,
		const T& fun) {
	ASSERT(!decisions.empty());

	// build actions
	if (tmp_actions.size() < decisions.size()) {
		tmp_actions.resize(decisions.size());
	}

	Actions* prev = previous;
	for (size_t i=0; i<decisions.size(); i++) {
		tmp_actions[i] = Actions(decisions[i], prev);
		prev = &tmp_actions[i];
	}

	fun(prev);
}

static bool can_extend_actions(Actions* previous,
	const ExploredActionsSet& set,
	const std::vector<Decision*>& decisions,
	std::vector<Actions>& tmp_actions,
	Actions** out_action = nullptr)
{
	bool found = false;

	with_temp_actions(tmp_actions, previous, decisions,
		[&] (auto prev) {
			auto it = set.find(prev);
			found = it != set.end();
			if (found && out_action) {
				*out_action = *it;
			}
		});

	return !found;
}

struct RuleIterator {
	struct Data {
		std::vector<Decision*> generated_choices;
	};


	RuleIterator(
		RuleTrigger::Input& input,
		DecisionFactory& decision_factory,
		const ExploredActionsSet& explored_set)
	 : rule_scan(*input.brain.rule_triggers, input),
	rule_gen(rule_scan, input, decision_factory),
	rule_filter(rule_gen,
		[&] (auto& iter) {
			auto& data = iter.get_data();
			ASSERT(!data.generated_choices.empty());
			return can_extend_actions(input.previous, explored_set,
				data.generated_choices, tmp_actions);
		})
	{
		tmp_actions.resize(8);
	}

	auto& get() { return rule_filter; }

private:
	RuleSpaceScan<Data> rule_scan;
	RuleSpaceGenerate<Data, decltype(rule_scan)> rule_gen;
	RuleSpaceFilter<Data, decltype(rule_gen)> rule_filter;

	std::vector<Actions> tmp_actions;
};



struct QueryStageShared {
	size_t num_threads = 0;

	std::shared_mutex shared_mutex;

	std::unordered_map<Actions*, DecisionInfo> infos;

	std::unique_ptr<ExplorationStrategy> exploration_strategy;

	// allocated Actions
	std::vector<std::shared_ptr<Actions>> all_actions;
	std::unordered_map<Actions*, std::shared_ptr<Actions>,
		ActionsHasher, ActionsEqual> all_actions_map;

	std::vector<Actions*> explored_actions;
	ExploredActionsSet explored_actions_set;

	std::atomic<size_t> prof_total_decisions = { 0 };
	std::atomic<size_t> prof_total_updates = { 0 };

	PlanProfile plan_profile;

	std::atomic<size_t> exploit_iteration = { 0 };
	size_t explore_iteration = 0;
	std::atomic<int64_t> num_explore_threads = { 0 };

	std::atomic<int64_t> exploit_unstable = { 0 };

	DecisionFactory decision_factory;
	const Brain& brain;

	size_t num_explore_calls = 0;
	size_t sum_explore_cycles = 0;

	size_t num_exploit_calls = 0;
	size_t sum_exploit_cycles = 0;

	size_t num_feedback_calls = 0;
	size_t sum_feedback_cycles = 0;

	// Set of actions, for which we do not require good performance info
	std::unordered_set<Actions*> quick_intermediate_steps;

	std::vector<Actions*> actions_to_verify;
	PipelineInfo* pipeline_info = nullptr;
	std::string pipeline_sign;

	void discard_exploration() {
		all_actions.clear();
		all_actions_map.clear();
		explored_actions.clear();
		explored_actions_set.clear();
		infos.clear();
		quick_intermediate_steps.clear();

		auto& base_info = infos[nullptr];
	}

	Actions* get_min_cycles_per_tup(const double* threshold = nullptr,
			double* out_cycles = nullptr, bool use_lock = true) const {
		Actions* min_dec = nullptr;
		double min_cyc_tup = 0.0;

		if (threshold) {
			min_cyc_tup = *threshold;
		}

		auto can_execute = [&] (auto actions) {
			return quick_intermediate_steps.find(actions) == quick_intermediate_steps.end();
		};

		if (use_lock) {
			for (auto& keyval : infos) {
				if (!can_execute(keyval.first)) {
					continue;
				}
				std::unique_lock lock(keyval.second.mutex);
				if (!keyval.second.can_use_perf()) {
					continue;
				}

				double cyc_tup = keyval.second.perf.get_cycles_per_tuple();
				if (min_dec && cyc_tup > min_cyc_tup) {
					continue;
				}

				min_dec = keyval.first;
				min_cyc_tup = cyc_tup;
			}
		} else {
			for (auto& keyval : infos) {
				if (!can_execute(keyval.first)) {
					continue;
				}
				if (!keyval.second.can_use_perf()) {
					continue;
				}

				double cyc_tup = keyval.second.perf.get_cycles_per_tuple();
				if (min_dec && cyc_tup > min_cyc_tup) {
					continue;
				}

				min_dec = keyval.first;
				min_cyc_tup = cyc_tup;
			}
		}

		if (out_cycles) {
			*out_cycles = min_cyc_tup;
		}

		return min_dec;
	}

    std::unique_ptr<std::mt19937> gen;

	QueryStageShared(Brain& brain, QueryConfig* config);

	std::shared_ptr<Actions> extend_actions(Actions* previous,
			const std::vector<Decision*>& new_decisions) {
		ASSERT(!new_decisions.empty());

		auto curr = previous;
		std::shared_ptr<Actions> curr_shared;
		for (auto& new_decision : new_decisions) {
			curr_shared = std::make_shared<Actions>(new_decision, curr);
			curr = curr_shared.get();

			auto all_actions_it = all_actions_map.find(curr);
			if (all_actions_it == all_actions_map.end()) {
				all_actions_map.insert({ curr, curr_shared });
				all_actions.push_back(curr_shared);

				auto& info = infos[curr];
				info.status = DecisionInfo::Status::kUnexplored;

				ASSERT(explored_actions_set.find(curr) == explored_actions_set.end());
			} else {
				curr_shared = all_actions_it->second;
				curr = curr_shared.get();
				ASSERT(all_actions_it->first == curr);
			}
		}

		bool exists = explored_actions_set.find(curr) != explored_actions_set.end();
		if (exists) {
			LOG_ERROR("extend_actions: already exists %p (%s)",
				curr, curr ? curr->to_string().c_str() : "NULL");
		}
		ASSERT(!exists);

		explored_actions_set.insert(curr);
		explored_actions.push_back(curr);

		auto& info = infos[curr];
		info.status = DecisionInfo::Status::kPending;

		return curr_shared;
	}
};



} /* engine::adaptive */

struct RandomizedExploration : ExplorationStrategy {
	RandomizedExploration(const std::string& dbg_name, size_t depth_period = 0,
		bool use_distance = false)
	 : ExplorationStrategy((depth_period ? "RandExplDep_" : "RandExpl_") + dbg_name),
		depth_period(depth_period), use_distance(use_distance)
	{
	}

	std::shared_ptr<Actions> explore(bool& done,
		const ExplorationStrategy::ExploreStrategyArgs& args,
		BudgetUser* budget_user) final;
private:
	std::shared_ptr<Actions> explore_depth(QueryStageShared& stage, BudgetUser* budget_user);
	std::shared_ptr<Actions> explore_breadth(QueryStageShared& stage, BudgetUser* budget_user);

	std::shared_ptr<Actions> extend(size_t& num_seen,
		QueryStageShared& stage,
		BudgetUser* budget_user,
		const std::vector<Actions*>& previous_actions,
		int use_distance,
		const char* dbg_path,
		const ExploredActionsSet* distance_measure_actions);

	size_t iteration = 0;
	const size_t depth_period = 0;

	const bool use_distance;

	Actions* inlined_actions = nullptr;
};

std::shared_ptr<Actions>
RandomizedExploration::explore(bool& done,
	const ExplorationStrategy::ExploreStrategyArgs& args,
	BudgetUser* budget_user)
{
	auto query_stage_shared = args.query_stage;
	ASSERT(query_stage_shared);

	done = false;
	std::shared_ptr<Actions> result;

	if (!inlined_actions) {
		std::vector<Actions> tmp_actions;

		auto inline_op = query_stage_shared->decision_factory.new_inline();
		Actions* found_actions = nullptr;

		std::vector<Decision*> decisions {inline_op};

		if (can_extend_actions(nullptr, query_stage_shared->explored_actions_set,
				decisions, tmp_actions, &found_actions)) {
			result = query_stage_shared->extend_actions(nullptr, decisions);

			if (result) {
				inlined_actions = result.get();
				// stage.quick_intermediate_steps.insert(inlined_actions);
				return result;
			}
		} else {
			ASSERT(found_actions);
			inlined_actions = found_actions;
		}
	}

	if (depth_period) {
		bool depth = !(++iteration % depth_period);

		if (depth) {
			result = explore_depth(*query_stage_shared, budget_user);
		}
		if (!result || !depth) {
			result = explore_breadth(*query_stage_shared, budget_user);
			if (!result) {
				done = true;
			}
		}
	} else {
		result = explore_breadth(*query_stage_shared, budget_user);
		if (!result) {
			done = true;
		}
	}
	return result;
}

std::shared_ptr<Actions>
RandomizedExploration::explore_depth(QueryStageShared& stage, BudgetUser* budget_user)
{
	double global_min_cyc_tup = 0.0;
	auto min_actions = stage.get_min_cycles_per_tup(nullptr, &global_min_cyc_tup,
		false);

	std::vector<Actions*> region;
	region.reserve(128);

	for (auto& keyval : stage.infos) {
		std::unique_lock lock(keyval.second.mutex);
		auto dec = keyval.first;
		if (!keyval.second.can_use_perf()) {
			continue;
		}
		auto cyc_tup = keyval.second.perf.get_cycles_per_tuple();
		if (dec && global_min_cyc_tup * (1.0 + kImproveMargin) > cyc_tup) {
			// also interesting
			region.push_back(dec);
		}
	}

	LOG_DEBUG("BrainSession('%s'): explore_depth: found %d interesting Actions",
		dbg_name.c_str(), (int)region.size());

	region.push_back(nullptr);

	ExploredActionsSet measure_set;
	if (use_distance) {
		for (auto& actions : region) {
			// insert action
			measure_set.insert(actions);
			if (!actions) {
				continue;
			}

			// insert prefix
			Actions* curr = actions;
			while (curr) {
				measure_set.insert(curr);
				curr = curr->get_previous();
			}
		}
	}

	size_t num_seen = 0;
	auto result = extend(num_seen, stage, budget_user, region,
		use_distance ? -1 : 0,
		"explore_depth",
		use_distance ? &measure_set : &stage.explored_actions_set);
	if (result) {
		LOG_DEBUG("BrainSession('%s'): explore_depth: num_seen %llu, produced %p ('%s')",
			dbg_name.c_str(), num_seen,
			result.get(), result ? result->to_string().c_str() : "?");
	}

	return result;
}

struct DistanceToAll {
	template<typename ActionSet>
	double operator()(Actions* previous,
			const std::vector<Decision*>& new_decisions,
			const ActionSet& distance_measure_actions,
			bool use_max) {
		bool first = true;
		double r = 0.0;

		for (auto& dec : new_decisions) {
			ASSERT(dec);
		}

		for (auto& keyval : distance_measure_actions) {
			const auto& old_actions = keyval;

			with_temp_actions(tmp_actions, previous, new_decisions,
				[&] (Actions* new_actions) {
					size_t count = 0;
					double dist = .0;

					if (!old_actions && !new_actions) {
						dist = .0;
					} else {
						auto a = new_actions ? new_actions : old_actions;

						double sum = new_actions->gowers_distance(
							tmp_actions1, tmp_actions2,
							count, old_actions);
						dist = sum / (double)count;
					}

					if (use_max) {
						if (first || dist < r) {
							r = dist;
							first = false;
						}
					} else {
						r += dist*dist;
					}
				});
		}

		return r;
	}
private:
	std::vector<const Decision*> tmp_actions1;
	std::vector<const Decision*> tmp_actions2;
	std::vector<Actions> tmp_actions;
};

template<typename T>
bool
open_search_space(Actions* previous, QueryStageShared& stage,
	BudgetUser* budget_user, const char* dbg_path, size_t& num_seen,
	const T& fun, ExploredActionsSet* explored_actions_set_ptr = nullptr,
	size_t max = 0)
{
	auto info_it = stage.infos.find(previous);
	ASSERT(info_it != stage.infos.end());

	auto& info = info_it->second;
	RuleTrigger::Input input(stage.brain, previous,
		info.example_plan, info.example_voila_context,
		&stage.plan_profile);

	RuleIterator iterator(input, stage.decision_factory,
		explored_actions_set_ptr ?
			*explored_actions_set_ptr : stage.explored_actions_set);
	auto& top_iterator = iterator.get();

	size_t num = 0;

	while (top_iterator.next()) {
		if (budget_user && (num_seen % 100 == 0) &&
				!budget_user->has_sufficient(dbg_path)) {
			return false;
		}
		num_seen++;
		auto& data = top_iterator.get_data();
		ASSERT(!data.generated_choices.empty());

		fun(data);

		num++;
		if (max && num >= max) {
			return true;
		}
	};
	return true;
}

std::shared_ptr<Actions>
RandomizedExploration::extend(size_t& num_seen,
	QueryStageShared& stage,
	BudgetUser* budget_user,
	const std::vector<Actions*>& input_previous_actions,
	int use_gowers_distance,
	const char* dbg_path,
	const ExploredActionsSet* input_distance_measure_actions)
{
	std::shared_ptr<Actions> result;

	const std::vector<Actions*>* ptr_previous_actions = &input_previous_actions;
	std::vector<Actions*> sampled_previous_actions;

	const size_t new_size = 512;

	if (ptr_previous_actions->size() > 1024) {
		LOG_DEBUG("extend(%s): Lot's of choices to extend (num %d)",
			dbg_path, (int)ptr_previous_actions->size());


		sampled_previous_actions.reserve(new_size);

		std::sample(ptr_previous_actions->begin(), ptr_previous_actions->end(),
			std::back_inserter(sampled_previous_actions), new_size, *stage.gen);

		ptr_previous_actions = &sampled_previous_actions;
	}

	const auto& previous_actions = *ptr_previous_actions;

	const ExploredActionsSet* distance_measure_actions = input_distance_measure_actions;
	ExploredActionsSet sampled_distance_measure_actions;
	if (use_distance && input_distance_measure_actions &&
			input_distance_measure_actions->size() > 1024) {
		std::vector<Actions*> data(input_distance_measure_actions->begin(),
			input_distance_measure_actions->end());

		std::vector<Actions*> sample;
		sample.reserve(new_size);

		if (budget_user && !budget_user->has_sufficient(dbg_path)) {
			return nullptr;
		}

		std::sample(data.begin(), data.end(),
			std::back_inserter(sample), new_size, *stage.gen);

		for (auto& a : sample) {
			sampled_distance_measure_actions.insert(a);
		}

		distance_measure_actions = &sampled_distance_measure_actions;
	}

	DistanceToAll distance_to_all;

	LOG_DEBUG("BrainSession('%s'): %s: extend: #previous_actions %d",
		dbg_name.c_str(), dbg_path, (int)previous_actions.size());

	struct Entry {
		Actions* prev;
		std::vector<Decision*> new_decisions;
		double dist;
	};
	std::vector<Entry> array;
	array.reserve(1024);

	if (use_gowers_distance) {
		bool first = true;
		double max_dist = 0.0;
		bool use_max = use_gowers_distance > 0;
		double min_dist = 0.0;

		ASSERT(distance_measure_actions);

		for (Actions* previous : previous_actions) {
			bool cont = open_search_space(previous, stage, budget_user,
				dbg_path, num_seen,
				[&] (auto& data) {
					double dist = distance_to_all(previous, data.generated_choices,
						*distance_measure_actions, use_max);

					if (use_max) {
						if (dist >= max_dist || first) {
							array.emplace_back(Entry { previous,
								std::move(data.generated_choices),
								dist });
							max_dist = dist;
							first = false;
						}
					} else {
						if (dist <= min_dist || first) {
							array.emplace_back(Entry { previous,
								std::move(data.generated_choices),
								dist });
							min_dist = dist;
							first = false;
						}
					}
				}
			);

			if (!cont) {
				return nullptr;
			}
		}

		std::sort(array.begin(), array.end(),
			[] (const auto& a, const auto& b) {
				return a.dist < b.dist;
			});

		LOG_TRACE("BrainSession('%s'): %s: extend: list:",
			dbg_name.c_str(), dbg_path);
		std::vector<Entry> equi_dist_sample;
		ReservoirSampleBuilder<Entry> equi_dist_reservoir(1,
			*stage.gen, equi_dist_sample);
		equi_dist_sample.reserve(1);

		for (auto& entry : array) {
			if (entry.dist < max_dist) {
				continue;
			}

			equi_dist_reservoir.push(entry);
			LOG_TRACE("BrainSession('%s'): %s: max_dist: %f",
				dbg_name.c_str(), dbg_path, entry.dist);
		}

		ASSERT(array.empty() == equi_dist_sample.empty());
		array = std::move(equi_dist_sample);
	} else {
		ReservoirSampleBuilder<Entry> reservoir(1, *stage.gen, array);

		auto try_extend = [&] (auto& previous_actions, size_t& seen) {
			for (Actions* previous : previous_actions) {
				bool cont = open_search_space(previous, stage, budget_user,
					dbg_path, seen,
					[&] (auto& data) {
						decltype(reservoir)::LazyAppend append(reservoir);
						append.emplace(Entry { previous,
							std::move(data.generated_choices), 0.0 });
					}
				);

				if (!cont) {
					return false;
				}
			}
			return true;
		};

		if (false) {
			// partition into length classes
			std::unordered_map<size_t, std::vector<Actions*>> length_class;
			size_t max_length = 0;

			// partition
			for (auto& actions : previous_actions) {
				size_t len = actions ? actions->size() : 0;

				max_length = std::max(max_length, len);
				length_class[len].push_back(actions);
			}

			// try to exetdn by length
			for (size_t i=0; i<max_length; i++) {
				size_t seen = 0;
				if (!try_extend(length_class[i], seen)) {
					num_seen += seen;
					return nullptr;
				}
				num_seen += seen;

				if (seen > 0) {
					// we sampled at least one new Decision
					break;
				}
			}
		} else {
			if (!try_extend(previous_actions, num_seen)) {
				return nullptr;
			}
		}

	}

	if (!array.empty()) {
		auto& entry = array[0];
		ASSERT(!entry.new_decisions.empty());

		auto previous = entry.prev;
#if 0
		auto& info = stage.infos[previous];
		engine::lolepop::Lolepop::print_plan(info.example_plan,
			"RandomizedExploration::explore_breadth");
#endif

		result = stage.extend_actions(previous,
			std::move(entry.new_decisions));
	}

	return result;
}

std::shared_ptr<Actions>
RandomizedExploration::explore_breadth(QueryStageShared& stage,
	BudgetUser* budget_user)
{
	uint64_t num_explored_actions = stage.explored_actions.size();
	size_t num_seen = 0;

	std::vector<Actions*> all(stage.explored_actions);
	all.push_back(nullptr);

	auto result = extend(num_seen, stage, budget_user, all,
		use_distance ? 1 : 0,
		"explore_breadth",
		&stage.explored_actions_set);

	LOG_DEBUG("BrainSession('%s'): explore_breadth: extend to '%s' "
		"(#explored_all %llu, #seen %llu)",
		dbg_name.c_str(), result ? result->to_string().c_str() : "?",
		num_explored_actions, num_seen);
	return result;
}


struct MctsStoredTree {
	struct Node {
		size_t visits = 0;
		double reward = .0;
		size_t failed = 0;

		std::shared_ptr<engine::lolepop::Lolepop> example_plan;
		std::shared_ptr<engine::voila::Context> example_voila_context;

		std::vector<std::unique_ptr<Node>> children;
		std::vector<DecisionState> decision_states;
	};

	std::unique_ptr<Node> root;
};


struct GlobalDecisionInfo {

	struct ChainKey {
		std::vector<DecisionState> decisions;

		bool operator==(const ChainKey& o) const {
			return decisions == o.decisions;
		}
	};

	struct ChainKeyHasher {
		size_t operator()(const ChainKey& k) const {
			size_t seed = 13;

			auto combine = [&] (size_t val) {
				seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
			};

			for (auto& d : k.decisions) {
				combine(std::hash<DecisionState>()(d));
			}
			return seed;
		}
	};

	struct GlobalDecInfo {
		double reward = .0;
		size_t visits = 0;
	};

	std::unordered_map<ChainKey, GlobalDecInfo, ChainKeyHasher> map;
	size_t num_runs = 0;
};



struct MonteCarloTreeSearch : ExplorationStrategy {
	bool cross_propagation = true;

	MonteCarloTreeSearch(const std::string& dbg_name)
	 : ExplorationStrategy("Mcts_" + dbg_name)
	{

	}

	GlobalDecisionInfo global_decision_info;

	void add_global_dec_info(const std::vector<DecisionState>& states,
			double reward, size_t visits) {
		if (!cross_propagation) {
			return;
		}
		auto& r = global_decision_info.map[GlobalDecisionInfo::ChainKey {
			states}];
		r.reward += reward;
		r.visits += visits;

		global_decision_info.num_runs++;
	}

	bool get_global_dec_info(const std::vector<Decision*>& decs,
			double& reward, size_t& visits, size_t& total_runs) const {
		if (cross_propagation || global_decision_info.map.empty()) {
			return false;
		}
		std::vector<DecisionState> states;
		states.reserve(decs.size());
		for (auto& dec : decs) {
			states.push_back(dec->get_decision_state());
		}

		auto it = global_decision_info.map.find(GlobalDecisionInfo::ChainKey {
			std::move(states)});
		if (it == global_decision_info.map.end()) {
			return false;
		}

		reward = it->second.reward;
		visits = it->second.visits;
		total_runs = global_decision_info.num_runs;

		return true;
	}

	struct TreeNode {
		size_t visits = 0;
		double reward = .0;
		size_t failed = 0;
		std::vector<Decision*> decisions;
		Actions* actions = nullptr;

		TreeNode* parent = nullptr;
		std::vector<std::unique_ptr<TreeNode>> children;

		TreeNode(const std::vector<Decision*>& decisions)
		 : decisions(decisions)
		{

		}

		double uct1(double C = 2.0) const {
			if (!visits) {
				static_assert(std::numeric_limits<double>::is_iec559, "infty == infty");
				return std::numeric_limits<double>::infinity();
			}

			double a = reward / (double)visits;
			double b = log((double)parent->visits) / (double)visits;

			return a + C*sqrt(b);
		}

		TreeNode* add_child(std::unique_ptr<TreeNode>&& child) {
			TreeNode* child_ptr = child.get();
			child->parent = this;
			children.emplace_back(std::move(child));
			return child_ptr;
		}
	};

	TreeNode* mcts_selection(TreeNode* node, QueryStageShared& stage) {
		double max_bound = 0.0;
		TreeNode* max_node = nullptr;

		if (node->children.empty()) {
			// leaf node -> select
			return node;
		}

		auto can_extend = [&] (const auto& curr) {
			return can_extend_actions(curr->parent->actions,
				stage.explored_actions_set,
				curr->decisions, tmp_actions);
		};

		struct GoodNode {
			double bound;
			TreeNode* node;

			mutable double tie_breaker_bound = 0.;
			mutable bool has_tie_breaker_bound = false;

			mutable double max_distance = 0.;
			mutable bool has_max_distance = false;

			double get_tie_breaker_bound(MonteCarloTreeSearch& tree) const {
				if (!has_tie_breaker_bound) {
					double g_reward = 0;
					size_t g_visits = 0;
					size_t g_total_runs = 0;
					ASSERT(node);
					tree.get_global_dec_info(node->decisions, g_reward, g_visits,
						g_total_runs);

					has_tie_breaker_bound = true;

					if (g_visits) {
						double n = g_visits;
						double delta = sqrt(2.0/3.0 * log((double)g_total_runs + 1.0) / n);

						tie_breaker_bound = g_reward/n + delta;
					} else {
						tie_breaker_bound = std::numeric_limits<double>::infinity();
					}

				}

				return tie_breaker_bound;
			}

			double get_max_distance(const TreeNode& parent) const {
				if (!has_max_distance) {
					ASSERT(node);
					const auto& left_tmp = node->decisions;

					for (auto& other_node : parent.children) {
						double dist = 0;
						size_t count = 0;

						ASSERT(other_node);

						const auto& right_tmp = other_node->decisions;
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
							double s = left_tmp[left_idx]->gowers_distance(c,
									right_tmp[right_idx]);
							ASSERT(s / (double)c <= 1.0);

							count += c;
							sum += s / discount_factor;

							discount_factor /= discount_rate;
						}

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

						dist = sum / (double)count;

						if (!has_max_distance) {
							has_max_distance = true;
							max_distance = dist;
						} else {
							max_distance = std::min(max_distance, dist);
						}
					}
				}

				return max_distance;
			}

			GoodNode(double bound, TreeNode* node) : bound(bound), node(node) {}
		};
		std::vector<GoodNode> good_nodes;
		good_nodes.reserve(node->children.size());

		bool shuffle = false;

		// find good next node
		for (auto& child : node->children) {
			double bound = child->uct1();

			LOG_TRACE("child %p, uct1 %f, type %lld",
				child.get(), bound, child->decisions[0]->get_decision_state().node_type);
			if (max_node) {
				if (std::isfinite(bound) && bound <= max_bound) {
					continue;
				}
			}

			// would run this node
			if (/*child->children.empty() && */!can_extend(child.get())) {
				continue;
			}

			max_bound = bound;
			max_node = child.get();

			good_nodes.emplace_back(GoodNode(bound, max_node));
		}

		if (shuffle) {
			std::shuffle(good_nodes.begin(), good_nodes.end(), *stage.gen);
		}


		for (bool failed : {false, true}) {
			std::vector<GoodNode> good_nodes2;
			good_nodes2.reserve(good_nodes.size());

			for (auto&& pair : good_nodes) {
				LOG_TRACE("filter good: bound %f < max_bound %f, node %p, failed %d",
					pair.bound, max_bound, pair.node, pair.node->failed);

				if (std::isfinite(max_bound) || std::isfinite(pair.bound)) {
					if (pair.bound < max_bound) {
						continue;
					}
				}


				if ((!pair.node->failed) != !failed) {
					continue;
				}

				good_nodes2.emplace_back(std::move(pair));
			}

			auto tie_break_by_type = [&] (auto& a, auto& b) {
				ASSERT(a.node && b.node);
#if 0
				// break tie

				auto& d = a.node->decisions;
				auto& e = b.node->decisions;
				ASSERT(d.size() > 0 && e.size() > 0);

				size_t i=0;
				for (; i<std::min(d.size(), e.size()); i++) {
					auto& s = d[i]->get_decision_state().node_type;
					auto& t = e[i]->get_decision_state().node_type;
					if (s != t) {
						return s > t;
					}
				}

				return d.size() > e.size();
#else
				return a.get_max_distance(*node) > b.get_max_distance(*node);
#endif
			};

			if (cross_propagation) {
				std::sort(good_nodes2.begin(), good_nodes2.end(),
					[&] (auto& a, auto& b) {
						double x = a.get_tie_breaker_bound(*this);
						double y = b.get_tie_breaker_bound(*this);

						bool x_finite = std::isfinite(x);
						bool y_finite = std::isfinite(y);

						if (x_finite && y_finite) {
							if (x != y) {
								return x > y;
							}
						} if (x_finite != y_finite) {
							return x > y;
						}

						return tie_break_by_type(a, b);
					}
				);
			} else {
				std::sort(good_nodes2.begin(), good_nodes2.end(),
					[&] (auto& a, auto& b) {
						return tie_break_by_type(a, b);
					}
				);
			}

			for (auto& n : good_nodes2) {
				LOG_TRACE("child %p, uct1 %f, tieb %f, type %lld",
					n.node, n.bound, n.get_tie_breaker_bound(*this),
					n.node->decisions[0]->get_decision_state().node_type);
			}

			for (auto& n : good_nodes2) {
				auto r = mcts_selection(n.node, stage);
				if (r) {
					LOG_TRACE("select = %p", r);
					return r;
				}
			}
		}


		return nullptr;
	}


	std::unique_ptr<TreeNode> root;
	TreeNode* last_node = nullptr;

	std::vector<Actions> tmp_actions;

	struct DuplElemHash {
		size_t operator()(const std::vector<Decision*>& e) const {
			size_t seed = 13;

			auto combine = [&] (size_t val) {
				seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
			};

			for (auto& dec : e) {
				combine((size_t)dec);
			}
			return seed;
		}
	};

	std::vector<std::pair<size_t, std::vector<Decision*>>> expand_node_tmp;

	bool expand_node(TreeNode* node, QueryStageShared& stage,
			BudgetUser* budget_user, const char* dbg_path,
			bool shuffle) {
		size_t num_seen = 0;

		LOG_TRACE("expand_node %p (%s)", node, dbg_path);

		std::unordered_set<std::vector<Decision*>, DuplElemHash> dupl_elem;
		expand_node_tmp.clear();

		ExploredActionsSet empty_set;

		bool cont = open_search_space(node->actions, stage, budget_user,
			dbg_path, num_seen,
			[&] (auto& data) {
				const auto& choices = data.generated_choices;
				ASSERT(!choices.empty());

				LOG_TRACE("expand_node: canidate");

				auto it = dupl_elem.find(choices);
				if (it == dupl_elem.end()) {
					dupl_elem.insert(choices);

					if (shuffle) {
						expand_node_tmp.push_back({0, choices});
					} else {
						LOG_TRACE("expand_node: add_child");
						node->add_child(std::make_unique<TreeNode>(choices));
					}
				}
			},
			&empty_set, 40
		);
		if (!cont) {
			return cont;
		}

		std::shuffle(expand_node_tmp.begin(), expand_node_tmp.end(), *stage.gen);

		for (auto& pair : expand_node_tmp) {
			LOG_TRACE("expand: %d", pair.first);
			node->add_child(std::make_unique<TreeNode>(
				std::move(pair.second)));
		}

		return cont;
	}

	Actions* inlined_actions = nullptr;

	void store_io_global_state(QueryStageShared& stage) final;
	void load_from_global_state(QueryStageShared& stage) final;

	void update_upwards(TreeNode* curr, double reward) {
		// propagate back up the tree
		while (curr) {
			LOG_TRACE("update %p with reward=%f", curr, reward);
			curr->visits++;
			curr->reward += reward;
			curr->failed = 0;

			curr = curr->parent;
		}
	}

	std::shared_ptr<Actions>
	explore(bool& done,
			const ExplorationStrategy::ExploreStrategyArgs& args,
			BudgetUser* budget_user) final {
		ASSERT(args.query_stage);
		auto& stage = *args.query_stage;

		std::shared_ptr<Actions> result;

		if (!inlined_actions) {
			std::vector<Actions> tmp_actions;

			auto inline_op = stage.decision_factory.new_inline();
			Actions* found_actions = nullptr;

			std::vector<Decision*> decisions {inline_op};

			if (can_extend_actions(nullptr, stage.explored_actions_set,
					decisions, tmp_actions, &found_actions)) {
				result = stage.extend_actions(nullptr, decisions);

				if (result) {
					inlined_actions = result.get();
					// stage.quick_intermediate_steps.insert(inlined_actions);
					return result;
				}
			} else {
				ASSERT(found_actions);
				inlined_actions = found_actions;
			}
		}

		if (!root) {
			ASSERT(!last_node);
			ASSERT(inlined_actions);

			root = std::make_unique<TreeNode>(
				std::vector<Decision*> { stage.decision_factory.new_inline() });
			root->actions = inlined_actions;

			// add initial children
			if (!expand_node(root.get(), stage, budget_user, "create_root", false)) {
				return nullptr;
			}

			ASSERT(!root->children.empty());
		}

		if (last_node) {
			auto& base_info = stage.infos[nullptr];

			ASSERT(base_info.can_use_perf());
			double base_cyc_tup = base_info.perf.get_cycles_per_tuple();

			auto& curr_info = stage.infos[last_node->actions];
			bool has_info = curr_info.can_use_perf();

			double reward;
			if (has_info) {

				// expand node
				if (!expand_node(last_node, stage, budget_user, "expand_last", true)) {
					return nullptr;
				}

				// back propagate rewards
				// better solutions have less cycles/tuple
				double curr_cyc_tup = curr_info.perf.get_cycles_per_tuple();
				reward = (base_cyc_tup - curr_cyc_tup) / base_cyc_tup * 10.0;
				LOG_TRACE("update: has base_cyc %f vs. curr %f",
					base_cyc_tup, curr_cyc_tup);

				update_upwards(last_node, reward);
			} else {
				reward = .0;
				LOG_TRACE("No performance info");

				last_node->failed++;
			}
		}


		// selection:
		TreeNode* new_node = mcts_selection(root.get(), stage);

		// sample new node
		last_node = nullptr;
		if (new_node) {
			ASSERT(new_node->parent);
			auto previous = new_node->parent->actions;

			Actions* found_action = nullptr;
			if (can_extend_actions(previous, stage.explored_actions_set,
					new_node->decisions, tmp_actions, &found_action)) {
				result = stage.extend_actions(previous, new_node->decisions);
				last_node = new_node;
				last_node->actions = result.get();
				ASSERT(last_node->actions);
			} else {
				ASSERT(false);
			}
		}

		return result;
	}

};

struct HeuristicExploration : ExplorationStrategy {
	HeuristicExploration(const std::string& dbg_name)
	 : ExplorationStrategy("Heur_" + dbg_name)
	{
		clear();
	}

	std::shared_ptr<Actions>
	explore(bool& done,
		const ExplorationStrategy::ExploreStrategyArgs& args,
		BudgetUser* budget_user) final;

	void clear() {
		inlined_actions = 0;
		strat1 = kDataCentric;

		jit_fragments.clear();
		jit_fragments2.clear();

		data_centric.clear();
		base_guesses.clear();

		inlined_actions = nullptr;
		predicated = 0;
	}

	void discard() final {
		clear();
	}

	enum Strategy1 {
		kDataCentric,

		kJitFragments,

		kFlavorized,

		kNUM_Strategy1,
	};

	Strategy1 strat1 = kDataCentric;

	struct JitFragments {
		Actions* last_actions = nullptr;
		int64_t max_num_fragments = -1;

		void clear() {
			last_actions = nullptr;
			max_num_fragments = -1;
		}
	};

	JitFragments jit_fragments;
	JitFragments jit_fragments2;

	std::shared_ptr<Actions> explore_jit_fragment(QueryStageShared& stage,
		JitFragments& jit_fragments, const FlavorSpec& flavor,
		int profile_guided, bool up);
	std::shared_ptr<Actions> explore_base(QueryStageShared& stage);
	std::shared_ptr<Actions> explore_flavorized(QueryStageShared& stage);

	struct DataCentric {
		bool first_run = true;

		size_t trigger_index = 0;

		std::vector<std::unique_ptr<engine::adaptive::RuleTrigger>> triggers;

		void clear() {
			first_run = true;
			trigger_index = 0;
			triggers.clear();
		}
	};
	DataCentric data_centric;
	DataCentric base_guesses;

	static std::shared_ptr<Actions>
	explore_helper(DataCentric& triggers, QueryStageShared& stage, Actions* previous)
	{
		ASSERT(!triggers.first_run && "Must be initialized");

		auto info_it = stage.infos.find(previous);
		ASSERT(info_it != stage.infos.end());

		auto& info = info_it->second;

		std::vector<Actions> tmp_actions;

		RuleTrigger::Input input(stage.brain, previous,
			info.example_plan, info.example_voila_context,
			&stage.plan_profile);

		while (triggers.trigger_index < triggers.triggers.size()) {
			auto& trigger = *triggers.triggers[triggers.trigger_index].get();

			std::unique_ptr<RuleTrigger::Context> context;

			if (trigger.can_trigger(input, context)) {
				std::vector<Decision*> new_decs(trigger.generate(input,
					stage.decision_factory, context));

				if (!new_decs.empty()) {
					triggers.trigger_index++;
					if (can_extend_actions(previous, stage.explored_actions_set,
								new_decs, tmp_actions)) {
						return stage.extend_actions(previous, std::move(new_decs));
					}
				}
			}
			triggers.trigger_index++;
		};

		return nullptr;
	}

	Actions* inlined_actions = nullptr;
	int predicated = 0;

	std::vector<Actions> tmp_actions;
};

static std::vector<Decision*>
_explore_jit_fragment_down(QueryStageShared& stage, int64_t limit,
	int64_t& out_size, Actions* inlined_actions,
	const FlavorSpec& flavor, int profile_guided)
{
	using FindJittableFragment = engine::adaptive::triggers::FindJittableFragment;

	ASSERT(inlined_actions);
	Actions* previous = inlined_actions;
	auto info_it = stage.infos.find(previous);
	ASSERT(info_it != stage.infos.end());

	auto& info = info_it->second;

	FindJittableFragment find_jittable(flavor, profile_guided);

	std::unique_ptr<RuleTrigger::Context> context;
	std::vector<Decision*> decisions;
	decisions.reserve(16);
	out_size = 0;

	// engine::lolepop::Lolepop::print_plan(info.example_plan, "explore_jit_fragment");

	std::vector<std::unique_ptr<Actions>> actions;
	actions.reserve(16);

	auto get_last = [&] (auto& actions) {
		return actions.empty() ? nullptr : actions[actions.size()-1].get();
	};

	while (1) {
		if (limit > 0 && out_size >= limit) {
			break;
		}

		RuleTrigger::Input input(stage.brain, get_last(actions),
			info.example_plan, info.example_voila_context,
			&stage.plan_profile);

		if (!find_jittable.can_trigger(input, context)) {
			break;
		}

		std::vector<Decision*> new_decs(find_jittable.generate(input,
			stage.decision_factory, context));

		if (new_decs.empty()) {
			break;
		}

		context.reset();
		for (auto& dec : new_decs) {
			decisions.push_back(dec);

			actions.push_back(std::make_unique<Actions>(dec, get_last(actions)));
		}
		out_size++;
	};

	ASSERT(limit < 0 || out_size <= limit);

	return decisions;
}


static std::vector<Decision*>
_explore_jit_fragment_up(QueryStageShared& stage, Actions* previous_actions,
	const FlavorSpec& flavor, int profile_guided)
{
	using FindJittableFragment = engine::adaptive::triggers::FindJittableFragment;

	Actions* previous = previous_actions;
	auto info_it = stage.infos.find(previous);
	ASSERT(info_it != stage.infos.end());

	auto& info = info_it->second;

	FindJittableFragment find_jittable(flavor, profile_guided);

	std::unique_ptr<RuleTrigger::Context> context;
	// engine::lolepop::Lolepop::print_plan(info.example_plan, "explore_jit_fragment");

	RuleTrigger::Input input(stage.brain, previous_actions,
		info.example_plan, info.example_voila_context,
		&stage.plan_profile);

	if (!find_jittable.can_trigger(input, context)) {
		return {};
	}

	return find_jittable.generate(input, stage.decision_factory, context);
}

std::shared_ptr<Actions>
HeuristicExploration::explore_jit_fragment(QueryStageShared& stage,
	JitFragments& jit_fragments, const FlavorSpec& flavor, int profile_guided,
	bool up)
{
	std::shared_ptr<Actions> result;
	auto& max_num = jit_fragments.max_num_fragments;

	if (up) {
		if (max_num < 0) {
			max_num = 1;
			jit_fragments.last_actions = inlined_actions;
		}

		auto previous = jit_fragments.last_actions;

		std::vector<Decision*> new_decisions(_explore_jit_fragment_up(stage,
			previous, flavor, profile_guided));

		if (new_decisions.empty()) {
			return nullptr;
		}

		Actions* equivalent_action = nullptr;
		if (!can_extend_actions(previous, stage.explored_actions_set,
				new_decisions, tmp_actions, &equivalent_action)) {
			ASSERT(equivalent_action);
			jit_fragments.last_actions = equivalent_action;
			return nullptr;
		}

		result = stage.extend_actions(previous, std::move(new_decisions));
		if (result) {
			jit_fragments.last_actions = result.get();
		}
	} else {
		ASSERT(false && "TODO: can_extend_actions && extend_actions");
#if 0
		if (max_num < 0) {
			// generate all possible JIT fragments
			result = _explore_jit_fragment_down(stage, -1, max_num, inlined_actions,
				flavor, profile_guided);
			ASSERT(max_num >= 0);
			max_num--;
		} else if (max_num > 0) {
			// return nullptr; // TODO

			// generate the first n-1
			int64_t num_found = 0;
			result = _explore_jit_fragment_down(stage, max_num, num_found, inlined_actions,
				flavor, profile_guided);

			ASSERT(num_found <= max_num);
			max_num = std::min<int64_t>(max_num-1, num_found);
		} else {
			// nothing
		}
		max_num = std::max<int64_t>(max_num, 0);
#endif
	}
	return result;
}

std::shared_ptr<Actions>
HeuristicExploration::explore_base(QueryStageShared& stage)
{
	if (base_guesses.first_run) {
		base_guesses.triggers.emplace_back(
			std::make_unique<triggers::PushDownMostSelectiveFilter>());
		base_guesses.triggers.emplace_back(
			std::make_unique<triggers::EnableBloomFilterForHighestSelJoin>(2));
		base_guesses.triggers.emplace_back(
			std::make_unique<triggers::EnableBloomFilterForHighestSelJoin>(1));
		base_guesses.first_run = false;
	}

	return explore_helper(base_guesses, stage, nullptr);
}

std::shared_ptr<Actions>
HeuristicExploration::explore_flavorized(QueryStageShared& stage)
{
	if (data_centric.first_run) {
		std::vector<std::unique_ptr<engine::adaptive::RuleTrigger>> rules0;
		std::vector<std::unique_ptr<engine::adaptive::RuleTrigger>> rules1;

		for_each_flavor([&] (const auto& spec) {
#if 0
			data_centric.triggers.emplace_back(
				std::make_unique<triggers::SetDefaultFlavor>(spec));
#endif
			rules0.emplace_back(
				std::make_unique<triggers::JitExpressionsStrategy>(spec));
		});

#if 1
		SetVectorKey::for_each_vector_size([&] (const auto& val) {
			if (val.vector_size == 1024 && val.full_eval &&
					val.bit_score_divisor == 24) {
				return;
			}

			rules1.emplace_back(
				std::make_unique<triggers::SetVectorSize>(val));
		});
#endif

		// interleave both lists
		size_t i0=0;
		size_t i1=0;
		auto& triggers = data_centric.triggers;

		for (; i0<rules0.size() && i1<rules1.size(); i0++, i1++) {
			triggers.emplace_back(std::move(rules0[i0]));
			triggers.emplace_back(std::move(rules1[i1]));
		}

		for (; i0<rules0.size(); i0++) {
			triggers.emplace_back(std::move(rules0[i0]));
		}

		for (; i1<rules1.size(); i1++) {
			triggers.emplace_back(std::move(rules1[i1]));
		}

		data_centric.first_run = false;
	}

	return explore_helper(data_centric, stage, nullptr);
}

std::shared_ptr<Actions>
HeuristicExploration::explore(bool& done,
	const ExplorationStrategy::ExploreStrategyArgs& args, BudgetUser* budget_user)
{
	std::shared_ptr<Actions> result;
	ASSERT(args.query_stage);
	auto& stage = *args.query_stage;

	done = false;

	result = explore_base(stage);
	if (result) {
		return result;
	}

	// make sure we have an Inline plan
	if (!inlined_actions) {
		std::vector<Actions> tmp_actions;

		auto inline_op = stage.decision_factory.new_inline();
		Actions* found_actions = nullptr;

		std::vector<Decision*> decisions {inline_op};

		if (can_extend_actions(nullptr, stage.explored_actions_set,
				decisions, tmp_actions, &found_actions)) {
			result = stage.extend_actions(nullptr, decisions);

			if (result) {
				inlined_actions = result.get();
				// stage.quick_intermediate_steps.insert(inlined_actions);
				return result;
			}
		} else {
			ASSERT(found_actions);
			inlined_actions = found_actions;
		}
	}

	ASSERT(inlined_actions);

	size_t iterations = 0;
	while (1) {
		size_t num_visited = 0;

		if (strat1 == Strategy1::kDataCentric) {
			strat1 = Strategy1::kJitFragments;
#if 1
			int profile_guided = 0; // data-centric
			result = explore_jit_fragment(stage, jit_fragments2,
				FlavorSpec::get_data_centric_flavor(predicated), profile_guided,
				true);
			if (result) {
				break;
			}
#endif
			num_visited++;
		}

		if (strat1 == Strategy1::kJitFragments) {
			strat1 = Strategy1::kFlavorized;
#if 1
			int profile_guided = 1; // fully profile guided

			result = explore_jit_fragment(stage, jit_fragments,
				FlavorSpec::get_data_centric_flavor(predicated), profile_guided,
				true);
			if (result) {
				break;
			}
#endif
			num_visited++;
		}

		if (strat1 == Strategy1::kFlavorized) {
			strat1 = Strategy1::kDataCentric;
#if 1
			result = explore_flavorized(stage);
			if (result) {
				break;
			} else {
				// ASSERT(false);
			}
#endif
			num_visited++;
		}

		if (num_visited >= Strategy1::kNUM_Strategy1) {
			ASSERT(num_visited == Strategy1::kNUM_Strategy1);

			if (predicated > 2) {
				done = true;
				break;
			} else {
				predicated++;
			}
		}

		iterations++;
		ASSERT(iterations < 1024);
	}


	// ASSERT(false && "unhandled case");
	return result;
}




QueryStageShared::QueryStageShared(Brain& brain, QueryConfig* config)
 : brain(brain)
{
	if (config->adaptive_random_noseed()) {
		gen = std::make_unique<std::mt19937>(std::random_device{}());
	} else {
		std::seed_seq seed { config->adaptive_random_seed() };
		gen = std::make_unique<std::mt19937>(seed);
	}

	ASSERT(gen);

	auto& info = infos[nullptr];
	info.status = DecisionInfo::Status::kPending;

	using ExplorationStrategy = QueryConfig::ExplorationStrategy;

	switch (config->adaptive_exploration_strategy()) {
	case ExplorationStrategy::kExploreRandom:
		exploration_strategy = std::make_unique<RandomizedExploration>("");
		break;
	case ExplorationStrategy::kExploreRandomDepth2:
		exploration_strategy = std::make_unique<RandomizedExploration>("", 2, false);
		break;
	case ExplorationStrategy::kExploreRandomMaxDist:
		exploration_strategy = std::make_unique<RandomizedExploration>("", 2, true);
		break;
	case ExplorationStrategy::kExploreHeuristic:
		exploration_strategy = std::make_unique<HeuristicExploration>("");
		break;
	case ExplorationStrategy::kExploreMCTS:
		exploration_strategy = std::make_unique<MonteCarloTreeSearch>("");
		break;
	default:
		ASSERT(false && "Invalid ExplorationStrategy");
		break;
	}
}







namespace engine::adaptive {
struct ChainInfo {
	std::map<int64_t, size_t> runtime_histogram;
	size_t num_samples = 0;
	int64_t min_runtime = -1;
	int64_t max_runtime = -1;

	void add_sample(int64_t cyc_tup) {
		runtime_histogram[cyc_tup]++;

		if (cyc_tup > max_runtime || max_runtime < 0) {
			max_runtime = cyc_tup;
		}

		if (cyc_tup < min_runtime || min_runtime < 0) {
			min_runtime = cyc_tup;
		}
	}

	void get_summary(int64_t& median, size_t& median_prefix_count) const {
		median = -1;
		median_prefix_count = 0;
		size_t median_idx = num_samples/2;

		for (auto& keyval : runtime_histogram) {
			auto& cyc_tup = keyval.first;
			auto& count = keyval.second;

			ASSERT(cyc_tup >= 0);

			median_prefix_count += count;
			if (median_prefix_count >= median_idx && median <= 0) {
				median = cyc_tup;
				break;
			}
		}
	}
};


struct PipelineInfo {
	struct ChainKey {
		std::vector<DecisionState> decisions;

		bool operator==(const ChainKey& o) const {
			return decisions == o.decisions;
		}
	};

	struct ChainKeyHasher {
		size_t operator()(const ChainKey& k) const {
			size_t seed = 13;

			auto combine = [&] (size_t val) {
				seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
			};

			for (auto& d : k.decisions) {
				combine(std::hash<DecisionState>()(d));
			}
			return seed;
		}
	};
	std::unordered_map<ChainKey, std::unique_ptr<ChainInfo>, ChainKeyHasher> chains;

	int64_t min_cyc_tup = -1;
	int64_t base_cyc_tup = -1;

	std::unique_ptr<MctsStoredTree> mcts_stored_tree;
	GlobalDecisionInfo global_decision_info;

	std::mutex mutex;

	void evict(std::mt19937& gen, size_t num) {
		if (!num) {
			return;
		}

		std::vector<ChainKey> to_remove;
		to_remove.reserve(num);

		ReservoirSampleBuilder builder(num, gen, to_remove);

		for (auto& key : to_remove) {
			chains.erase(key);
		}
	}
};



static Actions*
make_action_from_decisions(const std::vector<Decision*>& new_decs,
	QueryStageShared& stage, std::vector<Actions>& tmp_actions,
	Actions* previous = nullptr)
{
	if (new_decs.empty()) {
		return nullptr;
	}

	bool found = false;
	Actions* found_actions = nullptr;
	with_temp_actions(tmp_actions, previous, new_decs,
		[&] (auto prev) {
			auto it = stage.explored_actions_set.find(prev);
			found = it != stage.explored_actions_set.end();
			if (found) {
				found_actions = *it;
			}
		});

	if (found) {
		return found_actions;
	} else {
		auto r = stage.extend_actions(previous, new_decs);
		return r.get();
	}
}


struct GlobalState {
#if 0
	utils::Cache<std::string,
		std::unique_ptr<PipelineInfo>> m_pipeline_map;
#endif
	std::unordered_map<std::string, std::unique_ptr<PipelineInfo>> m_pipeline_map;

	std::mutex m_mutex;

	std::mt19937 m_random_gen;

	static const size_t kMaxNumPipelines = 1024;
	static const size_t kMaxNumInfos = 16*1024;

	GlobalState()
	 : //m_pipeline_map, // (kMaxNumPipelines)
		m_random_gen(std::random_device{}())
	{

	}

	void clear() {
		m_pipeline_map.clear();
	}

	PipelineInfo*
	load(const std::string& key, QueryStageShared& stage)
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		auto& info = m_pipeline_map[key];
		if (!info) {
			info = std::make_unique<PipelineInfo>();
		}

		return info.get();
#if 0
		auto pipe_it = m_pipeline_map.lookup_or_create(key, [&] () {
			LOG_WARN("create: key='%s'", key.c_str());
			ASSERT(!m_pipeline_map.lookup(key));
			return std::make_unique<PipelineInfo>();
		});
		if (!pipe_it) {
			return nullptr;
		}

		return pipe_it->get();
#endif
	}

	void
	install_learned_knowledge(PipelineInfo* pipe_info, QueryStageShared& stage,
		int64_t given_min_cyc = -1)
	{
		ASSERT(pipe_info);

		auto& factory = stage.decision_factory;

		std::lock_guard<std::mutex> lock(m_mutex);

		// get fastest choice
		int64_t min_cyc_tup_median = -1;
		PipelineInfo::ChainKey min_cyc_tup_median_key;

		int64_t min_cyc_tup_total = -1;
		PipelineInfo::ChainKey min_cyc_tup_total_key;

		LOG_DEBUG("install: num_chains %llu", pipe_info->chains.size());

		for (auto& chain : pipe_info->chains) {
			int64_t median = -1;
			size_t prefix = 0;
			chain.second->get_summary(median, prefix);

			LOG_DEBUG("install: median %lld", median);

			if (median < min_cyc_tup_median || min_cyc_tup_median < 0) {
				if (median < given_min_cyc || given_min_cyc < 0) {
					min_cyc_tup_median = median;
					min_cyc_tup_median_key = chain.first;

					ASSERT(pipe_info->chains.find(chain.first) != pipe_info->chains.end());
					ASSERT(pipe_info->chains.find(min_cyc_tup_median_key) != pipe_info->chains.end());

					continue;
				}
			}
#if 1
			if (chain.second->min_runtime < min_cyc_tup_total || min_cyc_tup_total < 0) {
				if (chain.second->min_runtime < given_min_cyc || given_min_cyc < 0) {
					min_cyc_tup_total = chain.second->min_runtime;
					min_cyc_tup_total_key = chain.first;

					ASSERT(pipe_info->chains.find(min_cyc_tup_total_key) != pipe_info->chains.end());

					continue;
				}
			}
#endif
		}
		std::vector<Actions> tmp_actions;

		auto add = [&] (const auto& chain_key) {
			std::vector<Decision*> new_decs;
			for (auto& dec_state : chain_key.decisions) {
				new_decs.push_back(factory.new_from_state(dec_state));
			}

			Actions* action = make_action_from_decisions(std::move(new_decs),
				stage, tmp_actions);

			auto& info = stage.infos[action];

			if (action) {
				auto memoized_it = pipe_info->chains.find(chain_key);
				ASSERT(memoized_it != pipe_info->chains.end());
				info.memory_info = memoized_it->second.get();
				stage.actions_to_verify.push_back(action);
			}
		};

		size_t found = 0;

		if (min_cyc_tup_median >= 0) {
			found++;
			add(min_cyc_tup_median_key);
		}
		if (min_cyc_tup_total >= 0) {
			add(min_cyc_tup_total_key);
			found++;
		}

		LOG_DEBUG("install: found %llu", found);

	}

	void
	update(PipelineInfo* pipe_info, QueryStageShared& stage)
	{
		LOG_DEBUG("Brain: back propagate knowledge");

		std::lock_guard<std::mutex> lock(m_mutex);

		// std::vector<DecisionState> stored_
		std::vector<DecisionState> new_decs;

		int64_t min_cyc_tup = -1;
		int64_t base_cyc_tup = -1;

		for (auto& actions_keyval : stage.infos) {
			new_decs.clear();

			auto& actions = actions_keyval.first;
			if (actions) {
				actions->for_each_bottomup([&] (auto& dec) {
					new_decs.push_back(dec->get_decision_state());
				});
			}

			const auto& info = actions_keyval.second;
			if (!info.can_use_perf()) {
				continue;
			}

			double cyc_tup_dbl = info.perf.get_cycles_per_tuple();
			int64_t cyc_tup_int = cyc_tup_dbl;

			if (cyc_tup_int < min_cyc_tup || min_cyc_tup < 0) {
				min_cyc_tup = cyc_tup_int;
			}

			if (!actions && base_cyc_tup < 0) {
				base_cyc_tup = cyc_tup_int;
			}

			auto& chain_info = pipe_info->chains[PipelineInfo::ChainKey {
				std::move(new_decs) }];
			if (!chain_info) {
				chain_info = std::make_unique<ChainInfo>();
			}

			LOG_TRACE("update: add_sample %lld", cyc_tup_int);
			chain_info->add_sample(cyc_tup_int);
		}

		if (pipe_info->min_cyc_tup > min_cyc_tup || pipe_info->min_cyc_tup < 0) {
			pipe_info->min_cyc_tup = min_cyc_tup;
		}

		pipe_info->base_cyc_tup = base_cyc_tup;

		LOG_DEBUG("update: num_chains %llu", pipe_info->chains.size());

		if (pipe_info->chains.size() > kMaxNumInfos) {
			size_t diff = pipe_info->chains.size() - kMaxNumInfos;

			pipe_info->evict(m_random_gen, diff);
		}
	}
};
}







BrainSession::BrainSession(Brain& brain, const BrainStageId& query_stage_id,
	QueryStageShared* query_stage_shared, const std::string& dbg_name,
	bool test_random)
 : brain(brain), query_stage_id(query_stage_id),
	query_stage_shared(query_stage_shared), dbg_name(dbg_name),
	test_random(test_random)
{

}

BrainSession::~BrainSession()
{

}

void
BrainSession::propagate_profiling() const
{
	auto& stage = query_stage_shared;
	if (!stage) {
		return;
	}

	std::unique_lock lock(stage->shared_mutex);

	stage->sum_explore_cycles += sum_explore_cycles;
	stage->num_explore_calls += num_explore_calls;
	stage->sum_exploit_cycles += sum_exploit_cycles;
	stage->num_exploit_calls += num_exploit_calls;
	stage->sum_feedback_cycles += sum_feedback_cycles;
	stage->num_feedback_calls += num_feedback_calls;
}

template<typename S>
static bool has_enough_prof_data(const S& perf)
{
	return perf.get_sum_tuples() >= kMinTuples &&
		perf.get_sum_cycles() >= kMinCycles;
}

template<typename S, typename T>
static void print_prof(S& s, const T& perf)
{
	s << "cyc_tup " << perf.get_cycles_per_tuple()
		<< ", sum_tup " << perf.get_sum_tuples()
		<< ", sum_cyc " << perf.get_sum_cycles();
}

void
BrainSession::dump_state() const
{
	if (query_stage_shared->infos.empty()) {
		return;
	}

	std::stringstream s;

	auto print = [] (auto& s, auto& info) {
		print_prof(s, info.perf);

		s << ", #explore " << info.explore_used
			<< ", #exploit " << info.exploit_used.load();
	};


	auto base_info_it = query_stage_shared->infos.find(nullptr);
	if (base_info_it != query_stage_shared->infos.end()) {
		std::unique_lock lock(base_info_it->second.mutex);

		auto& perf = base_info_it->second.perf;
		if (base_info_it->second.can_use_perf() &&
				has_enough_prof_data(perf)) {
			s << "Base: ";
			print(s, base_info_it->second);
			s << std::endl;
		}
	}



	for (auto& actions : query_stage_shared->explored_actions) {
		s << "Action '" << actions->to_string() << "': ";
		auto info_it = query_stage_shared->infos.find(actions);
		if (info_it == query_stage_shared->infos.end()) {
			s << "No info" << std::endl;
			continue;
		}

		std::unique_lock lock(info_it->second.mutex);

		s << "p_disc=" << (int)(info_it->second.progress_when_discovered * 100.0) << "% ";
		s << "p_perf=" << (int)(info_it->second.progress_when_use_perf *100.0) << "% ";

		if (!info_it->second.example_plan) {
			s << "No plan" << std::endl;
			continue;
		}
#if 0
		engine::lolepop::Lolepop::print_plan(info_it->second.example_plan,
			"DumpState");
#endif
		if (!info_it->second.can_use_perf()) {
			s << "No perf" << std::endl;
			continue;
		}
		const auto& perf = info_it->second.perf;
		if (!has_enough_prof_data(perf)) {
			s << "Not enough perf data" << std::endl;
			continue;
		}

		print(s, info_it->second);
		s << std::endl;
	}

	if (query_stage_shared->num_explore_calls) {
		s << std::endl;
		s << "Prof: " << std::endl;
		if (query_stage_shared->num_explore_calls) {
			s << "explore: cyc_call "
				<< (query_stage_shared->sum_explore_cycles / query_stage_shared->num_explore_calls)
				<< ", #calls " << query_stage_shared->num_explore_calls << std::endl;
		}
		if (query_stage_shared->num_exploit_calls) {
			s << "exploit: cyc_call "
				<< (query_stage_shared->sum_exploit_cycles / query_stage_shared->num_exploit_calls)
				<< ", #calls " << query_stage_shared->num_exploit_calls << std::endl;
		}
		if (query_stage_shared->num_feedback_calls) {
			s << "feedback: cyc_call "
				<< (query_stage_shared->sum_feedback_cycles / query_stage_shared->num_feedback_calls)
				<< ", #calls " << query_stage_shared->num_feedback_calls << std::endl;
		}
	}

	if (!s.str().empty()) {
		LOG_WARN("Dump state: %s", s.str().c_str());
	}
}

void
BrainSession::discard(bool explore, const char* dbg_path)
{
	LOG_ERROR("BrainSession('%s'): %s: Unstable base plan, discard exploration (%s)",
		dbg_name.c_str(), dbg_path, explore ? "explore" : "exploit");

	return;

	query_stage_shared->exploration_strategy->discard();
	query_stage_shared->discard_exploration();
}

bool
BrainSession::make_explore_decision(const ExploreArgs& args)
{
	auto prof_start = profiling::rdtsc();
	auto result = _make_explore_decision(args);
	auto prof_stop = profiling::rdtsc();

	num_explore_calls++;
	sum_explore_cycles += prof_stop - prof_start;

	return result;
}

static int64_t
normalize_float(double f, double factor = 10.0)
{
	double d = f/factor;
	int64_t x = -1;
	if (std::isfinite(d)) {
		double lo = std::numeric_limits<int64_t>::min();
		double hi = std::numeric_limits<int64_t>::max();

		d = std::min(d, hi);
		d = std::max(d, lo);

		x = (int64_t)(d);
	}
	return x;
}

bool
BrainSession::_make_explore_decision(const ExploreArgs& args)
{
	LOG_TRACE("BrainSession('%s'): make_explore_decision",
		dbg_name.c_str());

	ASSERT(query_stage_shared);

	current_reopt_trigger = 0;

	// update metrics
	bool unstable = false;
	bool has_enough_prof_data = true;

	bool reload_global_state = false;

	std::unique_lock lock(query_stage_shared->shared_mutex);

	{
		auto& curr_info = query_stage_shared->infos[current_actions];

		query_stage_shared->prof_total_decisions++;
		leave_current_actions(current_actions, args.update_metrics,
			nullptr, nullptr, "explore", unstable, nullptr, args.progress,
			false, &curr_info);

		if ((!current_actions && unstable) || query_stage_shared->exploit_unstable) {
			query_stage_shared->exploit_unstable = 0;

			discard(false, "explore");
			reload_global_state = true;
		}

		// lack of profile info, need to run this a bit longer?
		if (!has_enough_prof_data) {
			const bool is_quick_intermediate =
				query_stage_shared->quick_intermediate_steps.find(current_actions) ==
				query_stage_shared->quick_intermediate_steps.end();

			// if this is supposed to be quick, just ignore and continue
			if (!is_quick_intermediate) {
				LOG_DEBUG("BrainSession('%s'): Not enough performance info",
					dbg_name.c_str());

				auto& info = query_stage_shared->infos[current_actions];
				info.explore_used++;
				return true;
			}
		}
	}


	// do the real exploiration

	auto& stage = *query_stage_shared;

	if (args.bedrock_root) {
		stage.plan_profile.update(
			args.bedrock_root->profile.get());
	}

	if (args.bedrock_root && !stage.plan_profile.empty()) {
		stage.infos[nullptr];
	}

	if (!stage.pipeline_sign.empty() && brain.global_state) {
		if (reload_global_state || !stage.pipeline_info) {
			// update previous
			if (stage.pipeline_info) {
				stage.exploration_strategy->store_io_global_state(stage);
				brain.global_state->update(stage.pipeline_info, stage);
			}

			auto& base_perf = stage.infos[nullptr];

			// load new one
			{
				const double kNormCycTup = 10.0;
				const double kNormSelect =    0.10;

				const auto& flat_plan_prof = stage.plan_profile.flattend;

				int64_t cyc_tup_int = base_perf.can_use_perf() ?
					normalize_float(base_perf.perf.get_cycles_per_tuple(), kNormCycTup) : 0;

				std::ostringstream ss;
				ss << stage.pipeline_sign << "|" << cyc_tup_int << "{";

				for (auto& op_prof : flat_plan_prof) {
					ASSERT(op_prof);
					ss << "," << normalize_float(op_prof->prof_data.get_selectivity(), kNormSelect);
				}

				ss << "}";

				stage.pipeline_info = brain.global_state->load(
					ss.str(), stage);
			}

			ASSERT(stage.pipeline_info);

			stage.actions_to_verify.clear();

			// install new best actions
			int64_t min_cycles = -1;

			if (base_perf.can_use_perf()) {
				double adapt_margin = base_perf.perf.get_cycles_per_tuple() * (1.0 - kImproveMargin);
				min_cycles = adapt_margin;
			}
			brain.global_state->install_learned_knowledge(
				stage.pipeline_info, stage, min_cycles);

			stage.exploration_strategy->load_from_global_state(stage);
		}
	}

	query_stage_shared->prof_total_decisions++;

	size_t new_explore_iteration = ++query_stage_shared->explore_iteration;

	// run base strategy once in a while
	if (!(new_explore_iteration % kRunBaseExploreModulo)) {
		LOG_DEBUG("BrainSession('%s'): Explore: Run base strategy", dbg_name.c_str());
		current_actions = nullptr;

		auto& info = query_stage_shared->infos[current_actions];
		info.explore_used++;
		return true;
	}

	ExplorationStrategy::ExploreStrategyArgs strategy_args;
	strategy_args.iteration = new_explore_iteration;
	strategy_args.query_stage = query_stage_shared;

#if 0
	ASSERT(query_stage_shared->explored_actions.size() < 1000
			&& "Maybe we need to evict some");
#endif

	ASSERT(query_stage_shared->exploration_strategy);

	// Global state requires us to verify the best discovered flavors are still good
	if (!query_stage_shared->actions_to_verify.empty()) {
		current_actions = query_stage_shared->actions_to_verify.back();
		query_stage_shared->actions_to_verify.pop_back();

		LOG_DEBUG("BrainSession('%s'): make_explore_decision: verify %p (%s)",
			dbg_name.c_str(), current_actions,
			current_actions ? current_actions->to_string().c_str() : "NULL");

		auto& info = query_stage_shared->infos[current_actions];
		ASSERT(info.memory_info);
		info.explore_used++;
		return true;
	}

	bool done = false;
	std::shared_ptr<Actions> result(
		stage.exploration_strategy->explore(done, strategy_args,
			args.budget_user));


	current_actions = nullptr;
	if (done) {
		LOG_DEBUG("BrainSession('%s'): make_explore_decision: done",
			dbg_name.c_str());
		ASSERT(!result);
		return false;
	}

	LOG_DEBUG("BrainSession('%s'): make_explore_decision: explore %p (%s)",
		dbg_name.c_str(), result.get(),
		result ? result->to_string().c_str() : "NULL");

	current_is_new_exploration = true;
	current_actions = result.get();

	auto& info = query_stage_shared->infos[current_actions];
	info.explore_used++;
	info.progress_when_discovered = args.progress;

	if (query_stage_shared->quick_intermediate_steps.find(current_actions) !=
			query_stage_shared->quick_intermediate_steps.end()) {
		current_reopt_trigger = -1;
	}

	return true;
}

bool
BrainSession::make_exploit_decision(const BrainSession::ExploitArgs& args)
{
	auto prof_start = profiling::rdtsc();
	auto result = _make_exploit_decision(args);
	auto prof_stop = profiling::rdtsc();

	num_exploit_calls++;
	sum_exploit_cycles += prof_stop - prof_start;

	return result;
}

bool
BrainSession::_make_exploit_decision(const BrainSession::ExploitArgs& args)
{
	LOG_TRACE("BrainSession('%s'): make_exploit_decision",
		dbg_name.c_str());

	current_reopt_trigger = 0;

	std::shared_lock lock(query_stage_shared->shared_mutex);

	auto curr_info_it = query_stage_shared->infos.find(current_actions);
	ASSERT(curr_info_it != query_stage_shared->infos.end());

	query_stage_shared->prof_total_decisions++;
	bool unstable = false;
	leave_current_actions(current_actions, args.update_metrics,
		nullptr, nullptr, "exploit", unstable, nullptr, args.progress,
		true, &curr_info_it->second);

	if (!current_actions && unstable) {
		lock.unlock();

		{
			// std::unique_lock lock2(query_stage_shared->shared_mutex);
			query_stage_shared->exploit_unstable.fetch_add(1);
			// discard(false, "Exploit");
		}

		lock.lock();
	}

	size_t new_exploit_iteration = 1 + query_stage_shared->exploit_iteration.fetch_add(1);

	// run base strategy once in a while
	if (!(new_exploit_iteration % kRunBaseExploitModulo)) {
		LOG_DEBUG("BrainSession('%s'): Exploit: Run base strategy", dbg_name.c_str());
		current_reopt_trigger = 0;
		current_actions = nullptr;

		ASSERT(query_stage_shared->infos.find(current_actions) != query_stage_shared->infos.end());
		auto& info = query_stage_shared->infos[current_actions];
		info.exploit_used++;

		return true;
	}

	current_actions = nullptr;

	if (test_random) {
		auto& actions = query_stage_shared->explored_actions;
		if (!actions.empty()) {
			lock.unlock();

			{
				std::unique_lock lock(query_stage_shared->shared_mutex);
				std::uniform_int_distribution<size_t> distrib(0, actions.size()-1);
				current_actions = actions[distrib(*query_stage_shared->gen)];
			}

			lock.lock();
		}
	} else {
		current_actions = query_stage_shared->get_min_cycles_per_tup(
			&args.min_cyc_tup, nullptr, true);
	}

	ASSERT(query_stage_shared->infos.find(current_actions) != query_stage_shared->infos.end());
	auto& info = query_stage_shared->infos[current_actions];
	info.exploit_used++;

	LOG_DEBUG("BrainSession('%s'): make_exploit_decision: found %p ('%s')",
		dbg_name.c_str(), current_actions,
		current_actions ? current_actions->to_string().c_str() : "NULL");

	current_reopt_trigger = 1;
 	return true;
}

void
BrainSession::leave_current_actions(Actions* actions, const Metrics* update_metrics,
	const std::shared_ptr<engine::lolepop::Lolepop>& current_plan,
	const std::shared_ptr<engine::voila::Context>& current_voila_context,
	const char* dbg_path, bool& unstable, bool* out_has_enough_prof_data,
	double progress, bool explore, DecisionInfo* curr_info)
{
	if (current_is_new_exploration) {
		query_stage_shared->num_explore_threads--;

		current_is_new_exploration = false;
	}

	if (!curr_info) {
		return;
	}

	std::unique_lock lock(curr_info->mutex);


	if (update_metrics && update_metrics->num_tuples > 100) {
		LOG_DEBUG("BrainSession('%s'): update_metrics for '%s' with '%s'",
			dbg_name.c_str(),
			actions ? actions->to_string().c_str() : "NULL",
			update_metrics->to_string().c_str());

		auto& perf = curr_info->perf;

		uint64_t tot_tuples = perf.get_sum_tuples();
		uint64_t tot_cycles = perf.get_sum_cycles();
		const bool has_enough_perf_info = has_enough_prof_data(perf);
		double old_avg_cyc_tup = .0;

		if (out_has_enough_prof_data) {
			*out_has_enough_prof_data = has_enough_perf_info;
		}

		if (has_enough_perf_info) {
			old_avg_cyc_tup = perf.get_cycles_per_tuple();
		}

		// update metrics
		perf.push(*update_metrics);
		query_stage_shared->prof_total_updates++;

		if (has_enough_perf_info) {
			double new_avg_cyc_tup = perf.get_cycles_per_tuple();

			if (std::isfinite(old_avg_cyc_tup) && std::isfinite(new_avg_cyc_tup)) {
				double div1 = old_avg_cyc_tup / new_avg_cyc_tup;

				if (div1 > kStabilityMargin || div1 < (1/kStabilityMargin)) {
					unstable = true;
				}
			}

			if (unstable) {
				LOG_DEBUG("BrainSession('%s'): %s: Update '%s': "
					"Unstable performance: tot_tuples=%llu, tot_cycles=%llu "
					"old avg=%f, new avg=%f",
					dbg_name.c_str(), dbg_path,
					actions ? actions->to_string().c_str() : "NULL",
					tot_tuples, tot_cycles,
					old_avg_cyc_tup, new_avg_cyc_tup);
			}
		}

		auto expected = DecisionInfo::Status::kSucceeded;

		if (curr_info->status.compare_exchange_strong(
				expected, DecisionInfo::Status::kHasProfiling)) {
			curr_info->progress_when_use_perf = progress;
		}
	}

	if (current_plan && !curr_info->example_plan) {
		curr_info->example_plan = current_plan;

		ASSERT(current_voila_context && !curr_info->example_voila_context);
		curr_info->example_voila_context = current_voila_context;
	}
}


void
BrainSession::feedback(FeedbackReason reason,
	const std::shared_ptr<Lolepop>& current_plan,
	const std::shared_ptr<engine::voila::Context>& current_voila_context)
{
	auto prof_start = profiling::rdtsc();

	std::shared_lock lock(query_stage_shared->shared_mutex);

	auto curr_info_it = query_stage_shared->infos.find(current_actions);
	ASSERT(curr_info_it != query_stage_shared->infos.end());

	auto& info = curr_info_it->second;

	ASSERT(info.status != DecisionInfo::Status::kUnexplored);

	auto pending_new = DecisionInfo::Status::kFailed;
	auto pending_exp = DecisionInfo::Status::kPending;

	switch (reason) {
	case FeedbackReason::kSuccessful:
		pending_new = DecisionInfo::Status::kSucceeded;
		break;

	case FeedbackReason::kRecover:
		pending_new = DecisionInfo::Status::kFailed;
		break;

	default:
		ASSERT(false);
		break;
	}

	info.status.compare_exchange_strong(pending_exp, pending_new);

	auto prof_stop = profiling::rdtsc();

	num_feedback_calls++;
	sum_feedback_cycles += prof_stop - prof_start;

	if (current_plan && !info.example_plan) {
		std::unique_lock lock2(info.mutex);

		if (!info.example_plan) {
			info.example_plan = current_plan;
			info.example_voila_context = current_voila_context;
		}
	}
}


void
BrainStageId::from_stage(const QueryStage& query_stage)
{
	stage = query_stage.get_stage_id();
	query = query_stage.get_query().get_query_id();
}

namespace engine::protoplan {
struct SignaturePlanOpVisitor : PlanOpVisitor {
	template<typename T>
	void append_list(std::ostream& out, const T& list) {
		bool first = true;
		for (auto& val : list) {
			if (!first) {
				*s << ",";
			}
			*s << val;
			first = false;
		}
	}

	void visit(Scan& scan, const PlanOpPtr&) final {
		auto scan_op = dynamic_cast<const plan::Scan*>(scan.plan_op);
		ASSERT(scan_op);

		*s << ",t=" << scan_op->table << ",c=[";
		append_list(*s, scan_op->cols);
		*s <<  "]";
	}
	void visit(Select&, const PlanOpPtr&) final {

	}
	void visit(Output&, const PlanOpPtr&) final {

	}
	void visit(Project&, const PlanOpPtr&) final {

	}
	void visit(JoinWrite&, const PlanOpPtr&) final {

	}
	void visit(JoinBuild&, const PlanOpPtr&) final {

	}
	void visit(JoinProbeDriver& probe, const PlanOpPtr&) final {
		auto join_op = dynamic_cast<const plan::HashJoin*>(probe.plan_op);
		ASSERT(join_op);

		*s << ",fk=" << join_op->fk1
			<< ",pk=" << join_op->probe_keys.size()
			<< ",pp=" << join_op->probe_payloads.size()
			<< ",bp=" << join_op->build_payloads.size();
	}
	void visit(JoinProbeCheck&, const PlanOpPtr&) final {

	}
	void visit(JoinProbeGather&, const PlanOpPtr&) final {

	}
	void visit(GroupByBuild& build, const PlanOpPtr&) final {
		auto group_op = dynamic_cast<const plan::HashGroupBy*>(build.plan_op);
		ASSERT(group_op);

		*s << ",g=" << build.is_global
			<< ",a=" << build.has_aggregates
			<< ",k=" << group_op->keys.size()
			<< ",p=" << group_op->payloads.size()
			<< ",a=" << group_op->aggregates.size();
	}
	void visit(GroupByPartition&, const PlanOpPtr&) final {

	}
	void visit(GroupByScan&, const PlanOpPtr&) final {

	}

	SignaturePlanOpVisitor(std::ostringstream& stream) : s(&stream) {
	}

private:
	std::ostringstream* s;
};
} /* engine::protoplan */


static std::string
pipeline_signature(const QueryStage& stage,
	const std::shared_ptr<engine::lolepop::Lolepop>& first_op)
{
	std::ostringstream ss;

	ss << "i=" << stage.get_stage_id()
		<< ",p=" << stage.get_max_parallelism();

	LoleplanPass::apply_source_to_sink(first_op, [&] (auto& op) {
		ss << "," << op->name << "(";

		if (op->rel_op) {
			ss << ",ro=" << op->rel_op->get_stable_op_id();
		}
		if (op->plan_op) {
			ss << ",po=" << op->plan_op->get_stable_op_id();
			engine::protoplan::SignaturePlanOpVisitor plan_op_sign(ss);
			plan_op_sign(op->plan_op);
		}
		ss << ")";
	});
	return ss.str();
}

BrainSession*
Brain::make_session(QueryStage* stage, const std::string& dbg_name, bool test_random,
	const std::shared_ptr<Lolepop>& root_op, QueryConfig& config)
{
	ASSERT(stage && "Handle independent stages");

	bool learning = global_state && config.enable_learning();

	std::lock_guard<std::mutex> lock(mutex);

	BrainStageId id;
	QueryStageShared* query_stage_shared = nullptr;

	if (stage) {
		id.from_stage(*stage);

		auto it = running_stages.find(id);
		if (it == running_stages.end()) {
			auto config = stage->get_query().config.get();
			auto share = std::make_unique<QueryStageShared>(*this, config);
			if (learning) {
				std::string pipeline_sign(pipeline_signature(*stage, root_op));

				LOG_DEBUG("Brain: loading data for '%s'", pipeline_sign.c_str());
				share->pipeline_sign = std::move(pipeline_sign);
			}
			share->num_threads++;
			query_stage_shared = share.get();

			running_stages.insert({ id, std::move(share) });
		} else {
			it->second->num_threads++;
			query_stage_shared = it->second.get();
		}
	}

	ASSERT(!!query_stage_shared == !!stage);

	return new BrainSession(*this, id, query_stage_shared, dbg_name, test_random);
}

void
Brain::destroy_session(BrainSession* session)
{
	if (!session) {
		return;
	}

	std::lock_guard<std::mutex> lock(mutex);

	if (session->query_stage_shared) {
		auto it = running_stages.find(session->query_stage_id);
		ASSERT(it != running_stages.end() && "Must exist");
		ASSERT(it->second);

		ASSERT(it->second.get() == session->query_stage_shared);

		auto& share = *it->second;

		ASSERT(share.num_threads > 0);
		share.num_threads--;

		session->propagate_profiling();

		if (!share.num_threads) {
			if (global_state && share.pipeline_info) {
				share.exploration_strategy->store_io_global_state(share);
				global_state->update(share.pipeline_info, share);
			}

			session->dump_state();
			running_stages.erase(it);
		}
	}

	delete session;
}

void
Brain::clear()
{
	if (global_state) {
		global_state->clear();
	}
	rule_triggers = std::make_unique<RuleTriggers>();
}

Brain::Brain(excalibur::Context& sys_context)
 : sys_context(sys_context)
{
	rule_triggers = std::make_unique<RuleTriggers>();

	global_state = std::make_unique<engine::adaptive::GlobalState>();
}

Brain::~Brain()
{
}

struct MctsSave {
	bool _store(QueryStageShared& stage,
		std::unique_ptr<MctsStoredTree::Node>& r,
		MonteCarloTreeSearch::TreeNode* node)
	{
		bool failed = node->failed;
		if (!r->visits) {
			r->failed = node->failed;
		}

		if (!failed) {
			auto info_it = stage.infos.find(node->actions);

			if (info_it == stage.infos.end()) {
				return false;
			}

			std::unique_lock lock(info_it->second.mutex);

			r->example_plan = info_it->second.example_plan;
			if (!r->example_plan) {
				return false;
			}
			ASSERT(r->example_plan);
			r->example_voila_context = info_it->second.example_voila_context;
		}

		return true;
	}

	void store(QueryStageShared& stage,
		std::unique_ptr<MctsStoredTree::Node>& r,
		MonteCarloTreeSearch::TreeNode* node)
	{
		r->visits = node->visits;
		r->reward = node->reward;

		bool succ = _store(stage, r, node);
		if (!succ && !r->visits) {
			r->failed++;
			r->visits = 0;
			r->reward = 0;
		}
	}

	std::unique_ptr<MctsStoredTree::Node>
	operator()(QueryStageShared& stage, MonteCarloTreeSearch::TreeNode* node)
	{
		std::unique_ptr<MctsStoredTree::Node> r(std::make_unique<MctsStoredTree::Node>());

		r->children.reserve(node->children.size());
		for (auto& child : node->children) {
			r->children.emplace_back((*this)(stage, child.get()));
		}

		r->decision_states.reserve(node->decisions.size());
		for (auto& dec : node->decisions) {
			DecisionState state(dec->get_decision_state());
			stage.decision_factory.new_from_state(state);
			r->decision_states.push_back(std::move(state));
		}

		store(stage, r, node);

		return r;
	}
};

struct MctsLoad {
	std::vector<Actions> tmp_actions;
	MonteCarloTreeSearch& tree;

	MctsLoad(MonteCarloTreeSearch& tree) : tree(tree) {

	}

	std::unique_ptr<MonteCarloTreeSearch::TreeNode>
	operator()(QueryStageShared& stage, MctsStoredTree::Node* node,
		MonteCarloTreeSearch::TreeNode* parent)
	{
		std::vector<Decision*> new_decs;

		ASSERT(node->decision_states.size());
		new_decs.reserve(node->decision_states.size());
		for (auto& state : node->decision_states) {
			new_decs.push_back(stage.decision_factory.new_from_state(state));
		}
		ASSERT(!new_decs.empty());

		std::unique_ptr<MonteCarloTreeSearch::TreeNode> r(std::make_unique<MonteCarloTreeSearch::TreeNode>(
			std::move(new_decs)));

		r->reward = node->reward;
		r->visits = node->visits;
		r->failed = node->failed;

		tree.add_global_dec_info(node->decision_states, node->reward, node->visits);

		r->actions = make_action_from_decisions(r->decisions,
			stage, tmp_actions, parent ? parent->actions : nullptr);

		auto& info = stage.infos[r->actions];

		{
			std::unique_lock lock(info.mutex);
			if (!info.example_plan) {
				info.example_plan = node->example_plan;
				info.example_voila_context = node->example_voila_context;
			}
		}


		for (auto& child : node->children) {
			auto n = (*this)(stage, child.get(), r.get());
			auto p = n.get();
			r->add_child(std::move(n));

			ASSERT(p->parent);
		}

		return r;
	}

};

template<typename T>
void check_has_parent(const T& n, size_t level = 0)
{
	if (level > 0) {
		ASSERT(n.parent);
	}
	for (auto& child : n.children) {
		check_has_parent<T>(*child, level+1);
	}
}

void
MonteCarloTreeSearch::store_io_global_state(QueryStageShared& stage)
{
	LOG_DEBUG("store_io_global_state: store old");

	auto pipe = stage.pipeline_info;
	ASSERT(pipe);

	std::unique_lock lock(pipe->mutex);

	if (root && !root->children.empty()) {
		check_has_parent(*root);

		MctsSave save;
		pipe->mcts_stored_tree = std::make_unique<MctsStoredTree>();
		pipe->mcts_stored_tree->root = save(stage, root.get());

		pipe->global_decision_info = global_decision_info;
	}
}

void
MonteCarloTreeSearch::load_from_global_state(QueryStageShared& stage)
{
	LOG_DEBUG("load_from_global_state: load new");

	auto pipe = stage.pipeline_info;
	ASSERT(pipe);

	std::unique_lock lock(pipe->mutex);

	root.reset();

	if (pipe->mcts_stored_tree && pipe->mcts_stored_tree->root) {
		MctsLoad load(*this);
		root = load(stage, pipe->mcts_stored_tree->root.get(),
			nullptr);

		if (root) {
			check_has_parent(*root);
		}

		global_decision_info = pipe->global_decision_info;
	}
	last_node = nullptr;
}
