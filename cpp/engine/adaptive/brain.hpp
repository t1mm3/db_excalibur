#pragma once

#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

struct QueryStage;
struct QueryConfig;
struct BudgetUser;

namespace excalibur {
struct Context;
} /* excalibur */

namespace engine::lolepop {
struct Lolepop;
} /* engine::lolepop */

namespace engine::voila {
struct FlavorSpec;
struct Context;
} /* engine::voila */

namespace engine::adaptive {
struct Decision;
struct Actions;
struct Brain;
struct QueryStageShared;
struct Metrics;
struct RuleTriggers;
struct RuleIterator;
} /* engine::voila */

namespace engine::adaptive {

struct BrainStageId {
	size_t stage = 0;
	size_t query = 0;

	void from_stage(const QueryStage& stage);

	bool operator==(const BrainStageId& o) const {
		return stage == o.stage && query == o.query;
	}
};

} /* engine::adaptive */


namespace std {
	template <>
	struct hash<engine::adaptive::BrainStageId>
	{
		std::size_t operator()(const engine::adaptive::BrainStageId& k) const
		{
			using std::size_t;

			return ((hash<int>()(k.stage)
				^ (hash<int>()(k.stage) << 1)) >> 1)
				^ (hash<int>()(k.query) << 1);
		}
	};
} /* std */

namespace engine::adaptive {

struct ExplorationStrategy;
struct DecisionInfo;

struct BrainSession {
	using Lolepop = engine::lolepop::Lolepop;

	Brain& brain;
	BrainStageId query_stage_id;
	QueryStageShared* query_stage_shared;
	const std::string dbg_name;
	const bool test_random;

	enum FeedbackReason {
		kSuccessful,
		kRecover
	};

	BrainSession(Brain& brain, const BrainStageId& query_stage_id,
		QueryStageShared* query_stage_shared, const std::string& dbg_name,
		bool test_random);
	~BrainSession();

	struct ExploreArgs {
		const Metrics* update_metrics = nullptr;
		std::shared_ptr<Lolepop> bedrock_root;
		std::shared_ptr<Lolepop> current_root;
		std::shared_ptr<voila::Context> current_voila_context;
		BudgetUser* budget_user = nullptr;

		double progress = .0;
	};

	bool make_explore_decision(const ExploreArgs& args);


	struct ExploitArgs {
		double min_cyc_tup = 0.0;
		const Metrics* update_metrics = nullptr;
		double progress = .0;
	};

	bool make_exploit_decision(const ExploitArgs& args);

	void feedback(FeedbackReason reason,
		const std::shared_ptr<Lolepop>& current_plan,
		const std::shared_ptr<engine::voila::Context>& current_voila_context);

	Actions* get_current_actions() const { return current_actions; }

	int64_t get_reopt_trigger_countdown() const { return current_reopt_trigger; }

	void dump_state() const;

	void propagate_profiling() const;
protected:
	Actions* current_actions = nullptr;
	bool current_is_new_exploration = false;
	int64_t current_reopt_trigger = 0;

	std::vector<std::unique_ptr<Decision>>& get_all_decisions(
		Decision* base_decision = nullptr) const;

	bool _make_explore_decision(const ExploreArgs& args);
	bool _make_exploit_decision(const ExploitArgs& args);

	void leave_current_actions(Actions* actions, const Metrics* update_metrics,
		const std::shared_ptr<engine::lolepop::Lolepop>& current_plan,
		const std::shared_ptr<engine::voila::Context>& current_voila_context,
		const char* dbg_path, bool& out_unstable, bool* has_enough_prof_data,
		double progress, bool explore, DecisionInfo* curr_info);

	void discard(bool explore, const char* dbg_path);


	size_t num_explore_calls = 0;
	size_t sum_explore_cycles = 0;

	size_t num_exploit_calls = 0;
	size_t sum_exploit_cycles = 0;

	size_t num_feedback_calls = 0;
	size_t sum_feedback_cycles = 0;

};

struct ExplorationStrategy {
	ExplorationStrategy(const std::string& dbg_name) : dbg_name(dbg_name) {

	}

	struct ExploreStrategyArgs {
		size_t iteration = 0;
		QueryStageShared* query_stage = nullptr;
	};

	virtual std::shared_ptr<Actions> explore(bool& done,
		const ExploreStrategyArgs& args, BudgetUser* budget_user) = 0;

	virtual ~ExplorationStrategy() = default;

	virtual void discard() {}
	virtual void store_io_global_state(QueryStageShared& stage) {}
	virtual void load_from_global_state(QueryStageShared& stage) {}

protected:
	const std::string dbg_name;
};

struct GlobalState;

struct Brain {
	using Lolepop = engine::lolepop::Lolepop;

	BrainSession* make_session(QueryStage* stage, const std::string& dbg_name,
		bool test_random, const std::shared_ptr<Lolepop>& root_op,
		QueryConfig& config);
	void destroy_session(BrainSession* session);

	Brain(excalibur::Context& sys_context);
	virtual ~Brain();

	excalibur::Context& get_sys_context() const { return sys_context; }

	void clear();

private:
	excalibur::Context& sys_context;
	std::mutex mutex;
	std::unordered_map<BrainStageId, std::unique_ptr<QueryStageShared>> running_stages;

	std::unique_ptr<RuleTriggers> rule_triggers;

	friend struct QueryStageShared;
	friend struct BrainSession;
	friend struct RuleIterator;

	std::unique_ptr<GlobalState> global_state;
};

} /* engine::adaptive */

