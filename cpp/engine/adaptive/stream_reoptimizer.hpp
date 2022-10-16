#pragma once

#include <vector>
#include <memory>
#include <unordered_set>

#include "system/memory.hpp"
#include "system/profiling.hpp"

#include "engine/adaptive/metrics.hpp"

struct BudgetUser;
struct IBudgetManager;

namespace engine {
struct Stream;
struct StreamGenCodeProfData;
} /* engine */

namespace engine::voila {
struct CodeBlockBuilder;
struct FuseGroups;
} /* engine::voila */

namespace engine::adaptive {
struct CodeReoptGroup;
struct Decision;
struct Actions;
struct Brain;
struct BrainSession;
} /* engine::adaptive */

namespace engine::adaptive {

struct StreamReoptimizer {
	StreamReoptimizer(Stream& stream);

	void scan_feedback(bool& do_reopt, bool& force_reopt,
			int64_t cycles_last_reopt);

	void scan_feedback_with_tuples(size_t num_tuples) {
		m_track_sum_tuples += num_tuples;
		// m_track_last_call = profiling::rdtsc();
	}

	void set_bedrock(BudgetUser* budget_user);


	using ReoptGroupPtr = std::shared_ptr<adaptive::CodeReoptGroup>;

	void explore(BudgetUser& budget_user);
	void exploit();
	void recover(const char* dbg_path);
	~StreamReoptimizer();

	void execute();

	void update_progress(double p) {
		progress = p;
	}

	std::shared_ptr<IBudgetManager> exploration_budget_manager;

	void print_profile(std::ostream& s) const;

	void reset_scan_tracking() {
		reset_current_tracking();

		m_track_last_call = profiling::rdtsc();
	}

	std::string get_current_action_name() const;
private:
	ReoptGroupPtr m_bedrock_group;
	ReoptGroupPtr m_prev_group;
	ReoptGroupPtr m_curr_group;


	Stream& stream;
	size_t m_num_explore_calls = 0;
	size_t m_num_exploit_calls = 0;
	const double better_margin_plus1;

	Brain& brain;
	BrainSession* brain_session;

	size_t m_track_sum_tuples;
	uint64_t m_track_last_call;

	double progress = 0.0;

	void reset_current_tracking() {
		m_track_sum_tuples = 0;
		m_track_last_call = 0;
	}

	ReoptGroupPtr save_current_execution(BudgetUser* budget_user, bool first_call,
		adaptive::Actions* decision);

	void apply_actions(BudgetUser* budget_user, adaptive::Actions* actions,
		const char* dbg_path, bool explore);

	void update_current_metrics(const adaptive::Metrics& metrics);

	template<typename T>
	void with_metrics(const T& fun)
	{
		// track current execution
		double cyc_per_tup = .0;
		if (m_track_sum_tuples) {
			auto num_cycles = profiling::rdtsc() - m_track_last_call;
			cyc_per_tup = (double)num_cycles/(double)m_track_sum_tuples;
			update_current_metrics(adaptive::Metrics { cyc_per_tup, m_track_sum_tuples, num_cycles });
		}

		fun();

		reset_current_tracking();
	}

	std::unordered_map<Actions*, ReoptGroupPtr> cached_groups;

	size_t num_apply_actions_calls = 0;
	size_t sum_apply_actions_cycles = 0;

	std::unique_ptr<StreamGenCodeProfData> gen_code_prof;

	int64_t reopt_trigger_countdown = 0;
};

} /* engine::adaptive */