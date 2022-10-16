#include "budget.hpp"
#include "system/profiling.hpp"

void
IBudgetManager::update_user_meta(uint64_t sum_budget, uint64_t sum_excess_consumed,
	int64_t sum_time, int64_t num_allocs)
{
	std::lock_guard lock(mutex);

	user_meta.num_users++;
	user_meta.total_real_time += sum_time;
	user_meta.total_sum_budget += sum_budget;
	user_meta.total_sum_excess_consumed += sum_excess_consumed;
	user_meta.total_num_allocs += num_allocs;
}

uint64_t
IBudgetManager::get_average_real_alloc_time()
{
	std::lock_guard lock(mutex);

	if (!user_meta.num_users) {
		return 0;
	}

	return user_meta.total_real_time / user_meta.num_users;
}

double
IBudgetManager::get_average_overalloc_factor()
{
	std::lock_guard lock(mutex);

	if (!user_meta.num_users) {
		return .0;
	}

	double excess = user_meta.total_sum_excess_consumed;
	double alloced = user_meta.total_sum_budget;
	double overalloc = excess / alloced;

	if (!std::isfinite(overalloc) || overalloc <= .0) {
		return .0;
	}

	overalloc = std::min(overalloc, 10.0);
	return overalloc;
}



bool
GlobalBudgetManager::try_alloc_budget(int64_t curr_excess_consumed,
	int64_t future_budget)
{
	ASSERT(curr_excess_consumed >= 0);
	ASSERT(future_budget >= 0);

	std::lock_guard lock(mutex);

	LOG_DEBUG("GlobalBudgetManager: try_alloc_budget: "
		"curr_excess_consumed=%lld, future_budget=%lld"
		"curr_budget=%lld, max_budget=%lld",
		curr_excess_consumed, future_budget,
		curr_budget, max_budget);

	curr_budget += curr_excess_consumed;

	if (curr_budget + future_budget > max_budget) {
		return false;
	}

	curr_budget += future_budget;
	return true;
}

void
GlobalBudgetManager::return_budget(int64_t budget)
{
	std::lock_guard lock(mutex);

	/* ASSERT(curr_budget >= budget); */
	curr_budget -= budget;
}

void
GlobalBudgetManager::update_max_budget(int64_t max, int64_t& old_max, int64_t& curr_used)
{
	std::lock_guard lock(mutex);

	old_max = max_budget;
	curr_used = curr_budget;

	if (max >= 0) {
		max_budget = max;
	}
}



LimitedBudgetManager::LimitedBudgetManager(double fraction, IBudgetManager* parent)
 : parent(parent), fraction(fraction)
{

}

bool
LimitedBudgetManager::try_alloc_budget(int64_t curr_excess_consumed, int64_t future_budget)
{
	ASSERT(has_max_budget);

	LOG_TRACE("LimitedBudgetManager: try_alloc_budget: "
		"curr_excess_consumed=%lld, future_budget=%lld, "
		"curr_budget=%lld, max_budget=%lld",
		curr_excess_consumed, future_budget,
		curr_budget, max_budget);

	curr_budget += curr_excess_consumed;

	if (curr_budget + future_budget > max_budget) {
		LOG_TRACE("LimitedBudgetManager: OutOfBudget");
		return false;
	}

	if (!parent->try_alloc_budget(curr_excess_consumed, future_budget)) {
		return false;
	}

	curr_budget += future_budget;
	return true;
}

void
LimitedBudgetManager::return_budget(int64_t budget)
{
	parent->return_budget(budget);

	ASSERT(curr_budget >= budget);
	curr_budget -= budget;
}

void
LimitedBudgetManager::update_max_budget(int64_t max, int64_t& old_max, int64_t& curr_used)
{
	parent->update_max_budget(max, old_max, curr_used);
	has_max_budget = true;

	double new_max = (double)max*fraction;

	ASSERT(std::isfinite(new_max));
	int64_t new_max_int = (int64_t)new_max;

	curr_used = curr_budget;
	old_max = max_budget;

	max_budget = new_max_int;
	curr_budget = 0;

	LOG_TRACE("update_max_budget: max %lld, internal_max %lld",
		max, new_max_int);
}


uint64_t
BudgetUser::get_initial_budget(uint64_t init_budget) const
{
	uint64_t r = init_budget;
	uint64_t avg = 0;

	if (!init_budget) {
		avg = manager->get_average_real_alloc_time();
		r = std::max(avg, 2*kDefaultBudget);
	}

	LOG_DEBUG("get_initial_budget: r=%llu, init=%llu, avg=%llu",
		r, init_budget, avg);

	return r;
}

bool
BudgetUser::ensure_sufficient_slow_path(uint64_t new_budget,
	uint64_t curr_tick, const char* dbg_name, bool throw_oob)
{
	if (!manager) {
		return true;
	}

	ASSERT(has_budget && new_budget);

	uint64_t consumed_ticks = curr_tick - clock_start;

	if (consumed_ticks + new_budget >= curr_budget) {
		// we minimally need to allocate our excess cycles
		int64_t curr_excess_ticks = 0;
		if (consumed_ticks > curr_budget) {
			curr_excess_ticks = consumed_ticks - curr_budget;
		}

		uint64_t b = new_budget;

		// account for recent consumed ticks over budget
		{
			double overalloc = (double)b * manager->get_average_overalloc_factor();
			ASSERT(overalloc >= .0);

			uint64_t overalloc_int = b + overalloc;

			LOG_DEBUG("ensure_sufficient: Overalloc to %f (%llu)",
				overalloc, overalloc_int);

			b = std::max(b, overalloc_int);
		}

		// overalloc budget a bit, to save some expensive calls
		b = std::max(b, 10*kDefaultBudget);

		alloc_calls++;
		sum_budget_alloced += b;
		sum_excess_consumed += curr_excess_ticks;

		bool successful = manager->try_alloc_budget(curr_excess_ticks, b);
		if (!successful) {
			LOG_DEBUG("OutOfBudgetException: dbg_name '%s'", dbg_name);

			if (throw_oob) {
				throw OutOfBudgetException(dbg_name);
			} else {
				return false;
			}
		}

		curr_budget += b;
	}

	return true;
}

void
BudgetUser::release()
{
	if (!manager) {
		return;
	}

	uint64_t tick = get_tick();
	uint64_t diff = tick - clock_start;

	ASSERT(has_budget);
	has_budget = false;

	if (diff <= curr_budget) {
		// return some
		auto b = curr_budget - diff;
		if (b > 0) {
			manager->return_budget(b);
		}
	} else {
		// exceeded budget
		auto amnt = diff - curr_budget;
		if (!manager->try_alloc_budget(amnt, 0)) {
			LOG_WARN("release: Exceeded budget by %llu", amnt);
		}
	}

	manager->update_user_meta(sum_budget_alloced, sum_excess_consumed,
		diff, alloc_calls);
}