#pragma once

#include <stdint.h>
#include <mutex>
#include <memory>
#include <cmath>
#include "system/system.hpp"
#include "system/profiling.hpp"

static constexpr uint64_t kDefaultBudget =
#ifdef IS_DEBUG_BUILD
		1*
#else
		10*
#endif
		1024*1024;

struct IBudgetManager {
	virtual bool try_alloc_budget(int64_t curr_excess_consumed,
		int64_t future_budget) = 0;
	virtual void return_budget(int64_t budget) = 0;
	virtual void update_max_budget(int64_t max, int64_t& old_max,
		int64_t& curr_used) = 0;

	virtual void update_user_meta(uint64_t sum_budget, uint64_t sum_excess_consumed, 
		int64_t sum_time, int64_t num_allocs);

	virtual ~IBudgetManager() = default;

	virtual uint64_t get_average_real_alloc_time();
	virtual double get_average_overalloc_factor();

private:
	struct UserMeta {
		uint64_t num_users = 0;
		uint64_t total_real_time = 0;
		uint64_t total_sum_budget = 0;
		uint64_t total_sum_excess_consumed = 0;
		uint64_t total_num_allocs = 0;
	};

	UserMeta user_meta;
	std::mutex mutex;
};

struct GlobalBudgetManager : IBudgetManager {
	bool try_alloc_budget(int64_t curr_excess_consumed, int64_t future_budget) final;
	void return_budget(int64_t budget) final;
	void update_max_budget(int64_t max, int64_t& old_max, int64_t& curr_used) final;

private:
	std::mutex mutex;
	int64_t curr_budget = 0;
	int64_t max_budget = 0;
};

struct LimitedBudgetManager : IBudgetManager {
	LimitedBudgetManager(double fraction, IBudgetManager* parent);

	bool try_alloc_budget(int64_t curr_excess_consumed, int64_t future_budget) final;
	void return_budget(int64_t budget) final;
	void update_max_budget(int64_t max, int64_t& old_max, int64_t& curr_used) final;

private:
	int64_t curr_budget = 0;
	int64_t max_budget = 0;
	IBudgetManager* parent;
	bool has_max_budget = false;
	double fraction = 1.0;
};

struct OutOfBudgetException {
	OutOfBudgetException(const char* dbg_name) {
	}
};

struct BudgetUser {
	BudgetUser(IBudgetManager* manager) : manager(manager) {
	}

	template<typename T>
	bool
	with(const T& fun, const char* dbg_name = nullptr)
	{
		return with<T>(0, fun, dbg_name);
	}

	template<typename T>
	bool
	with(uint64_t init_budget, const T& fun, const char* dbg_name = nullptr)
	{
		ASSERT(!has_budget);

		if (manager) {
			init_budget = get_initial_budget(init_budget);
			bool successful = manager->try_alloc_budget(0, init_budget);
			if (!successful) {
				return false;
			}
			alloc_calls++;
			sum_budget_alloced += init_budget;
		}
		has_budget = true;
		curr_budget = init_budget;

		clock_start = get_tick();
		try {
			fun();
			release();
			return true;
		} catch (OutOfBudgetException& e) {
			LOG_DEBUG("Ran out of budget");
			release();
			return false;
		} catch (...) {
			release();
			throw;
		}
	}

	void ensure_sufficient(uint64_t new_budget, const char* dbg_name = nullptr) {
		_ensure_sufficient(new_budget, dbg_name, true);
	}

	bool has_sufficient(uint64_t new_budget, const char* dbg_name = nullptr) {
		return _ensure_sufficient(new_budget, dbg_name, false);
	}

	bool has_sufficient(const char* dbg_name = nullptr) {
		return has_sufficient(kDefaultBudget, dbg_name);
	}

	std::unique_ptr<BudgetUser> create_sibling() const {
		return std::make_unique<BudgetUser>(manager);
	}

private:
	IBudgetManager* manager;
	uint64_t curr_budget = 0;
	bool has_budget = false;
	uint64_t clock_start;

	// tracking info
	uint64_t alloc_calls = 0;
	uint64_t sum_budget_alloced = 0;
	uint64_t sum_excess_consumed = 0;

	void release();
	bool ensure_sufficient_slow_path(uint64_t new_budget, uint64_t curr_tick,
		const char* dbg_name, bool throw_oob);

	uint64_t get_initial_budget(uint64_t init_budget) const;

	uint64_t get_tick() const {
		 return profiling::rdtsc();
	}

	inline bool _ensure_sufficient(uint64_t new_budget, const char* dbg_name,
			bool throw_oob) {
		if (!manager) {
			return true;
		}
		auto curr_tick = get_tick();

		uint64_t consumed_ticks = curr_tick - clock_start;
		if (UNLIKELY(consumed_ticks + new_budget >= curr_budget)) {
			return ensure_sufficient_slow_path(new_budget, curr_tick, dbg_name,
				throw_oob);
		}
		return true;
	}
};
