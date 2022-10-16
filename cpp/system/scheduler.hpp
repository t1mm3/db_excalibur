#pragma once

#include <vector>
#include <functional>
#include <queue>
#include <memory>
#include <future>
#include <atomic>
#include <utility>
#include <thread>

#include "system.hpp"
#include "profiling.hpp"

struct BudgetUser;

namespace scheduler {
struct Task;
struct FuncTask;
struct SchedulerThread;
struct Barrier;
struct Queue;


struct Scheduler {
private:
	friend struct SchedulerThread;

	std::mutex m_mutex;
	Queue* queues;
	size_t num_queues;

	std::vector<std::unique_ptr<SchedulerThread>> m_threads;
	std::unique_ptr<Barrier> m_barrier;
	std::condition_variable m_has_work;

	size_t m_queues_insert_index = 0;

	std::atomic<bool> m_initialized;
	std::atomic<bool> m_running;

	uint64_t m_total_submitted;
	uint64_t m_total_done;

public:
	void submit(const std::shared_ptr<Task>& task);
	void submit_n(const std::vector<std::shared_ptr<Task>>& tasks);
	void submit(const std::function<void()>& f) {
		submit(std::dynamic_pointer_cast<Task>(std::make_shared<FuncTask>(f)));
	}


	
	Scheduler();
	void init(size_t threads = std::thread::hardware_concurrency());
	~Scheduler();

	bool is_idle() {
		std::unique_lock<std::mutex> lock(m_mutex);
		return m_total_submitted == m_total_done;
	}

	void wait_until_idle() {
		while (!is_idle()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
	}

	bool is_running() const {
		return m_running;
	}

	size_t get_num_threads() const {
		return m_threads.size();
	}
};

enum TaskClass {
	kUnknown = 0,
	kQuery,
	kInitialCompilation,
	kIncrementalCompilation
};

} /* scheduler */

extern scheduler::Scheduler g_scheduler;

namespace scheduler {

Task* get_current_task();
int64_t get_current_worker_id();
int64_t get_current_numa_node();
bool __yield(Task* task, bool force);

struct Scheduler;
struct TaskContinuation;

struct Task {
	bool interrupted = false; //!< True, if task got interrupted, will be re-scheduled
	bool cheaply_interruptable = false; //!< True, if task is cheaply interruptable
	bool interruptable = false;
	uint64_t start_rdtsc_clock;
	int64_t rdtsc_budget = -1;

	uint64_t virtual_rdtsc_clock_run_start = 0;
	uint64_t virtual_rdtsc_clock_acc = 0;
	TaskClass task_class = TaskClass::kUnknown;
	std::unique_ptr<TaskContinuation> continuation;
	SchedulerThread* scheduler_thread = nullptr;
	BudgetUser* budget_user = nullptr;

	int64_t home_queue_id = -1;

	virtual void operator()();

	Task();
	virtual ~Task();

private:
	std::shared_ptr<Task> m_next;

	friend struct Scheduler;
	friend struct SchedulerThread;
};

struct FuncTask : Task {
	std::function<void()> m_func;

	FuncTask(const std::function<void()>& f) : m_func(f) {
	}
	
	void operator()() override {
		m_func();
	}
};

struct FsmTaskFlags {
	static const uint32_t kDone = 1 << 1;
	static const uint32_t kRepeat = 1 << 2;
	static const uint32_t kYield = 1 << 3;
};

struct AsyncOptions {
	TaskClass task_class = TaskClass::kUnknown;
	bool long_running = false;
};

template<typename F, typename... Args>
auto async_ex(const AsyncOptions& options,
	F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type>
{
	using return_type = typename std::result_of<F(Args...)>::type;
	auto task = std::make_shared<std::packaged_task<return_type()>>(
		std::bind(std::forward<F>(f), std::forward<Args>(args)...));
	std::future<return_type> result = task->get_future();

	auto func_task = std::make_shared<FuncTask>([task] () {
		(*task)();
	});

	func_task->task_class = options.task_class;

	// TODO: requires workign task migration (#11)
	func_task->interruptable = false; // options.long_running;

	g_scheduler.submit(std::dynamic_pointer_cast<Task>(std::move(func_task)));

	return result;
}

template<typename F, typename... Args>
auto async(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type>
{
	return async_ex(AsyncOptions(),
		std::forward<F>(f), std::forward<Args>(args)...);
}

inline bool is_idle() {
	return g_scheduler.is_idle();
}


inline bool
should_yield(Task* task = get_current_task())
{
	if (UNLIKELY(!task)) {
		return false;
	}
	if (UNLIKELY(!g_scheduler.is_running())) {
		return true;
	}

	if (!task->interruptable) {
		return false;
	}

	if (task->rdtsc_budget < 0) {
		return false;
	}

	int64_t diff = profiling::physical_rdtsc() - task->start_rdtsc_clock;
	return (diff > task->rdtsc_budget);
}

inline bool
yield(Task* task = get_current_task())
{
	bool y = should_yield(task);
	if (LIKELY(!y)) {
		return false;
	}
	return __yield(task, false);
}


template<typename StateT>
struct FsmTask : Task, FsmTaskFlags {

	FsmTask(const StateT& initial_state)
	 : state(initial_state)
	{
		cheaply_interruptable = true;
		interruptable = true;
	}

	void operator()() override {
		Task* task = get_current_task();
		while (1) {
			uint32_t cont = dispatch(state);
			if (!cont || (cont & kDone)) {
				break;
			}
			if ((cont & kYield) || should_yield(task)) {
				__yield(task, !!(cont & kYield));
				break;
			}
		}
	}

	virtual uint32_t dispatch(StateT& state) = 0;

private:
	StateT state;
};

struct Scope {
	bool did_yield = false;

	Scope(const char* name, Task* task) {
	}

	void set_yield() {
		did_yield = true;
	}

	~Scope() {
	}
};

#define SCHEDULER_SCOPE(NAME, TASK) scheduler::Scope sched_scope_##NAME(#NAME, TASK)
#define SCHEDULER_SCOPE_YIELD(NAME, TASK) sched_scope_##NAME.set_yield()

#if 0
	rmt_ScopedCPUSample(NAME, 0);
#endif

} /* schedule */

