#include "scheduler.hpp"
#include "system.hpp"
#include <Remotery.h>
#include "build.hpp"

#include <boost/context/continuation.hpp>

#ifdef HAS_LIBNUMA
#include <numa.h>
#endif

#ifdef HAS_PAPI
#include <papi.h>
#include <pthread.h>
#endif

using namespace scheduler;

Scheduler g_scheduler;

#if 0
static Remotery* g_rmt;
#endif

static thread_local Task* g_scheduler_curr_task = nullptr;
static thread_local scheduler::SchedulerThread* g_scheduler_curr_thread = nullptr;

Task::Task()
{

}

Task::~Task()
{
}

void
Task::operator()()
{
	ASSERT(false && "not implemented");
}

namespace scheduler {

struct Barrier {
public:
	explicit Barrier(std::size_t iCount) : 
		mThreshold(iCount), 
		mCount(iCount), 
		mGeneration(0) {
	}

	void Wait() {
		std::unique_lock<std::mutex> lLock{mMutex};
		auto lGen = mGeneration;
		if (!--mCount) {
			mGeneration++;
			mCount = mThreshold;
			mCond.notify_all();
		} else {
			mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
		}
	}

private:
	std::mutex mMutex;
	std::condition_variable mCond;
	std::size_t mThreshold;
	std::size_t mCount;
	std::size_t mGeneration;
};

struct Queue {
	std::shared_ptr<Task> head;
	std::shared_ptr<Task> tail;
	int64_t max_parallel = -1;
	int64_t cur_parallel = 0;
	int64_t length = 0;
};

struct SchedulerThread {
	Scheduler& sched;
	const size_t id;
	const int64_t numa_node;
	Queue* queues;
	Queue local_queue;
	const size_t num_queues;
	std::thread thread;

	SchedulerThread(Scheduler& sched, size_t id, int64_t numa_node)
	 : sched(sched), id(id), numa_node(numa_node),
	 	queues(&sched.queues[0]), num_queues(sched.num_queues), thread(_loop, this)
	{
		ASSERT(numa_node >= 0);
		ASSERT(queues);
	}

	void join() {
		thread.join();
	}

	~SchedulerThread() {
	}

	static void _loop(SchedulerThread* t) { t->loop(); }

	void loop();

	void run_task(Task& task);

	bool get_next_task(std::shared_ptr<Task>& task, Queue*& queue);
};

struct TaskContinuation {
	boost::context::continuation continuation;
	boost::context::continuation* sink = nullptr;
	int id;

	TaskContinuation(int id) : id(id) {

	}
};
} /* scheduler */

inline bool
can_use_queue(const Queue& q)
{
	if (!q.head) {
		return false;
	}

	return q.max_parallel == -1 || q.cur_parallel < q.max_parallel;
}

bool
SchedulerThread::get_next_task(std::shared_ptr<Task>& task,
	scheduler::Queue*& queue)
{
	auto home_queue = &queues[numa_node];

	queue = nullptr;

	if (!queue && can_use_queue(local_queue)) {
		queue = &local_queue;
	}

	if (!queue && can_use_queue(*home_queue)) {
		queue = home_queue;
	}

	if (!queue) {
		// find other queue
		size_t offset = numa_node+1;
		for (size_t i=0; i<num_queues; i++) {
			if (offset >= num_queues) {
				ASSERT(offset == num_queues);
				offset = 0;
			}

			if (can_use_queue(queues[offset])) {
				queue = &queues[offset];
				break;
			}
		}
	}

	if (!queue) {
		// find other local queue
		for (auto& t : sched.m_threads) {
			if (!t || t.get() == this) {
				continue;
			}
			queue = &t->local_queue;
			if (queue) {
				break;
			}
		}
	}

	// get new task
	if (queue && queue->head) {
		if (queue->head.get() == queue->tail.get()) {
			queue->tail.reset();
		}

		ASSERT(queue->length > 0);
		queue->cur_parallel++;
		queue->length--;
		task = queue->head;
		queue->head = task->m_next;
		return true;
	} else  {
		return false;
	}
}

void
SchedulerThread::loop()
{
	g_scheduler_curr_thread = this;

#ifdef HAS_LINUX_cpu_set_t
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(id, &cpuset);
	int rc = pthread_setaffinity_np(
		thread.native_handle(), sizeof(cpu_set_t), &cpuset);
	if (rc) {
		LOG_ERROR("pthread_setaffinity_np: Error");
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	{
		CPU_ZERO(&cpuset);
		pthread_getaffinity_np(thread.native_handle(),
			sizeof(cpuset), &cpuset);

		ASSERT(CPU_ISSET(id, &cpuset));

		int cpu = sched_getcpu();
		ASSERT(cpu == (int)id);
	}
#endif

#ifdef HAS_LIBNUMA
	if (false && numa_node >= 0) {
		ASSERT(numa_node < 64);
		auto mask = numa_allocate_nodemask();
		numa_bitmask_clearall(mask);
		mask = numa_bitmask_setbit(mask, numa_node);
		numa_bind(mask);
		numa_free_nodemask(mask);
	}
	// TODO: screws up thread assignments
#endif

	std::this_thread::sleep_for(std::chrono::milliseconds(20));

#ifdef HAS_PAPI
	if (PAPI_thread_init(pthread_self) != PAPI_OK) {
		LOG_ERROR("PAPI: Cannot initalize SchedulerThread");
		ASSERT(false);
	}
#endif

	sched.m_barrier->Wait();
	std::shared_ptr<Task> task;
	std::shared_ptr<Task> prev_task;
	Queue* queue = nullptr;

	while (1) {
		std::unique_lock lock(sched.m_mutex);

		if (!sched.is_running()) {
			break;
		}

		if (task) {
			if (task->interrupted) {
				// LOG_WARN("Reschedule %p", task.get());
				ASSERT(queue);

				// wake up some workers
				if (queue->max_parallel != -1) {
					ASSERT(queue->cur_parallel == queue->max_parallel);

					// wake up some more workers
					sched.m_has_work.notify_all();
				}

				// re-enqueue
				if (queue->head) {
					ASSERT(queue->tail);
					queue->tail->m_next = task;
				} else {
					queue->head = task;
				}
				queue->tail = task;

				ASSERT(queue->cur_parallel > 0);
				queue->cur_parallel--;
				queue->length++;

				// move deallocation out of lock
				// task.reset();
				prev_task = std::move(task);
			} else {
				// LOG_WARN("Task Done %p", task.get());
				sched.m_total_done++;
				task.reset();
			}
		}

		if (!get_next_task(task, queue)) {
			sched.m_has_work.wait(lock);
		}

		lock.unlock();

		prev_task.reset();
		if (task) {
			task->m_next.reset();

			try {
				run_task(*task);
			} catch (...) {
				LOG_ERROR("Scheduler: catched exception");
			}
		}
	}
}

void
SchedulerThread::run_task(Task& task)
{
	LOG_TRACE("Run Task %p", &task);
	// LOG_DEBUG("Running task %p", task.get());
	g_scheduler_curr_task = &task;

	task.interrupted = false;
	task.start_rdtsc_clock = profiling::physical_rdtsc();
	task.rdtsc_budget = 1000000;
	task.scheduler_thread = this;
	task.virtual_rdtsc_clock_run_start = task.start_rdtsc_clock;

	rmt_ScopedCPUSample(SchedulerTask, 0);
	if (!task.interruptable || task.cheaply_interruptable) {
		task();
	} else {
		if (task.continuation) {
			auto& cont = task.continuation->continuation;
			ASSERT(task.continuation->id == (int)id && "Todo: otherwise sink may be wrong");

			cont = cont.resume();
		} else {
			task.continuation = std::make_unique<TaskContinuation>(id);

			task.continuation->continuation = boost::context::callcc(
				[this, &task] (boost::context::continuation&& sink) {
					task.continuation->sink = &sink;
					task();
					return std::move(sink);
				}
			);
		}

		if (!task.interrupted) {
			task.continuation.reset();
		}
	}

	auto stop = profiling::physical_rdtsc();
	ASSERT(stop >= task.virtual_rdtsc_clock_run_start);
	auto diff = stop - task.virtual_rdtsc_clock_run_start;
	task.virtual_rdtsc_clock_acc += diff;

	g_scheduler_curr_task = nullptr;
}

Scheduler::Scheduler()
{
#ifdef HAS_PAPI
	int retval;

	retval = PAPI_library_init(PAPI_VER_CURRENT);
	if (retval != PAPI_VER_CURRENT) {
		LOG_ERROR("PAPI: Error initializing: %s", PAPI_strerror(retval));
		ASSERT(false);
	}
#endif

#if 0
	rmtError error = rmt_CreateGlobalInstance(&g_rmt);
	if(RMT_ERROR_NONE != error) {
		LOG_ERROR("Error launching Remotery %d\n", error);
	}
#endif
	m_running = false;
	m_initialized = false;
	num_queues = 0;
	queues = nullptr;
}

void
Scheduler::init(size_t threads)
{
	std::unique_lock<std::mutex> lock(m_mutex);
	if (m_initialized) {
		return;
	}

	m_barrier = std::make_unique<Barrier>(threads+1);
	LOG_TRACE("Initializing scheduler ...");
#ifdef HAS_LIBNUMA
	int r = numa_available();
	ASSERT(r != -1);
#endif

	m_running = true;
	int64_t numa_node = -1;

	int64_t max_node = 0;
	for (unsigned int i=0; i<threads; i++) {
#ifdef HAS_LIBNUMA
		numa_node = numa_node_of_cpu(i);
		ASSERT(numa_node != -1);

		max_node = std::max(max_node, numa_node);
#else
		max_node = 0;
#endif
	}

	ASSERT(max_node >= 0);
	num_queues = max_node+1;
	queues = new Queue[num_queues];

	for (unsigned int i=0; i<threads; i++) {
#ifdef HAS_LIBNUMA
		numa_node = numa_node_of_cpu(i);
		ASSERT(numa_node != -1);
#else
		numa_node = 0;
#endif
		m_threads.emplace_back(std::make_unique<SchedulerThread>(*this, i, numa_node));
	}


	m_barrier->Wait();

	m_initialized = true;
	LOG_TRACE("Done");
}

Scheduler::~Scheduler()
{
	LOG_TRACE("Shutting down scheduler ...");
	{
		std::unique_lock<std::mutex> lock(m_mutex);
		m_running = false;
		m_has_work.notify_all();
	}
	LOG_TRACE("Sent signal to workers");

	for (auto& t : m_threads) {
		t->join();
	}

#if 0
	rmt_DestroyGlobalInstance(g_rmt);
#endif

	delete[] queues;
}

void
Scheduler::submit_n(const std::vector<std::shared_ptr<Task>>& tasks)
{
	if (!m_initialized) {
		init();
	}
	// LOG_DEBUG("Submitting task %p ...", task.get());

	std::unique_lock<std::mutex> lock(m_mutex);
	ASSERT(m_initialized.load());

	for (auto& task : tasks) {
		Queue* queue = nullptr;

		if (task->task_class == kIncrementalCompilation) {
			auto worker_id = get_current_worker_id();
			if (worker_id >= 0) {
				ASSERT(worker_id < m_threads.size());

				auto& worker = m_threads[worker_id];
				ASSERT(worker);

				queue = &worker->local_queue;
			}
		}

		// assign queue
		if (!queue) {
			if (task->home_queue_id < 0) {
				// auto assign
				task->home_queue_id = m_queues_insert_index;
				m_queues_insert_index++;
				if (m_queues_insert_index >= num_queues) {
					m_queues_insert_index = 0;
				}
			}

			ASSERT(task->home_queue_id < num_queues);
			queue = &queues[task->home_queue_id];
		}

		if (queue->tail) {
			queue->tail->m_next = task;
		} else {
			queue->head = task;
		}
		queue->tail = task;
		queue->length++;
	}

	m_total_submitted += tasks.size();

	m_has_work.notify_all();
}

void
Scheduler::submit(const std::shared_ptr<Task>& task)
{
	if (!m_initialized) {
		init();
	}
	// LOG_DEBUG("Submitting task %p ...", task.get());

	std::unique_lock<std::mutex> lock(m_mutex);
	ASSERT(m_initialized.load());

	Queue* queue = nullptr;

	if (task->task_class == kIncrementalCompilation) {
		auto worker_id = get_current_worker_id();
		if (worker_id >= 0) {
			ASSERT(worker_id < m_threads.size());

			auto& worker = m_threads[worker_id];
			ASSERT(worker);

			queue = &worker->local_queue;
		}
	}

	if (!queue) {
		if (task->home_queue_id < 0) {
			// auto assign
			task->home_queue_id = m_queues_insert_index;
			m_queues_insert_index++;
			if (m_queues_insert_index >= num_queues) {
				m_queues_insert_index = 0;
			}
		}

		ASSERT(task->home_queue_id < num_queues);
		queue = &queues[task->home_queue_id];
	}

	if (queue->tail) {
		queue->tail->m_next = task;
	} else {
		queue->head = task;
	}
	queue->tail = task;
	queue->length++;

	m_total_submitted++;

	m_has_work.notify_all();
}


namespace scheduler {

Task*
get_current_task()
{
	return g_scheduler_curr_task;
}

int64_t
get_current_worker_id()
{
	auto t = g_scheduler_curr_thread;
	if (!t) {
		return -1;
	}
	return t->id;
}

int64_t
get_current_numa_node()
{
	auto t = g_scheduler_curr_thread;
	if (!t) {
		return -1;
	}
	return t->numa_node;
}

bool
__yield(Task* task, bool force)
{
	LOG_TRACE("Yield %p", task);
	bool y = force || should_yield(task);
	task->interrupted = y;
	if (task->interrupted && task->continuation) {
		// expensive context saving and jumping back to scheduler
		LOG_TRACE("yield: jump back");

		auto sink = task->continuation->sink;
		ASSERT(sink);
		*sink = sink->resume();
		LOG_TRACE("yield: continue");
	}
	return y;
}

} /* scheduler */
