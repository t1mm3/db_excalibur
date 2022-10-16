#include "code_cache.hpp"

#include "excalibur_context.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"
#include "system/profiling.hpp"

#include "engine/voila/compile_request.hpp"
#include "engine/query.hpp"
#include "engine/budget.hpp"

#include "llvmcompiler/llvm_interface.hpp"
#include "flavor.hpp"

#include <queue>
#include <chrono>
#include <thread>

using namespace engine;
using namespace voila;

namespace engine::voila {
struct ReqEntry {
	std::atomic<uint64_t> last_updated;
	std::atomic<uint64_t> num_hits;
	std::atomic<int64_t> curr_outside_uses;
	std::atomic<uint64_t> evict_counter;

	std::shared_future<CodeFragmentPtr> future;
	const Signature full_signature;
	const Signature op_signature;

	std::shared_ptr<FlavorSpec> flavor_spec;

	ReqEntry(const Signature& full_signature, const Signature& op_signature,
		const std::shared_ptr<FlavorSpec>& flavor_spec)
	 : full_signature(full_signature), op_signature(op_signature),
		flavor_spec(flavor_spec)
	{
		last_updated = 0;
		num_hits = 0;
		curr_outside_uses = 0;
	}

	bool has_completed() const {
		if (cache_has_completed) {
			return true;
		}
		cache_has_completed = _has_completed();
		return cache_has_completed;
	}

	bool has_outside_uses() const {
		return curr_outside_uses.load();
	}

	bool can_evict() const {
		return !has_outside_uses() && has_completed();
	}

	bool has_error = false;

private:
	bool _has_completed() const;
	mutable bool cache_has_completed = false;
};


struct ReqPool {
	memory::BlockedPool<ReqEntry> pool;

	ReqPool(memory::Context& ctx) : pool(ctx) {}

	ReqEntry* alloc() { return pool.alloc(); }
	void free(ReqEntry* p) { pool.free(p); }
};
} /* engine::voila */

bool
ReqEntry::_has_completed() const
{
	ASSERT(future.valid());
	auto status = future.wait_for(std::chrono::seconds(0));
	switch (status) {
	case std::future_status::ready:   return true;
	case std::future_status::timeout: return false;
	default:
		ASSERT(false && "Invalid future status");
		return true;
	}
}

void
CodeCache::wait_until_quiet()
{
	std::this_thread::sleep_for(std::chrono::seconds(1));
	trigger_cleanup();
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

CodeCache::CodeCache(excalibur::Context& sys_context)
 : mem_context(nullptr, "CodeCache", -1), sys_context(sys_context)
{
	reset_stats();

	request_pool = std::make_unique<ReqPool>(mem_context);

	prof_num_requests = 0;
	prof_num_compiled = 0;
	prof_num_hits = 0;
	prof_num_miss = 0;

	time_stamp = 0;

	requests.reserve(capacity);

	watchdog_active = true;
	watchdog_thread = std::thread(&CodeCache::watchdog_func, this);
}

CodeCache::~CodeCache()
{
	{
		std::unique_lock lock(mutex);
		watchdog_active = false;
		clear_nolock();
	}

	watchdog_thread.join();
}

ReqEntry*
CodeCache::new_req_entry(const Signature& full_signature,
	const Signature& op_signature, const std::shared_ptr<FlavorSpec>& flavor_spec,
	uint64_t this_time_stamp)
{
#ifdef COMBINE_ALLOCS
	void* data = request_pool->alloc();
	ReqEntry* result = new (data) ReqEntry(full_signature, op_signature, flavor_spec);
#else
	ReqEntry* result = new ReqEntry(full_signature, op_signature, flavor_spec);
#endif

	result->last_updated = this_time_stamp;
	result->curr_outside_uses = 1;
	result->evict_counter = 1;

	LOG_TRACE("CodeCache: new_req %p", result);
	return result;
}

void
CodeCache::free_req_entry(ReqEntry* entry, bool do_free)
{
	if (!entry) {
		return;
	}
#ifdef COMBINE_ALLOCS
	entry->~ReqEntry();
	request_pool->free(entry);
#else
	delete entry;
#endif
}

void
CodeCache::release(CodeCacheItemPtr&& f)
{
	auto& entry = f->internal_entry;
	LOG_TRACE("CodeCache::release(%p): ReqEntry %p", f.get(), entry);

	if (!entry) {
		return;
	}

	std::shared_lock lock(mutex);

	auto old = entry->curr_outside_uses.fetch_add(-1);
	ASSERT(old > 0);
}

CodeCacheItemPtr
CodeCache::get(const std::shared_ptr<CompileRequest>& req, Query* query, uint64_t flags)
{
	uint64_t start_cycles = 0;

	if (query && query->profile) {
		start_cycles = profiling::rdtsc();
	}

	const bool async_compilation = req->asynchronous_compilation;
	auto cached_item = _get(req, query, flags);

	if (query && query->profile) {
		query->profile->total_code_cache_get_cy += profiling::rdtsc() - start_cycles;
	}

	if (!async_compilation) {
		cached_item->future.wait();
	}
	return cached_item;
}

static const uint64_t kBudget = 1024*1024;

static CodeFragmentPtr
compile_fragment(std::string name,
	const std::shared_ptr<CompileRequest>& req, std::string dbg_name,
	CodeCache* code_cache, ReqEntry* entry,
	std::shared_ptr<QueryProfile> query_profile)
{
	auto task = scheduler::get_current_task();
	SCHEDULER_SCOPE(CompileFragment, task);

	ASSERT(req && code_cache);

	auto start_clock = std::chrono::high_resolution_clock::now();
	auto start_cycles = profiling::rdtsc();

	BudgetUser budget_user(req->budget_manager.get());

	CodeFragmentPtr result;

	bool succeeded = false;
	try {
		succeeded = budget_user.with([&] () {
			result = engine::voila::llvmcompiler::compile_fragment(name, *req,
				dbg_name, budget_user);
		});

		if (!succeeded) {
			LOG_DEBUG("compile_fragment(req=%p): Ran out of budget '%s'",
				req.get(), dbg_name.c_str());

			throw OutOfBudgetException("compile_fragment");
		}
	} catch(...) {
		code_cache->entry_has_error(entry);
		throw;
	}


	ASSERT(result);

	auto stop_cycles = profiling::rdtsc();
	auto stop_clock = std::chrono::high_resolution_clock::now();

	auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
		stop_clock - start_clock).count();
	auto duration_cy = stop_cycles - start_cycles;

	// add to attached query
	if (query_profile) {
		query_profile->total_compilation_time_us += duration_us;
		query_profile->total_compilation_time_cy += duration_cy;
	}

	result->compilation_time_us = duration_us;

	return result;
}

CodeCacheItemPtr
CodeCache::request_compilation(const std::string& dbg_name, ReqEntry* entry,
	const std::string& full_signature, const std::string& op_signature,
	uint64_t flags, const std::shared_ptr<CompileRequest>& req, Query* query)
{
	LOG_TRACE("CodeCache::get: request compilation");
	// std::string func_name("jit_" + std::to_string(id_counter) + full_signature);
	std::string func_name("jit_" + std::to_string(id_counter));

	id_counter++;
	prof_num_compiled++;

#if 1
	scheduler::AsyncOptions async_options;

	async_options.task_class = flags & kIncrementalCompilation ?
		scheduler::TaskClass::kIncrementalCompilation :
		scheduler::TaskClass::kInitialCompilation;
	async_options.long_running = true;

	std::shared_future<CodeFragmentPtr> future =
		scheduler::async_ex(
			async_options,
			compile_fragment,
			std::move(func_name),
			req,
			std::move(dbg_name),
			this,
			entry,
			query ? query->profile : nullptr).share();
#else
	std::shared_future<CodeFragmentPtr> future =
		std::async(std::launch::async,
			compile_fragment,
			std::move(func_name), req,
			std::move(dbg_name), this,
			query ? query->profile : nullptr).share();
#endif


	return std::make_unique<CodeCacheItem>(std::move(future), entry, *this,
		full_signature, op_signature);
}

CodeCacheItemPtr
CodeCache::_get(const std::shared_ptr<CompileRequest>& req, Query* query, uint64_t flags)
{
	auto this_time_stamp = time_stamp.fetch_add(1);
	prof_num_requests++;

	std::string full_signature(req->get_full_signature(query));
	std::string op_signature(req->get_operation_signature(query));

	ASSERT(!full_signature.empty() && !op_signature.empty());

	if (!req->can_be_cached || hack_all_unique) {
		// make unique
		std::string unique_str("unique_" + std::to_string(this_time_stamp));
		full_signature += unique_str;
		op_signature += unique_str;
	}

	LOG_DEBUG("CodeCache::get: signature '%s'", full_signature.c_str());

	auto cached_item = [&] (const char* path, auto& entry, auto& lock,
			auto&& full_signature, auto&& op_signature) {
		LOG_TRACE("get: cached(%s)", path);

		ASSERT(!entry->has_error);

		// increase reference counter
		auto old = entry->curr_outside_uses.fetch_add(1);
		ASSERT(old >= 0);

		LOG_TRACE("CodeCache: cached %p", entry);

		lock.unlock();

		prof_num_hits++;
		entry->num_hits++;
		entry->last_updated = this_time_stamp;
		entry->evict_counter++;


		return std::make_unique<CodeCacheItem>(entry->future, entry, *this,
			std::move(full_signature), std::move(op_signature));
	};

#define CHECK(TYPE_STRING) \
	{ \
		if (flags & kGetFlagMatchAnyFlavor) { \
			auto req_it = flavors.find(op_signature); \
			if (LIKELY(req_it != flavors.end())) { \
				auto& vec = req_it->second; \
				if (LIKELY(!vec.empty())) { \
					/* match, bail out */ \
					return cached_item(TYPE_STRING, vec[0], lock, \
						std::move(full_signature), std::move(op_signature)); \
				} \
			} \
		} else { \
			auto req_it = requests.find(full_signature); \
			if (LIKELY(req_it != requests.end())) { \
				/* match, bail out */ \
				return cached_item(TYPE_STRING, req_it->second, lock, \
					std::move(full_signature), std::move(op_signature)); \
			} \
		} \
	}


	// quick check
	{
		std::shared_lock lock(mutex);
		CHECK("shared_lock");
	}

	// expensive check
	{
		std::unique_lock lock(mutex);
		CHECK("unique_lock");

		auto new_req = new_req_entry(full_signature, op_signature,
			req->flavor, this_time_stamp);

		prof_num_miss++;
		auto cached_item = request_compilation(full_signature, new_req,
			full_signature, op_signature, flags, req, query);
		new_req->future = cached_item->future;
		requests.insert({ std::move(full_signature), new_req});

		auto& flavor_vec = flavors[std::move(op_signature)];
		flavor_vec.push_back(new_req);

		return cached_item;
	}

#undef CHECK
}

void
CodeCache::watchdog_func()
{
	while (1) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		trigger_cleanup();
		cleanup_iteration++;

		std::unique_lock lock(mutex);
		if (!watchdog_active) {
			break;
		}
	}
}

struct MarkableTuple {
	uint64_t num_hits;
	ReqEntry* entry;
};

struct MarkableTupleLFUCompare {
	bool operator()(MarkableTuple& a, MarkableTuple& b) const {
		return a.num_hits > b.num_hits;
	}
};

struct EvictableTuple {
	uint64_t last_updated;
	ReqEntry* entry;
};

void
CodeCache::remove_entry_from_tables_nolock(ReqEntry* entry)
{
	requests.erase(entry->full_signature);

	auto flavor_it = flavors.find(entry->op_signature);
	ASSERT(flavor_it != flavors.end());
	auto& flavor_vec = flavor_it->second;
	if (flavor_vec.size() == 1) {
		flavors.erase(flavor_it);
	} else {
		auto item = std::find(flavor_vec.begin(), flavor_vec.end(), entry);
		ASSERT(item != flavor_vec.end() && "Must exist");

		flavor_vec.erase(item);

		auto item2 = std::find(flavor_vec.begin(), flavor_vec.end(), entry);
		ASSERT(item2 == flavor_vec.end() && "Must not exist");
	}
}

bool
CodeCache::erase_entry_nolock(ReqEntry* entry, bool force)
{
	if (!force) {
		if (!entry->can_evict() || entry->evict_counter.load()) {
			return false;
		}
	}

	// it had it's chance, delete the request
	remove_entry_from_tables_nolock(entry);

	free_req_entry(entry, false);

	return true;
}

void
CodeCache::entry_has_error(ReqEntry* entry)
{
	ASSERT(entry);

	std::unique_lock lock(mutex);

	// delete later, but remove from bookkeeping
	erroneous_requests.insert(entry);
	remove_entry_from_tables_nolock(entry);

	entry->has_error = true;
}

void
CodeCache::trigger_cleanup()
{
	{
		std::unordered_set<ReqEntry*> new_errors;

		std::unique_lock lock(mutex);

		for (auto& entry : erroneous_requests) {
			if (entry->has_outside_uses()) {
				LOG_TRACE("CodeCache: erroneous request reenter %p (uses %d)",
					entry, (int)entry->curr_outside_uses.load());
				new_errors.insert(entry);
			} else {
				// get rd of this one
				LOG_TRACE("CodeCache: erroneous request free: %p", entry);
				free_req_entry(entry, false);
			}
		}

		erroneous_requests = std::move(new_errors);
	}


	std::vector<MarkableTuple> markable_tuple_container;
	std::vector<EvictableTuple> evictable_entries;

	double eviction_chance;

	// compute metrics
	{
		uint64_t evicted = 0;
		uint64_t evict = 0;
		for (size_t i=0; i<kCleanupWindowSize; i++) {
			evict += cleanup_evict[i];
			evicted += cleanup_evicted[i];
		}

		if (evicted > 0 && evict > 0) {
			eviction_chance = (double)evicted / (double)evict;
			eviction_chance = std::max(eviction_chance, 0.10);
			eviction_chance = std::min(eviction_chance, 0.90);
		} else {
			eviction_chance = 0.5;
		}

		LOG_TRACE("CodeCache: eviction_chance %f (%lld/%lld)",
			eviction_chance, evicted, evict);
	}

	// predict sizes
	{
		std::shared_lock lock(mutex);

		if (!cleanup_enabled) {
			return;
		}

		int64_t num_excess = cleanup_get_num_excess();
		if (!num_excess) {
			return;
		}

		int64_t num_requests = requests.size();

		lock.unlock();

		// allocate without lock
		markable_tuple_container.reserve(num_requests);
		evictable_entries.reserve(num_excess);
	}

	// build LFU heap
	std::priority_queue<MarkableTuple, std::vector<MarkableTuple>,
			MarkableTupleLFUCompare> lfu_queue(
		MarkableTupleLFUCompare(), std::move(markable_tuple_container));

	{
		std::shared_lock lock(mutex);

		int64_t num_excess = cleanup_get_num_excess();

		for (auto& keyval : requests) {
			auto& entry = keyval.second;

			if (!entry->can_evict()) {
				continue;
			}

			if (entry->evict_counter.load()) {
				// candidate for marking for eviction
				lfu_queue.push(MarkableTuple { entry->num_hits, entry });
			} else {
				// candidate for eviction
				evictable_entries.push_back(EvictableTuple { entry->last_updated, entry });
			}
		}

		int64_t num = (double)num_excess / eviction_chance;
		for (int64_t i=0; i<num; i++) {
			if (lfu_queue.empty()) {
				break;
			}
			auto markable_tuple = lfu_queue.top();
			auto entry = markable_tuple.entry;
			entry->evict_counter = 0;

			lfu_queue.pop();

			LOG_TRACE("CodeCache: Marking %p (num_hits %llu) for eviction",
				entry, entry->num_hits.load());
		}
	}


	// remove oldest entries first
	std::sort(evictable_entries.begin(), evictable_entries.end(),
		[] (auto& a, auto& b) {
			return a.last_updated > b.last_updated;
		});

	int64_t num_evict = 0;
	int64_t num_evicted = 0;

	if (evictable_entries.size() > 0) {
		std::unique_lock lock(mutex);

		num_evict = cleanup_get_num_excess();
		for (auto& evictable_tuple : evictable_entries) {
			auto entry = evictable_tuple.entry;

			if (!entry->can_evict() || entry->evict_counter.load()) {
				continue;
			}

			LOG_TRACE("evict entry %p last_updated %lld",
				entry, evictable_tuple.last_updated);

			if (!erase_entry_nolock(entry)) {
				continue;
			}
			num_evicted++;

			if (num_evicted >= num_evict) {
				break;
			}
		}

		lock.unlock();
	} else {
		std::shared_lock lock(mutex);

		num_evict = cleanup_get_num_excess();
	}


	LOG_DEBUG("CodeCache: Evicted %lld/%lld entries (eviction_chance %f)",
		num_evicted, num_evicted, eviction_chance);

	cleanup_evict[cleanup_index] = num_evict;
	cleanup_evicted[cleanup_index] = num_evicted;
	cleanup_index = (cleanup_index + 1) % kCleanupWindowSize;
}


struct ReqEntryLRUCompare {
	bool operator()(ReqEntry* a, ReqEntry* b) {
		return score_entry(*a) > score_entry(*b);
	}

	// lowest score will get evicted
	uint64_t
	score_entry(const ReqEntry& a) const
	{
		return a.last_updated;
	}
};

struct ReqEntryLFUCompare {
	bool operator()(ReqEntry* a, ReqEntry* b) {
		return score_entry(*a) > score_entry(*b);
	}

	// lowest score will get evicted
	uint64_t
	score_entry(const ReqEntry& a) const
	{
		return a.num_hits;
	}
};


template<typename M, typename T, typename F>
int64_t cleanup_helper(M& requests, std::vector<ReqEntry*>&& container,
	const T& compare, int64_t target_num, const F& free_req_entry)
{
	if (target_num <= 0) {
		return 0;
	}

	std::priority_queue<ReqEntry*, std::vector<ReqEntry*>, T> entries(
		compare, std::move(container));

	for (auto& keyval : requests) {
		auto& entry = keyval.second;

		if (!entry->can_evict()) {
			continue;
		}

		entries.push(entry);
	}

	int64_t num_freed = 0;

	for (int64_t i=0; i<target_num; i++) {
		if (entries.empty()) {
			break;
		}

		auto entry = entries.top();
		ASSERT(entry);

		requests.erase(entry->signature);
		free_req_entry(entry, false);

		entries.pop();
		num_freed++;
	}

	return num_freed;
}

void
CodeCache::clear_nolock()
{
	for (auto& req_pair : requests) {
		free_req_entry(req_pair.second, true);
	}

	requests.clear();
	flavors.clear();
}

void
CodeCache::clear(size_t new_size)
{
	std::unique_lock lock(mutex);
	clear_nolock();

	if (!new_size) {
		new_size = capacity;
	}

	if (new_size != capacity) {
		// resize
		if (new_size > capacity) {
			requests.reserve(new_size);
		}
		capacity = new_size;
	}
}

#include "system/profiling.hpp"

void CodeFragmentStats::append(const std::string& full_signature, const std::string& op_signature,
		size_t query_id, size_t stage_id, const profiling::ProfileData* prof) {

	using ProfIdent = profiling::ProfileData::ProfIdent;
	uint64_t sum_time, num_in_rows, num_out_rows, num_calls;

	bool succ = true;
	succ &= prof->get_uint64(&sum_time, ProfIdent::kSumTime);
	succ &= prof->get_uint64(&num_in_rows, ProfIdent::kNumInputTuples);
	succ &= prof->get_uint64(&num_out_rows, ProfIdent::kNumOutputTuples);
	succ &= prof->get_uint64(&num_calls, ProfIdent::kNumCalls);
	ASSERT(succ);

	std::unique_lock lock(mutex);

	fast_update_rows.emplace_back(Row { query_id, stage_id,
		full_signature, op_signature, sum_time, num_in_rows,
		num_out_rows, num_calls });

	if (fast_update_rows.size() > kMaxUpdateRows) {
		integrate_update_rows_nolock();
	}
}

CodeFragmentStats::CodeFragmentStats() {
	fast_update_rows.reserve(kMaxUpdateRows);

	watchdog_active = true;
	watchdog_thread = std::thread(&CodeFragmentStats::watchdog_func, this);
}

CodeFragmentStats::~CodeFragmentStats() {
	{
		std::unique_lock lock(mutex);
		watchdog_active = false;
	}

	watchdog_thread.join();
}

void CodeFragmentStats::integrate_update_rows_nolock() {
	if (fast_update_rows.empty()) {
		return;
	}

	LOG_DEBUG("integrate_update_rows_nolock for %llu rows",
		fast_update_rows.size());

	for (auto& row : fast_update_rows) {
		double fill_rate = (double)row.num_in_rows / (double)row.num_calls;

		if (fill_rate < 100.0) {
			continue;
		}

		LOG_TRACE("%s: %f",
			row.full_signature.c_str(),
			(double)row.sum_time / (double)row.num_in_rows);
		auto& consol = consolidated_info[std::move(row.op_signature)];
		auto& prim = consol[std::move(row.full_signature)];

		prim.sum_time += row.sum_time;
		prim.num_in_rows += row.num_in_rows;
	}

	fast_update_rows.clear();
}

void CodeFragmentStats::watchdog_func() {
	while (1) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		std::unique_lock lock(mutex);
		if (!watchdog_active) {
			break;
		}
		integrate_update_rows_nolock();
	}
}

void
CodeCache::update_profiling(const CodeCacheItemPtr& item,
	const profiling::ProfileData* prof, QueryStage& stage)
{
	sys_context.get_code_fragment_stats().append(
		item->full_signature, item->op_signature, stage.get_stage_id(),
		stage.get_query().get_query_id(), prof);
}



void
CodeCacheItem::wait()
{
	ASSERT(future.valid());

	while (1) {
		auto status = future.wait_for(std::chrono::milliseconds(5));
		if (status == std::future_status::ready) {
			break;
		} else if (status == std::future_status::timeout) {
			scheduler::yield();
		} else {
			ASSERT(false && "Invalid future status");
		}
	}
}

bool
CodeCacheItem::has_error() const
{
	ASSERT(internal_entry);
	std::shared_lock lock(code_cache.mutex);
	return internal_entry->has_error;
}