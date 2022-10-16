#pragma once

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <memory>
#include <future>
#include <thread>
#include <shared_mutex>

#include "signature.hpp"

#include "system/memory.hpp"
#include "system/system.hpp"

namespace excalibur {
struct Context;
}

struct Query;
struct QueryStage;
struct QueryConfig;

namespace engine::voila {
struct FlavorSpec;
}

namespace profiling {
struct ProfileData;
}


namespace engine {
namespace voila {

struct CodeFragment {
	typedef void (*call_t)(void* item, void* context);

	CodeFragment(const std::string& name) : call(nullptr), name(name) {
	}

	call_t call;
	const std::string name;
	size_t binary_size_bytes = 0;

	std::string ir_code;

	// set from CodeCache
	double compilation_time_us = 0.0;

	virtual void compile() = 0;
	virtual ~CodeFragment() = default;
};

typedef std::shared_ptr<CodeFragment> CodeFragmentPtr;

struct CodeCache;
struct ReqEntry;

struct CodeCacheItem {
	std::shared_future<CodeFragmentPtr> future;
	ReqEntry* internal_entry;
	CodeCache& code_cache;
	const Signature full_signature;
	const Signature op_signature;

	CodeCacheItem(const std::shared_future<CodeFragmentPtr>& future, ReqEntry* entry,
		CodeCache& code_cache, const Signature& full_signature,
		const Signature& op_signature)
	 : future(future), internal_entry(entry), code_cache(code_cache),
		full_signature(full_signature), op_signature(op_signature)
	{
	}

	bool has_error() const;

	void wait();
};

struct ReqPool;

typedef std::unique_ptr<CodeCacheItem> CodeCacheItemPtr;

struct CodeCache {
	typedef Signature key_t;
	mutable std::shared_mutex mutex;
private:

	// Requests indexed by full_signature
	std::unordered_map<key_t, ReqEntry*> requests;

	// Requests indexed by op signature
	std::unordered_map<key_t, std::vector<ReqEntry*>> flavors;

	memory::Context mem_context;
	excalibur::Context& sys_context;

	std::unique_ptr<ReqPool> request_pool;

	uint64_t id_counter = 0;

	int64_t capacity = 512*1024;

	std::atomic<size_t> prof_num_requests;
	std::atomic<size_t> prof_num_hits;

	size_t prof_num_miss;
	size_t prof_num_compiled;

	std::atomic<uint64_t> time_stamp;
	double margin_for_new_fragments = 0.1;

	bool cleanup_enabled = true;
	bool hack_all_unique = false;

	void clear_nolock();

	ReqEntry* new_req_entry(const Signature& full_signature,
		const Signature& op_signature, const std::shared_ptr<FlavorSpec>& flavor_spec,
		uint64_t this_time_stamp);
	void free_req_entry(ReqEntry* entry, bool do_free);

	bool watchdog_active;
	std::thread watchdog_thread;

	void watchdog_func();
	void trigger_cleanup();

	CodeCacheItemPtr _get(const std::shared_ptr<CompileRequest>& req,
		Query* query, uint64_t flags);
	CodeCacheItemPtr request_compilation(const std::string& dbg_name,
		ReqEntry* entry, const std::string& signature, const std::string& op_signature,
		uint64_t flags, const std::shared_ptr<CompileRequest>& req, Query* query);

	int64_t cleanup_get_num_excess() const {
		const int64_t num_requests = requests.size();
		if (num_requests <= capacity) {
			return 0;
		}
		return (double)num_requests
			- (1.0 - margin_for_new_fragments) * (double)capacity;
	}

	static constexpr size_t kCleanupWindowSize = 4;
	uint64_t cleanup_evict[kCleanupWindowSize];
	uint64_t cleanup_evicted[kCleanupWindowSize];
	uint64_t cleanup_index = 0;

	uint64_t cleanup_iteration = 0;

	void remove_entry_from_tables_nolock(ReqEntry* entry);
	bool erase_entry_nolock(ReqEntry* entry, bool force = false);
	std::unordered_set<ReqEntry*> erroneous_requests;

public:
	void entry_has_error(ReqEntry* entry);

	CodeCache(const CodeCache&) = delete;
	CodeCache& operator=(const CodeCache&) = delete;

	CodeCache(excalibur::Context& sys_context);
	~CodeCache();
	void clear(size_t new_size = 0);

	void release(CodeCacheItemPtr&& f);

	void wait_until_quiet();

	void update_profiling(const CodeCacheItemPtr& item,
		const profiling::ProfileData* prof, QueryStage& stage);

	static const uint64_t kGetFlagSimplePrimitive = 1;
	static const uint64_t kGetFlagMatchAnyFlavor = 2;
	static const uint64_t kIncrementalCompilation = 4;
	CodeCacheItemPtr get(const std::shared_ptr<CompileRequest>& req,
		Query* query, uint64_t flags = 0);

	void reset_stats() {
		std::unique_lock<std::shared_mutex> lock(mutex);

		prof_num_requests = 0;
		prof_num_compiled = 0;

		for (size_t i=0; i<kCleanupWindowSize; i++) {
			cleanup_evict[i] = 0;
			cleanup_evicted[i] = 0;
		}
	}

	void set_cleanup(bool enabled) {
		std::unique_lock<std::shared_mutex> lock(mutex);

		cleanup_enabled = enabled;
	}

	void set_all_unique(bool enabled) {
		std::unique_lock<std::shared_mutex> lock(mutex);

		hack_all_unique = enabled;
	}

	struct Stats {
		size_t num_requests;
		size_t num_compiled;
		size_t num_cached_requests;
		size_t num_cached_flavors;
	};

	Stats get_stats() const {
		std::unique_lock<std::shared_mutex> lock(mutex);

		return Stats { prof_num_requests.load(), prof_num_compiled,
			requests.size(), flavors.size() };
	}
};

struct CodeFragmentStats {
	void append(const std::string& full_signature, const std::string& op_signature,
			size_t query_id, size_t stage_id, const profiling::ProfileData* prof);

	CodeFragmentStats(const CodeFragmentStats&) = delete;
	CodeFragmentStats& operator=(const CodeFragmentStats&) = delete;

	CodeFragmentStats();

	~CodeFragmentStats();

	const static size_t kMaxUpdateRows = 128*1024;

private:
	void integrate_update_rows_nolock();

	std::mutex mutex;

	struct Row {
		uint64_t query_id;
		uint64_t stage_id;

		Signature full_signature;
		Signature op_signature;

		uint64_t sum_time;
		uint64_t num_in_rows;
		uint64_t num_out_rows;
		uint64_t num_calls;
	};
	std::vector<Row> fast_update_rows;

	bool watchdog_active;
	std::thread watchdog_thread;

	void watchdog_func();

	struct ConsolidatedInfo {
		uint64_t sum_time = 0;
		uint64_t num_in_rows = 0;
	};
	std::unordered_map<Signature,
		std::unordered_map<Signature, ConsolidatedInfo>>
			consolidated_info;

};

} /* voila */
} /* engine */