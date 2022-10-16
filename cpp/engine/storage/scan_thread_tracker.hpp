#pragma once

#include "scan_interface.hpp"
#include "system/system.hpp"
#include <unordered_map>

#include "system/profiling.hpp"

namespace engine {
namespace storage {

struct ScanTrackerInfo {
	bool init = false;
	uint64_t last_call = 0;

	uint64_t total_time_spent_outside = 0;
	uint64_t total_time_superclass = 0;
	uint64_t num_calls = 0;

	size_t tuples_returned = 0;

	static ScanTrackerInfo* get(ScanThread* t) {
		return t->scan_tracker_info;
	}

	static void set(ScanThread* t, ScanTrackerInfo* info) {
		t->scan_tracker_info = info;
	}
};

template<typename BASE>
struct ScanThreadTracker : BASE {
	size_t next(size_t& offset, ScanThread* thread, size_t morsel) final {
		const uint64_t call_clock_begin = profiling::rdtsc();

		auto info = ScanTrackerInfo::get(thread);
		ASSERT(info);

		if (info->init) {
			// took 'diff' cycles until we called next again
			const uint64_t diff = call_clock_begin - info->last_call;
			info->total_time_spent_outside += diff;
			LOG_TRACE("Scan: consumption speed %f cyc/tup, %f tuples/cyc, subcall %f cyc/tup",
				(double)info->total_time_spent_outside / (double)info->tuples_returned,
				(double)info->tuples_returned / (double)info->total_time_spent_outside,
				(double)info->total_time_superclass / (double)info->tuples_returned);

		}

		const uint64_t call_subclass = profiling::rdtsc();
		size_t num = BASE::next(offset, thread, morsel);
		const uint64_t call_clock_end = profiling::rdtsc();

		info->total_time_superclass += call_clock_end - call_subclass;
		info->last_call = call_clock_end;
		info->tuples_returned += num;
		info->init = true;
		info->num_calls++;
		return num;
	}

	void register_scan(ScanThread* thread) final {
		BASE::register_scan(thread);

		ASSERT(!ScanTrackerInfo::get(thread));

		ScanTrackerInfo::set(thread, new ScanTrackerInfo());
	}

	void unregister_scan(ScanThread* thread) final {
		auto info = ScanTrackerInfo::get(thread);
		ASSERT(info);
		delete info;
		info = nullptr;

		BASE::unregister_scan(thread);

	}

	ScanThreadTracker(const ScanInterface::Args& args)
	 : BASE(args)
	{
	}
};

} /* storage */
} /* engine */
