#include "profiling.hpp"
#include "system.hpp"
#include "scheduler.hpp"

#include <sstream>
#include <cstring>
#include <iomanip>

namespace profiling {

#define NEEDS_VIRTUAL_RDTSC

uint64_t
rdtsc(scheduler::Task* task)
{
#ifdef NEEDS_VIRTUAL_RDTSC
	if (!task) {
		task = scheduler::get_current_task();
	}
	auto clock = profiling::physical_rdtsc();

	if (!task) {
		return clock;
	}

	ASSERT(clock >= task->virtual_rdtsc_clock_run_start);
	return task->virtual_rdtsc_clock_acc + (clock - task->virtual_rdtsc_clock_run_start);
#else
	return profiling::physical_rdtsc();
#endif
}
} /* profiling */

using namespace profiling;

bool
ProfileData::check(bool fail) const
{
#define CHECK(NAME, TYPE, _) \
	if (ProfType::kUndefined != prof_types[ProfIdent::k##NAME]) { \
		if (fail) { \
			ASSERT(ProfType::kNative_##TYPE == prof_types[ProfIdent::k##NAME]); \
		} else if (ProfType::kNative_##TYPE != prof_types[ProfIdent::k##NAME]) { \
			return false; \
		} \
	}

	ProfileData_EXPAND(CHECK, _)

#undef CHECK

	return true;
}

void
ProfileData::print(std::ostream& s, const std::string& prefix,
	const std::string& newline) const
{
	check();

#define PRINT(NAME, TYPE, _) \
	if (ProfType::kUndefined != prof_types[ProfIdent::k##NAME]) {\
		auto ptr = (TYPE*)(prof_data[ProfIdent::k##NAME]); \
		if (ptr) { \
			s << prefix << #NAME << ": " << *ptr << newline; \
		} \
	}

	s << prefix << "Profile '" << name << "' {" << newline;
	ProfileData_EXPAND(PRINT, _)
	s << prefix << "}" << newline;
#undef PRINT
}

void
ProfileData::to_json(std::ostream& s)
{
	check();

#define PRINT(NAME, TYPE, _) \
	if (ProfType::kUndefined != prof_types[ProfIdent::k##NAME]) {\
		auto ptr = (TYPE*)(prof_data[ProfIdent::k##NAME]); \
		if (ptr) { \
			s << "\"" << #NAME << "\"" << ": " << *ptr << ",\n"; \
		} \
	}

	ProfileData_EXPAND(PRINT, _)

#undef PRINT
}

void
ProfileData::add_prof_item(ProfIdent ident, ProfType type, void* data)
{
	prof_data[ident] = data; 
	prof_types[ident] = type;
}

ProfileData::ProfileData(const std::string& name)
 : name(name)
{
	memset(&prof_data[0], 0, sizeof(prof_data));
	memset(&prof_types[0], 0, sizeof(prof_types));
}

bool
ProfileData::get_uint64(uint64_t* out, ProfIdent ident) const
{
	if (prof_types[ident] != kNative_uint64_t) {
		return false;
	}

	memcpy(out, prof_data[ident], sizeof(*out));
	return true;
}



std::string 
JsonUtils::escape(const std::string& s)
{
	// thanks to https://stackoverflow.com/questions/7724448/simple-json-string-escape-for-c
	std::ostringstream o;
	for (auto c = s.cbegin(); c != s.cend(); c++) {
		if (*c == '"' || *c == '\\' || ('\x00' <= *c && *c <= '\x1f')) {
			o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
		} else {
			o << *c;
		}
	}
	return o.str();
	}
