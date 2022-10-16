#pragma once

#include <stdint.h>
#include <vector>
#include <string>
#include <iostream>

#define HAS_PROFILING

namespace scheduler {
struct Task;
} /* scheduler */

namespace profiling {


inline static
/* Inspired by https://github.com/google/benchmark/blob/master/src/cycleclock.h */
#if defined(__i386__)
uint64_t
_rdtsc()
{
	uint64_t r;
	__asm__ volatile (".byte 0x0f, 0x31" : "=A" (r));
	return r;
}
#elif defined(__x86_64__) || defined(__amd64__)
uint64_t
_rdtsc()
{
	uint32_t hi, lo;
	__asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
	return ((uint64_t)lo) | (((uint64_t)hi)<<32);
}
#elif defined(_MSC_VER)
#include <intrin.h>
uint64_t
_rdtsc()
{
	return __rdtsc();
}
#elif defined(__powerpc__) || defined(__ppc__)
uint64_t
_rdtsc()
{
	// This returns a time-base, which is not always precisely a cycle-count.
	int64_t tbl, tbu0, tbu1;
	asm("mftbu %0" : "=r"(tbu0));
	asm("mftb  %0" : "=r"(tbl));
	asm("mftbu %0" : "=r"(tbu1));
	tbl &= -(int64_t)(tbu0 == tbu1);
	// high 32 bits in tbu1; low 32 bits in tbl  (tbu0 is garbage)
	return (tbu1 << 32) | tbl;
}
#elif defined(__sparc__)
uint64_t
_rdtsc()
{
	int64_t tick;
	asm(".byte 0x83, 0x41, 0x00, 0x00");
	asm("mov   %%g1, %0" : "=r"(tick));
	return tick;
}
#elif defined(__ia64__)
uint64_t
_rdtsc()
{
	int64_t itc;
	asm("mov %0 = ar.itc" : "=r"(itc));
	return itc;
}
#elif defined(__aarch64__)
uint64_t
_rdtsc()
{
	// System timer of ARMv8 runs at a different frequency than the CPU's.
	// The frequency is fixed, typically in the range 1-50MHz.  It can be
	// read at CNTFRQ special register.  We assume the OS has set up
	// the virtual timer properly.
	int64_t virtual_timer_value;
	asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
	return virtual_timer_value;
}
#else
uint64_t
_rdtsc()
{
	0;
}
#endif


inline uint64_t physical_rdtsc()
{
	return _rdtsc();
}

uint64_t rdtsc(scheduler::Task* task = nullptr);

struct ProfileData {
	enum ProfType {
		kUndefined = 0,
		kNative_size_t = 1,
		kNative_int64_t = 2,
		kNative_uint64_t = 3,
	};

	enum ProfIdent {
#define ProfileData_EXPAND(F, ARG) \
	F(NumCalls, uint64_t, ARG) \
	F(NumInputTuples, uint64_t, ARG) \
	F(NumOutputTuples, uint64_t, ARG) \
	F(SumTime, uint64_t, ARG) \
	F(CompilationTimeUs, uint64_t, ARG) \
	F(BinarySizeBytes, uint64_t, ARG)

#define DEF(NAME, TYPE, ARG) k##NAME,

	ProfileData_EXPAND(DEF, _)

#undef DEF


		kMaxValue__
	};

	bool check(bool fail = true) const;

	void print(std::ostream& s, const std::string& prefix = "",
			const std::string& newline = "\n") const;
	virtual void to_json(std::ostream& o);

	std::string get_name() const { return name; }

	virtual ~ProfileData() = default;

	bool get_uint64(uint64_t* out, ProfIdent ident) const;

protected:
	const std::string name;
	void* prof_data[ProfIdent::kMaxValue__];
	ProfType prof_types[ProfIdent::kMaxValue__];

	void add_prof_item(ProfIdent ident, ProfType type, void* data);

	ProfileData(const std::string& name);
};

struct JsonUtils {
	static std::string escape(const std::string& s);
};

} /* profiling */
