#include "system.hpp"
#include <stdio.h>
#include <cstring>
#include <cassert>
#include <mutex>
#include <atomic>
#include <thread>

static ISystem* g_system;
LogLevel g_dynamic_min_level = INFO;

#include "scheduler.hpp"
#include "build.hpp"

static const size_t g_backtrace_buffer_size = 128;
static void* g_backtrace_buffer[g_backtrace_buffer_size];
static std::mutex g_backtrace_buffer_mutex;


static std::atomic<uint64_t> g_log_sequence_num;
static LogContext g_log_context_segfault;
static std::mutex g_log_mutex;
static int g_log_init;

template<size_t kBufferSize, size_t kMaxMessage>
struct LogReplay {
	struct Entry {
		const char* file;
		int line;
		int level;
		uint64_t seqnum;

		char msg[128];
	};

	Entry arr[kBufferSize];
	size_t fill;

	Entry* begin_write() {
		Entry* ptr = &arr[fill % kBufferSize];
		fill++;
		return ptr;
	}

	void end_write(Entry* e) {
		(void)e;
	}

	template<typename T>
	void iterate(const T& fun, size_t skip = 0) {
		if (!fill) {
			return;
		}

		// find min sequence number
		size_t i=0;
		uint64_t min_num = arr[i].seqnum;
		uint64_t min_idx = i;
		for (i=1; i<fill; i++) {
			if (arr[i].seqnum < min_num) {
				min_num = arr[i].seqnum;
				min_idx = i;
			}
		}

		for (size_t k=min_idx + skip; k<fill; k++) {
			i = k % kBufferSize;
			fun(arr[i]);
		}
	}

	void reset() {
		fill = 0;
	}

	LogReplay() {
		reset();
	}
};

static LogReplay<1024, 64> g_log_replay;
static bool g_log_has_replay;


static int log_will_log(int level, LogContext* ctx)
{
	if (level <= TRACE) {
		return 0;
	}
	if (g_system && level < g_system->min_log_level) {
		return 0;
	}

	return 1;
}

extern "C" int
__log_will_log(int level, LogContext* context)
{
	return log_will_log(level, context);	
}

extern "C" int
__log_init_context_will_log(int level, LogContext* ctx)
{
	if (!log_will_log(level, ctx)) {
		return 0;
	}

	ctx->sequence_num = ++g_log_sequence_num;
	return 1;
}

#include <signal.h>

static void
segfault_sigaction(int signal, siginfo_t* si, void* arg)
{
	if (__log_init_context_will_log(FATAL, &g_log_context_segfault)) {
		__log_message(FATAL, &g_log_context_segfault, "Caught segfault at address %p", si->si_addr);
		__log_backtrace(WARN, &g_log_context_segfault);
	}


	exit(1);
}

static void install_signal_handlers()
{
	struct sigaction sa;

	memset(&sa, 0, sizeof(struct sigaction));
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = segfault_sigaction;
	sa.sa_flags = SA_SIGINFO;

	sigaction(SIGSEGV, &sa, NULL);
}

#ifdef HAS_LIBUNWIND
#include <libunwind.h>
#endif

#include <cxxabi.h>
#include <boost/core/demangle.hpp>

static char*
demangle(char* symbol)
{
	return symbol;
	int status;

	return abi::__cxa_demangle(symbol, NULL, NULL, &status);
	// return symbol;
}

extern "C" void
__log_backtrace(int level, LogContext* context)
{
	std::lock_guard<std::mutex> lg(g_backtrace_buffer_mutex);

#ifdef HAS_LIBUNWIND
	unw_cursor_t cursor;
	unw_context_t ctx;

	unw_getcontext(&ctx);
	unw_init_local(&cursor, &ctx);

	int n=0;
	while (unw_step(&cursor)) {
		unw_word_t ip, sp, off;

		unw_get_reg(&cursor, UNW_REG_IP, &ip);
		unw_get_reg(&cursor, UNW_REG_SP, &sp);

		char symbol[256] = {"<unknown>"};
		char *name = symbol;

		if (!unw_get_proc_name(&cursor, symbol, sizeof(symbol), &off)) {
			name = demangle(symbol);
		}

		__log_message(level, context, "  #%-2d 0x%016" PRIxPTR " sp=0x%016" PRIxPTR " %s + 0x%" PRIxPTR,
			++n,
			static_cast<uintptr_t>(ip),
			static_cast<uintptr_t>(sp),
			name,
			static_cast<uintptr_t>(off));

		if (name != symbol) {
			free(name);
		}
	}
#else

#ifdef HAS_LIBBACKTRACE
	int num = backtrace(g_backtrace_buffer, g_backtrace_buffer_size);
	__log_message(level, context, "Backtrace with %d frames:", num);

	char** strings = backtrace_symbols(g_backtrace_buffer, num);
	if (strings == NULL) {
		__log_message(level, context, "Could not allocate backtrace symbol strings");
		return;
	}

	for (int i=0; i<num; i++) {
		auto& symbol = strings[i];

		boost::core::scoped_demangled_name demangler(symbol);

		__log_message(level, context, "  #%-2d %s",
			i+1, demangler.get() ? demangler.get() : symbol);
	}

	free(strings);
#endif

#endif
}

const char*
strip_path_from_filename(const char* filename) {
	const char* prev = filename;
	const char* path = "/";
	size_t path_len = 1;

	while (prev) {
		const char* n = strstr(prev, path);
		if (!n) {
			return prev;
		}
		prev = n+path_len;
	}
	return nullptr;
}

extern "C" void
__log_message(int level, LogContext* context, const char* format, ...)
{
	if (!log_will_log(level, context)) {
		return;
	}

	std::lock_guard<std::mutex> lg(g_log_mutex);

	if (!g_log_init) {
		install_signal_handlers();
		g_log_init = 1;
	}

	// replay FATALs
	if (level == FATAL && g_log_has_replay) {
		printf("FATAL!\n");
		g_log_replay.iterate([] (auto entry) {
			fprintf(stderr, "[REPLAY] %llu %s:%d %s\n",
				(long long unsigned)entry.seqnum,
				strip_path_from_filename(entry.file),
				entry.line, entry.msg);
		});

		g_log_replay.reset();
	}

	va_list arglist;

	auto out = stderr;

	switch (level) {
	case FATAL:
		fprintf(out, "\033[1;31m");
		fprintf(out, "[FATAL] ");
		fprintf(out, "\033[0m");
		break;

	case ERROR:
		fprintf(out, "\033[1;31m");
		fprintf(out, "[ERROR] ");
		fprintf(out, "\033[0m");
		break;

	case WARN:
		fprintf(out, "\033[1;93m");
		fprintf(out, "[WARN]  ");
		fprintf(out, "\033[0m");
		break;		
	case INFO:
		fprintf(out, "[INFO]  ");
		break;
	case DEBUG:
		fprintf(out, "[DEBUG] ");
		break;
	case TRACE:
		fprintf(out, "[TRACE] ");
		break;

	default:
		fprintf(out, "");
		break;
	}

	auto wid = scheduler::get_current_worker_id();
	if (wid == -1) {
		fprintf(out, "M ");
	} else {
		// assert(wid >= 0);
		fprintf(out, "W%2lld ", (long long int)wid);
	}
	fprintf(out, "S%llu ", (long long unsigned)context->sequence_num);

	va_start(arglist, format);
	vfprintf(out, format, arglist);
	va_end(arglist);
	fprintf(out, "\n");

	if (g_log_has_replay) {
		auto entry = g_log_replay.begin_write();
		entry->file = context->file;
		entry->line = context->line;
		entry->level = level;
		entry->seqnum = context->sequence_num;
		va_start(arglist, format);

		memset(entry->msg, 0, sizeof(entry->msg));
		int num = vsnprintf(entry->msg, sizeof(entry->msg)-1, format, arglist);
		if (num < 0) {
			fprintf(stderr, "error during vsfnprintf()\n");
			return;
		}
		if (num > 0) {
			entry->msg[std::min(num, (int)sizeof(entry->msg)-1)] = 0;
		}
		va_end(arglist);

		g_log_replay.end_write(entry);
	}
}


extern "C" ISystem* system_get()
{
	return g_system;
}

extern "C" void system_set(ISystem* system)
{
	g_system = system;
}

extern "C" void
__assert_break(LogContext* context, const char* assert)
{
	int* x = nullptr;
	__log_message(FATAL, context, "Assertion '%s' failed at %s:%d",
		assert, context->file, context->line);

	__log_backtrace(WARN, context);
	*x = 1;
}