#pragma once

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string>

#define LIKELY(x)		(!__builtin_expect(!(x), 0))
#define UNLIKELY(x)		__builtin_expect(!!(x), 0)
#define FORCE_INLINE __attribute__((always_inline))

struct IObject {

};

struct Exception {
	Exception(const std::string& msg) : msg(msg) {
	}

	const std::string& get_message() const {
		return msg;
	}
private:
	const std::string msg;
};

struct ISystem {
	int min_log_level = 0;
};

enum LogLevel {
	TRACE = -1,
	DEBUG = 0,
	INFO = 1,
	WARN = 2,
	ERROR = 3,
	FATAL = 4,
};

struct LogContext {
	IObject* obj;
	const char* file;
	int line;

	uint64_t sequence_num;
};

#define RESTRICT __restrict__
#define NOINLINE __attribute__ ((noinline))

#ifdef IS_DEBUG_BUILD
	#define DBG_ASSERT(x) ASSERT(x)
#else
	#ifdef IS_RELEASE_BUILD
		#define DBG_ASSERT(x)
	#else
		#error "Neither Debug nor Release build"
	#endif
#endif

#define ASSERT(x) if (UNLIKELY(!(x))) { \
		LogContext __ctx {nullptr, __FILE__, __LINE__, 0}; \
		__assert_break(&__ctx, #x); \
	}

#define PRECONDITION(x) ASSERT(x)

inline const char* __msg_from_str(const char* s)
{
	return s;
}

inline const char* __msg_from_str(const std::string& s)
{
	return s.c_str();
}

#define THROW_MSG(x, msg) { \
		LOG_ERROR("Throwing " #x " with message '%s'", \
			__msg_from_str(msg)); \
		throw x(msg); \
	}

#define THROW(x, ...) { \
		LOG_ERROR("Throwing " #x " at %s:%d", __FILE__, __LINE__); \
		throw x(__VA_ARGS__); \
	}

#define RETHROW() throw


#define _LOG_STATIC_MIN_LEVEL DEBUG

extern LogLevel g_dynamic_min_level;

#define OBJ_LOG_ON_LEVEL(object, level, ...) { \
	LogContext __ctx {object, __FILE__, __LINE__, 0}; \
	if (((level) >= _LOG_STATIC_MIN_LEVEL) \
			&& ((level) >= g_dynamic_min_level) \
			&& __log_init_context_will_log(level, &__ctx)) { \
		__log_message(level, &__ctx, __VA_ARGS__); \
		if (level >= FATAL) { __log_backtrace(WARN, &__ctx); } \
	} \
}

#define LOG_FATAL(...) OBJ_LOG_ON_LEVEL(nullptr, FATAL, __VA_ARGS__)
#define LOG_ERROR(...) OBJ_LOG_ON_LEVEL(nullptr, ERROR, __VA_ARGS__)
#define LOG_DEBUG(...) OBJ_LOG_ON_LEVEL(nullptr, DEBUG, __VA_ARGS__)
#define LOG_WARN(...) OBJ_LOG_ON_LEVEL(nullptr, WARN, __VA_ARGS__)
#define LOG_INFO(...) OBJ_LOG_ON_LEVEL(nullptr, INFO, __VA_ARGS__)
#define LOG_TRACE(...) OBJ_LOG_ON_LEVEL(nullptr, TRACE, __VA_ARGS__)
#define LOG_MESSAGE(level, ...) OBJ_LOG_ON_LEVEL(nullptr, level, __VA_ARGS__)
#define LOG_WILL_LOG(level) (((level) >= _LOG_STATIC_MIN_LEVEL && (level) >= g_dynamic_min_level) ? \
	__log_will_log_cpp(level, LogContext{nullptr, __FILE__, __LINE__, 0}) : false)

#define GLOB_LOG_ERROR(...) OBJ_LOG_ON_LEVEL(nullptr, ERROR, __VA_ARGS__)
#define GLOB_LOG_DEBUG(...) OBJ_LOG_ON_LEVEL(nullptr, DEBUG, __VA_ARGS__)
#define GLOB_LOG_INFO(...) OBJ_LOG_ON_LEVEL(nullptr, INFO, __VA_ARGS__)



extern "C" void __log_message(int level, LogContext* context, const char* format, ...);
extern "C" void __log_backtrace(int level, LogContext* context);
extern "C" int __log_init_context_will_log(int level, LogContext* context);
extern "C" int __log_will_log(int level, LogContext* context);

inline bool __log_will_log_cpp(int level, const LogContext& ctx) {
	LogContext c(ctx);
	return !!__log_will_log(level, &c);
}

extern "C" void __assert_break(LogContext* context, const char* assert);
 
extern "C" ISystem* system_get();
extern "C" void system_set(ISystem* system);

extern "C" uint64_t rdtsc();