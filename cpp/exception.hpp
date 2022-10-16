#pragma once

#include <string>

namespace proto {
	struct ErrorMessage;
}

struct Exception {
	static std::string get_stack_trace();

	const int32_t code;
	const std::string msg;
	const std::string stack;

	Exception(int32_t code, const std::string& msg,
		const std::string& stack = get_stack_trace());
	void set(proto::ErrorMessage& msg) const;
};

struct InvalidArgumentException : Exception {
	InvalidArgumentException(const std::string& msg) : Exception(1, msg) { }
};

struct OutOfMemoryException : Exception {
	OutOfMemoryException(const std::string& msg) : Exception(2, msg) { }
};

struct TerminatingException : Exception {
	TerminatingException(const std::string& msg) : Exception(3, msg) { }	
};

struct NotImplementedException : Exception {
	NotImplementedException(const std::string& msg) : Exception(4, msg) { }	
};


inline void require(bool cond, const std::string& err) {
	if (!cond) {
		throw InvalidArgumentException(err);
	}
}

template<typename T>
inline void wrap_cpp_exceptions(const T& func)
{
	try {
		func();
	} catch (std::bad_alloc& a) {
		throw OutOfMemoryException("Captured std::bad_alloc");
	}
}
