#pragma once

#include <stdint.h>
#include <string>
#include <memory>

namespace engine {

struct Stream;

namespace lolepop {
struct Lolepop;
}

struct StreamPass {
	Stream& stream;

private:
	const std::string _log_prefix;
	int64_t round = 0;

protected:
	int64_t num_applied = 0;

	const char* log_prefix() const {
		return _log_prefix.c_str();
	}

	StreamPass(Stream& stream, const std::string& log_prefix)
	 : stream(stream), _log_prefix(log_prefix) {

	}

#if 0
	virtual bool run();
#endif
	virtual void lolepop(std::shared_ptr<lolepop::Lolepop>& op) {
	}

public:
#if 0
	void operator()();
#endif
};

} /* engine */