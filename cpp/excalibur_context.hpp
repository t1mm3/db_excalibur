#pragma once

#include <string>
#include <memory>

// Prototypes
namespace engine {
namespace voila {
struct CodeCache;
struct CodeFragmentStats;
struct GlobalJit;
}

namespace adaptive {
struct Brain;
}
}


// API
namespace excalibur {

struct Exception {
	Exception(const std::string& msg) : m_msg(msg) {
	}

	virtual const char* what() const throw () {
		return m_msg.c_str();
	}

private:
	const std::string m_msg;
};

struct Context {
	// not copyable
	Context(const Context&) = delete;
	Context& operator=(const Context&) = delete;

	Context();
	~Context();


	// INTERNAL:


	auto& get_code_cache() {
		return *code_cache;
	}

	auto& get_code_fragment_stats() {
		return *code_fragment_stats;
	}

	auto& get_brain() {
		return *brain;
	}

	std::unique_ptr<engine::voila::GlobalJit> llvmcompiler_global_jit;

private:
	std::unique_ptr<engine::voila::CodeCache> code_cache;
	std::unique_ptr<engine::voila::CodeFragmentStats> code_fragment_stats;

	std::unique_ptr<engine::adaptive::Brain> brain;
};

struct Transaction {

};

struct Query {

};

} /* excalibur */
