#pragma once
#include <memory>

#include "system/system.hpp"
#include "engine/voila/global_jit.hpp"

struct BudgetUser;

namespace engine::voila {
struct CompileRequest;
struct CodeFragment;

typedef std::shared_ptr<CodeFragment> CodeFragmentPtr;
}


namespace engine {
namespace voila {
namespace llvmcompiler {

struct CompilerException : Exception {
	CompilerException(const std::string& msg) : Exception(msg) {}
};


CodeFragmentPtr compile_fragment(const std::string name,
	CompileRequest& req, const std::string dbg_name,
	BudgetUser& budget_user);

std::unique_ptr<engine::voila::GlobalJit> make_global_jit();

} /* llvmcompiler */
} /* voila */
} /* engine */


