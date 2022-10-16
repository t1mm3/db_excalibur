#include "llvm_interface.hpp"
#include "llvm_fragment.hpp"
#include "llvm_compiler.hpp"

#include "engine/budget.hpp"

#include "engine/voila/compile_request.hpp"
#include "engine/voila/code_cache.hpp"

using namespace engine;
using namespace voila;
using namespace llvmcompiler;

CodeFragmentPtr
engine::voila::llvmcompiler::compile_fragment(const std::string name,
	CompileRequest& req, const std::string dbg_name,
	BudgetUser& budget_user)
{
	auto fragment = std::make_shared<llvmcompiler::LlvmFragment>(name,
		req.enable_optimizations, req.verify, req.paranoid, req.flavor,
		req.sys_context, &budget_user);

	LlvmCompiler compiler(name, *fragment, req, dbg_name);
	compiler();

	budget_user.ensure_sufficient(kDefaultBudget, "llvmcompiler");

	fragment->compile();
	return std::dynamic_pointer_cast<CodeFragment>(std::move(fragment));
}

std::unique_ptr<engine::voila::GlobalJit>
engine::voila::llvmcompiler::make_global_jit()
{
	return LlvmFragment::make_global_jit();
}