#pragma once

#include <memory>
#include <string>

#include "engine/voila/code_cache.hpp"
#include "engine/voila/global_jit.hpp"

struct BudgetUser;

namespace engine::voila {
struct FlavorSpec;
}

namespace llvm {
namespace orc {

struct KaleidoscopeJIT;

} /* orc */

struct Module;
struct LLVMContext;
struct DataLayout;
} /* llvm */

namespace engine {
namespace voila {

namespace llvmcompiler {

struct LlvmCompiler;

struct DataCodeMemoryContainer;

struct LlvmFragment : CodeFragment {
private:
	// JIT cpmiler to generate/optimize code
	std::unique_ptr<llvm::orc::KaleidoscopeJIT> llvm_jit;

	// Module for generating initial LLVM IR
	std::unique_ptr<llvm::Module> llvm_module;

	friend struct LlvmCompiler;
public:

	// Container for binary data to keep around
	std::unique_ptr<DataCodeMemoryContainer> memory_container;

	const bool enable_optimizations;
	const bool verify;
	const bool paranoid;

	BudgetUser* budget_user;
	const std::shared_ptr<engine::voila::FlavorSpec> flavor;
	
	LlvmFragment(const std::string& name, bool enable_optimizations,
		bool verify, bool paranoid,
		const std::shared_ptr<engine::voila::FlavorSpec>& flavor,
		excalibur::Context& sys_context,
		BudgetUser* budget_user);

	~LlvmFragment();
	void compile() override;

	llvm::LLVMContext& get_context();
	const llvm::DataLayout& get_data_layout() const;

	static std::unique_ptr<engine::voila::GlobalJit> make_global_jit();

};

} /* llvmcompiler */
} /* voila */
} /* engine */