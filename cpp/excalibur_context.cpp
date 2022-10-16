#include "excalibur_context.hpp"
#include "engine/voila/code_cache.hpp"
#include "engine/voila/llvmcompiler/llvm_interface.hpp"
#include "engine/voila/global_jit.hpp"
#include "engine/adaptive/brain.hpp"

using namespace excalibur;

Context::Context()
{
	code_cache = std::make_unique<engine::voila::CodeCache>(*this);
	code_fragment_stats = std::make_unique<engine::voila::CodeFragmentStats>();

	llvmcompiler_global_jit = engine::voila::llvmcompiler::make_global_jit();

	brain = std::make_unique<engine::adaptive::Brain>(*this);
}

Context::~Context()
{

}