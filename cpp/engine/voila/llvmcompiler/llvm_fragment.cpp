#include "llvm_fragment.hpp"
#include "excalibur_context.hpp"
#include "system/system.hpp"
#include "system/memory.hpp"
#include "system/scheduler.hpp"
#include "engine/budget.hpp"
#include "engine/voila/flavor.hpp"
#include "llvm_interface.hpp"

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include "llvm/ExecutionEngine/Orc/ObjectTransformLayer.h"
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>

#include <llvm/IR/Mangler.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/LegacyPassManagers.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>

#include <llvm/Transforms/IPO/Inliner.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <llvm/Transforms/Vectorize.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>
#include <llvm/Transforms/Vectorize/SLPVectorizer.h>

#include <llvm/Support/DynamicLibrary.h>

#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Target/TargetOptions.h>
#include "llvm/IR/Verifier.h"

#include "llvm/ADT/StringRef.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Analysis/CallGraph.h"
#include "llvm/Analysis/CallGraphSCCPass.h"
#include "llvm/Analysis/LoopPass.h"
#include "llvm/Analysis/RegionPass.h"
#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/AsmParser/Parser.h"
#include "llvm/CodeGen/CommandFlags.h"
#include "llvm/CodeGen/TargetPassConfig.h"
#include "llvm/Config/llvm-config.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/DebugInfo.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LLVMRemarkStreamer.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LegacyPassNameParser.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/InitializePasses.h"
#include "llvm/LinkAllIR.h"
#include "llvm/LinkAllPasses.h"
#include "llvm/MC/SubtargetFeature.h"
#include "llvm/Support/Debug.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/PluginLoader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/SystemUtils.h"
#include "llvm/Support/TargetRegistry.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/ToolOutputFile.h"
#include "llvm/Support/YAMLTraits.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Coroutines.h"
#include "llvm/Transforms/IPO/AlwaysInliner.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/IPO/WholeProgramDevirt.h"
#include "llvm/Transforms/Utils/Cloning.h"

#include "llvm/Transforms/Utils/Debugify.h"

#include "llvm_build_utils.hpp"

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#ifdef HAS_CAPSTONE
#include <capstone/capstone.h>
#endif

#include <memory>

using CompilerException = engine::voila::llvmcompiler::CompilerException;
using LlvmFragment = engine::voila::llvmcompiler::LlvmFragment;

using namespace llvm;
using namespace engine;
using namespace voila;
using namespace llvmcompiler;

namespace engine::voila::llvmcompiler {
struct GlobalJit : engine::voila::GlobalJit {
	GlobalJit() {
		llvm::InitializeNativeTarget();
		llvm::InitializeNativeTargetAsmPrinter();

		// Initialize passes
		PassRegistry &Registry = *PassRegistry::getPassRegistry();
		initializeCore(Registry);
		initializeCoroutines(Registry);
		initializeScalarOpts(Registry);
		initializeObjCARCOpts(Registry);
		initializeVectorization(Registry);
		initializeIPO(Registry);
		initializeAnalysis(Registry);
		initializeTransformUtils(Registry);
		initializeInstCombine(Registry);
		initializeAggressiveInstCombine(Registry);
		initializeInstrumentation(Registry);
		initializeTarget(Registry);
		// For codegen passes, only passes that do IR to IR transformation are
		// supported.
		initializeExpandMemCmpPassPass(Registry);
		// initializeScalarizeMaskedMemIntrinLegacyPassPass(Registry);
		initializeCodeGenPreparePass(Registry);
		initializeAtomicExpandPass(Registry);
		initializeRewriteSymbolsLegacyPassPass(Registry);
		initializeWinEHPreparePass(Registry);
		// initializeDwarfEHPrepareLegacyPassPass(Registry);
		initializeSafeStackLegacyPassPass(Registry);
		initializeSjLjEHPreparePass(Registry);
		initializePreISelIntrinsicLoweringLegacyPassPass(Registry);
		initializeGlobalMergePass(Registry);
		initializeIndirectBrExpandPassPass(Registry);
		initializeInterleavedLoadCombinePass(Registry);
		initializeInterleavedAccessPass(Registry);
		initializeEntryExitInstrumenterPass(Registry);
		initializePostInlineEntryExitInstrumenterPass(Registry);
		initializeUnreachableBlockElimLegacyPassPass(Registry);
		initializeExpandReductionsPass(Registry);
		initializeWasmEHPreparePass(Registry);
		initializeWriteBitcodePassPass(Registry);
		initializeHardwareLoopsPass(Registry);
		initializeTypePromotionPass(Registry);
	}
};	
}


namespace engine::voila::llvmcompiler {

struct DataCodeMemoryContainer {
	SectionMemoryManager memory_manager;
};

} /* engine::voila::llvmcompiler */



struct LlvmFragmentMemoryManager : public SectionMemoryManager {
	engine::voila::llvmcompiler::DataCodeMemoryContainer* memory_container;

	LlvmFragmentMemoryManager(LlvmFragment& fragment)
	 : fragment(fragment)
	{
		memory_container = fragment.memory_container.get();
	}

	uint8_t*
	allocateCodeSection(uintptr_t size, unsigned align, unsigned secId,
			StringRef secName) final
	{
		LOG_DEBUG("AllocateCodeSection(size=%llu, align=%u, sectionId=%u, sectionName='%s')",
			size, align, secId, secName.str().c_str());

		fragment.binary_size_bytes += size;

		if (memory_container) {
			return memory_container->memory_manager.allocateCodeSection(size, align, secId, secName);
		} else {
			return SectionMemoryManager::allocateCodeSection(size, align, secId, secName);
		}
	}

	uint8_t*
	allocateDataSection(uintptr_t size, unsigned align, unsigned secId,
			StringRef secName, bool isReadOnly) final
	{
		LOG_DEBUG("allocateDataSection(size=%llu, align=%u, sectionId=%u, sectionName='%s', isReadOnly=%d)",
			size, align, secId, secName.str().c_str(), (int)isReadOnly);

		fragment.binary_size_bytes += size;

		if (memory_container) {
			return memory_container->memory_manager.allocateDataSection(size, align, secId, secName, isReadOnly);
		} else {
			return SectionMemoryManager::allocateDataSection(size, align, secId, secName, isReadOnly);
		}
	}

	bool
	finalizeMemory(std::string *err = nullptr) final
	{
		LOG_DEBUG("finalizeMemory(%s%s)",
			err ? "errorMsg=" : "no-error", err ? err->c_str() : "");
		if (memory_container) {
			return memory_container->memory_manager.finalizeMemory(err);
		} else {
			return SectionMemoryManager::finalizeMemory(err);
		}
	}

	void
	invalidateInstructionCache() final {
		LOG_DEBUG("invalidateInstructionCache");
		if (memory_container) {
			memory_container->memory_manager.invalidateInstructionCache();
		} else {
			SectionMemoryManager::invalidateInstructionCache();
		}
	}

	virtual ~LlvmFragmentMemoryManager() = default;

private:
	LlvmFragment& fragment;
};

static void
disassemble(const char* data, size_t size)
{
	if (!LOG_WILL_LOG(DEBUG)) {
		return;
	}

	return;

#if defined(__x86_64__) || defined(__amd64__)
#ifdef HAS_CAPSTONE
	csh handle;
	cs_insn *insn;
	size_t count;

	LOG_DEBUG("Disassemling section");

	if (cs_open(CS_ARCH_X86, CS_MODE_64, &handle) != CS_ERR_OK) {
		LOG_ERROR("Failed to open capstone disassembler");
	}
	count = cs_disasm(handle, (const uint8_t*)data, size, 0, 0, &insn);
	if (count > 0) {
		size_t j;
		for (j = 0; j < count; j++) {
			LOG_DEBUG("0x%" PRIx64 ":\t%s\t\t%s",
				insn[j].address, insn[j].mnemonic, insn[j].op_str);
		}

		cs_free(insn, count);
	} else {
		LOG_ERROR("Failed to disassemble");
	}
	cs_close(&handle);
#endif
#endif
}


// We hijack an LLVM pass to inject scheduler pre-emption
// and budget checking.
// This relies on the BudgetUser being stored inside the
// current Task
struct PreemptModulePass : llvm::ModulePass {
	static char ID;

	PreemptModulePass() : ModulePass(ID) {}

	virtual bool runOnModule(Module &M) {
		LOG_DEBUG("PreemptModulePass");
		auto task = scheduler::get_current_task();
		if (!task) {
			return false;
		}

		if (task->budget_user) {
			task->budget_user->ensure_sufficient(kDefaultBudget, "LLVM");
		}
		scheduler::yield(task);
		return false;
	}
};

char PreemptModulePass::ID = 0;
static RegisterPass<PreemptModulePass> g_preempt_module_pass("prempt-module-pass", "Prempt LLVM execution");

#define REGISTER_PASS(NAME, POINT) static llvm::RegisterStandardPasses NAME( \
	llvm::PassManagerBuilder::POINT, \
	[] (const llvm::PassManagerBuilder &Builder, llvm::legacy::PassManagerBase &PM) { \
		PM.add(new PreemptModulePass()); \
	});

REGISTER_PASS(g_register_preempt_module_pass1, EP_EarlyAsPossible)
REGISTER_PASS(g_register_preempt_module_pass2, EP_VectorizerStart)
REGISTER_PASS(g_register_preempt_module_pass3, EP_Peephole)
REGISTER_PASS(g_register_preempt_module_pass4, EP_LateLoopOptimizations)

// ... and at the end, before emitting code
REGISTER_PASS(g_register_preempt_module_pass5, EP_OptimizerLast)

namespace llvm {
namespace orc {

struct KaleidoscopeJIT {
private:
	std::unique_ptr<TargetMachine> TM;
	const bool enable_optimizations;
	const bool verify;
	const bool reroll_loops;
	const std::string target_triple;
	ExecutionSession ES;
	RTDyldObjectLinkingLayer ObjectLayer;
	IRCompileLayer CompileLayer;
	IRTransformLayer OptimizeLayer;
	ObjectTransformLayer TransformLayer;

	DataLayout DL;
	MangleAndInterner Mangle;
	ThreadSafeContext Ctx;

	JITDylib &MainJD;

	LlvmFragment& fragment;

	static llvm::Expected<std::unique_ptr<llvm::MemoryBuffer>>
	disasm_func(std::unique_ptr<MemoryBuffer> buffer)
	{
		ASSERT(buffer);

		disassemble(buffer->getBufferStart(), buffer->getBufferSize());
		return std::move(buffer);
	}
public:
	KaleidoscopeJIT(std::unique_ptr<TargetMachine>&& GivenTM, DataLayout DL,
		const std::string& target_triple, LlvmFragment& fragment)
	: TM(std::move(GivenTM)),
		enable_optimizations(fragment.enable_optimizations),
		verify(fragment.verify),
		reroll_loops(fragment.flavor ? fragment.flavor->reroll_loops : false),
		target_triple(target_triple),
		ObjectLayer(ES,
		[&]() {
			return std::make_unique<LlvmFragmentMemoryManager>(fragment);
		}),
		CompileLayer(ES, TransformLayer,
			std::make_unique<SimpleCompiler>(*TM)),
		OptimizeLayer(ES, CompileLayer, [&] (auto TSM, const auto& R) {
			return optimize_module(std::move(TSM), R, this);
		}),
		TransformLayer(ES, ObjectLayer, disasm_func),
		DL(std::move(DL)),
		Mangle(ES, this->DL),
		Ctx(std::make_unique<LLVMContext>()),
		MainJD(ES.createBareJITDylib("<main>")),
		fragment(fragment)
	{
		MainJD.addGenerator(
			cantFail(
				DynamicLibrarySearchGenerator::GetForCurrentProcess(
					DL.getGlobalPrefix())));
	}

	const DataLayout& get_data_layout() const { return DL; }

	LLVMContext& get_context() { return *Ctx.getContext(); }

	static std::unique_ptr<KaleidoscopeJIT> make(LlvmFragment& fragment) {
		auto JTMB = JITTargetMachineBuilder::detectHost();
		if (!JTMB) {
			LOG_ERROR("LLVM: Cannot detect host");
			return nullptr;
		}

		JTMB->setCodeGenOptLevel(llvm::CodeGenOpt::Level::Aggressive);
		// or None

		auto TM = JTMB->createTargetMachine();
		if (!TM) {
			LOG_ERROR("LLVM: Cannot create TargetMachine");
			return nullptr;
		}
		std::string target_triple = JTMB->getTargetTriple().str();

		LOG_TRACE("Target CPU '%s' triple '%s' features '%s'",
			(*TM)->getTargetCPU().str().c_str(),
			target_triple.c_str(),
			(*TM)->getTargetFeatureString().str().c_str());

		auto DL = JTMB->getDefaultDataLayoutForTarget();
		if (!DL) {
			LOG_ERROR("LLVM: Cannot detect data layout");
			return nullptr;
		}

		// fix data layout such that i128 is aligned on 128-bits
		std::string DL_string(DL->getStringRepresentation());
		DL->reset(DL_string.empty() ?
			"i128:128:128" : (DL_string + "-i128:128:128"));

		return std::make_unique<KaleidoscopeJIT>(std::move(*TM), std::move(*DL),
			target_triple, fragment);
	}

	Error add_module(std::unique_ptr<Module> M) {
		M->setDataLayout(DL);
		M->setTargetTriple(target_triple);
		return OptimizeLayer.add(MainJD, ThreadSafeModule(std::move(M), std::move(Ctx)));
	}

	Expected<JITEvaluatedSymbol> lookup(StringRef Name) {
		return ES.lookup({&MainJD}, Mangle(Name.str()));
	}

	void release_some_memory() {
		TM.reset();
	}

	void ensure_sufficient_budget() {
		if (fragment.budget_user) {
			fragment.budget_user->ensure_sufficient(kDefaultBudget, "JIT");
		}
	}

private:
	static Expected<ThreadSafeModule>
	optimize_module(ThreadSafeModule TSM, const MaterializationResponsibility &R,
			KaleidoscopeJIT* jit)
	{
		ASSERT(jit && jit->TM);

		jit->ensure_sufficient_budget();
		scheduler::yield();

		LOG_DEBUG("Optimize module");
		TSM.withModuleDo([&](Module &M) {

			auto print = [&] (bool optimized) {
				if (LOG_WILL_LOG(DEBUG)) {
					auto& ir_code = jit->fragment.ir_code;
					ir_code.clear();
					llvm::raw_string_ostream rso(ir_code);
					M.print(rso, nullptr);

					LOG_DEBUG("LLVM module:\n%s", rso.str().c_str());
				}
			};

			auto verify = [&] (bool optimized) {
				if (!jit->verify) {
					return;
				}
				std::string type_str;
				llvm::raw_string_ostream rso(type_str);
				if (llvm::verifyModule(M, &rso)) {
					LOG_ERROR("Couldn't verify LLVM module: %s",
						rso.str().c_str());

					std::string type_str;
					llvm::raw_string_ostream rso(type_str);
					M.print(rso, nullptr);
					LOG_WARN("LLVM module:\n%s", rso.str().c_str());
					ASSERT(false);
				} else {
					LOG_TRACE("Verified.");
				}
			};

#if 1
			// print(false);
			if (!jit->enable_optimizations) {
				LOG_DEBUG("Skip optimize");
				verify(false);
				return;
			}
#endif
			verify(false);

			LOG_DEBUG("JIT: Optimize");

			{
				auto MPM = std::make_unique<legacy::PassManager>();
				MPM->add(createGlobalDCEPass());
				MPM->add(createPromoteMemoryToRegisterPass());
				MPM->add(createInstructionCombiningPass());
				MPM->run(M);

				auto FPM = std::make_unique<legacy::FunctionPassManager>(&M);
				FPM->add(createCFGSimplificationPass());
				FPM->add(createPromoteMemoryToRegisterPass());
				FPM->add(createInstructionCombiningPass());
				FPM->doInitialization();
				// Run the optimizations over all functions in the module being added to
				// the JIT.
				LOG_DEBUG("JIT: Optimize function");
				for (auto &F : M) {
					FPM->run(F);
				}

				FPM->doFinalization();
			}

			// Create a function pass manager.
			PassManagerBuilder Builder;
			Builder.OptLevel = 3;
			Builder.LoopVectorize = true;
			Builder.SLPVectorize = false;
			Builder.Inliner = createFunctionInliningPass(10);

			Builder.RerollLoops = false;
			Builder.DisableUnrollLoops = true;
			if (jit->reroll_loops) {
				Builder.RerollLoops = true;
				Builder.DisableUnrollLoops = false;
			}

			Builder.SizeLevel = 0;

			auto& TM = *jit->TM;
			TM.adjustPassManager(Builder);


			auto FPM = std::make_unique<legacy::FunctionPassManager>(&M);
			FPM->add(createTargetTransformInfoWrapperPass(TM.getTargetIRAnalysis()));
			Builder.populateFunctionPassManager(*FPM);

			auto MPM = std::make_unique<legacy::PassManager>();
			MPM->add(createTargetTransformInfoWrapperPass(TM.getTargetIRAnalysis()));
			Builder.populateModulePassManager(*MPM);

			jit->ensure_sufficient_budget();

			FPM->doInitialization();
			// Run the optimizations over all functions in the module being added to
			// the JIT.
			LOG_DEBUG("JIT: Optimize function");
			for (auto &F : M) {
				FPM->run(F);
			}

			FPM->doFinalization();

			jit->ensure_sufficient_budget();

			LOG_DEBUG("JIT: Optimize module");
			MPM->run(M);

			LOG_TRACE("JIT: Optimize done");
			print(true);
			verify(true);
		});

		return std::move(TSM);
	}
};

} /* orc */
} /* llvm */



LlvmFragment::LlvmFragment(const std::string& name, bool enable_optimizations,
	bool verify, bool paranoid,
	const std::shared_ptr<engine::voila::FlavorSpec>& flavor,
	excalibur::Context& sys_context,
	BudgetUser* budget_user)
 : CodeFragment(name), enable_optimizations(enable_optimizations), verify(verify),
 	paranoid(paranoid), budget_user(budget_user), flavor(flavor)
{
	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "llvmfragment");
	}
	llvm_jit = llvm::orc::KaleidoscopeJIT::make(*this);
	if (!llvm_jit) {
		LOG_ERROR("Cannot create JIT");
		THROW(CompilerException, "Cannot create JIT");
	}

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "llvmfragment");
	}
	llvm_module = std::make_unique<llvm::Module>(name, llvm_jit->get_context());

	memory_container = std::make_unique<DataCodeMemoryContainer>();
}

void
LlvmFragment::compile() {
	auto task = scheduler::get_current_task();

	// hack our BudgetUser into the current Task
	ASSERT(!task->budget_user);
	task->budget_user = budget_user;

	scheduler::yield(task);
	if (llvm_jit->add_module(std::move(llvm_module))) {
		LOG_ERROR("Cannot add module to JIT");
		THROW(CompilerException, "Cannot add module to JIT");
	}

	scheduler::yield(task);
	LOG_TRACE("JIT: Lookup call");
	auto a = llvm_jit->lookup(name);
	if (!a) {
		LOG_ERROR("Cannot lookup jitted function '%s'",
			name.c_str());
		THROW(CompilerException, "Cannot lookup JIT function");
	}

	call = (call_t)a->getAddress();

	LOG_DEBUG("JIT: Deallocate");
	llvm_module.reset();
	if (memory_container) {
		llvm_jit.reset();
	} else {
		llvm_jit->release_some_memory();
	}

	task->budget_user = nullptr;
	budget_user = nullptr;
}

llvm::LLVMContext&
LlvmFragment::get_context()
{
	ASSERT(llvm_jit);
	return llvm_jit->get_context();
}

const llvm::DataLayout&
LlvmFragment::get_data_layout() const
{
	ASSERT(llvm_jit);
	return llvm_jit->get_data_layout();
}

LlvmFragment::~LlvmFragment()
{
	LOG_TRACE("LlvmFragment: Destroy");
	llvm_module.reset();
	llvm_jit.reset();
	LOG_TRACE("LlvmFragment: Done");
}

std::unique_ptr<engine::voila::GlobalJit>
LlvmFragment::make_global_jit()
{
	return std::make_unique<GlobalJit>();
}