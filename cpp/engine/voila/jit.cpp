#include "jit.hpp"
#include "system/build.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"
#include "engine/voila/voila.hpp"
#include "engine/xvalue.hpp"
#include "engine/stream.hpp"
#include "engine/query.hpp"
#include "engine/bloomfilter.hpp"
#include "engine/profile.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/table.hpp"
#include "engine/budget.hpp"
#include "engine/bandit_utils.hpp"
#include "engine/adaptive/decision.hpp"
#include "engine/voila/statement_identifier.hpp"
#include "code_cache.hpp"
#include "compile_request.hpp"
#include "jit_complex_kernels.hpp"
#include "jit_prepare_pass.hpp"
#include "statement_fragment_selection_pass.hpp"

#include <string>
#include <memory>
#include <unordered_map>

// #define HAS_COMPUTED_GOTO
// #define PRINT_CODES

using namespace engine;
using namespace voila;

#if 0
#define TRACE_DEBUG(...) LOG_DEBUG(__VA_ARGS__)
#define TRACE_WARN(...) LOG_WARN(__VA_ARGS__)
#define TRACE_ERROR(...) LOG_ERROR(__VA_ARGS__)
#define DUMP_VOILA(x) x->dump()
#else
#define TRACE_DEBUG(...) LOG_DEBUG(__VA_ARGS__)
#define TRACE_WARN(...) LOG_DEBUG(__VA_ARGS__)
#define TRACE_ERROR(...) LOG_DEBUG(__VA_ARGS__)
#define DUMP_VOILA(x)
#endif

#define VM_PRIM_PREFETCH(SPEC_IP) __builtin_prefetch(&__codes_arr[SPEC_IP], 0, 1);
#define VM_PRIM_PREFETCH_NEXT() VM_PRIM_PREFETCH(__ip+1)


namespace engine::voila {
struct CodeBlockFuture : CodeBlock::BlockExtra {
	CodeCacheItemPtr item;
	QueryProfile* query_profile;
	JitPrimArg* jit_arg;

	CodeBlockFuture(CodeCacheItemPtr&& item, QueryProfile* query_profile,
		JitPrimArg* jit_arg)
	 : item(std::move(item)), query_profile(query_profile), jit_arg(jit_arg)
	{
	}

	~CodeBlockFuture() {
		item->code_cache.release(std::move(item));
	}

	void propagate_internal_profiling(const profiling::ProfileData* prof,
			QueryStage& stage) {
		item->code_cache.update_profiling(item, prof, stage);
	}


	template<typename T>
	static auto
	future_poll(const T& future)
	{
		ASSERT(future.valid());
		return future.wait_for(std::chrono::milliseconds(0));
	}

	bool has_completed() const {
		switch (future_poll(item->future)) {
		case std::future_status::ready:		return true;
		case std::future_status::timeout:	return false;
		default:
			ASSERT(false && "Invalid future status");
			break;
		}
		return true;
	}

	void wait() const {
		ASSERT(item->future.valid());
		item->future.wait();
	}

	CodeFragmentPtr get() const {
		try {
			ASSERT(item->future.valid());
			return item->future.get();
		} catch (...) {
			ASSERT(item && item->has_error());
			throw;
		}
	}
};
}


struct MultiColScanState : CodeBlock::BlockExtra {
	std::vector<std::pair<XValue*, size_t>> col_map;

	void add(XValue* xval, size_t col_index) {
		col_map.push_back({xval, col_index});
	}

	size_t size() const { return col_map.size(); }
};


JitPrimArg*
JitPrimArg::make(memory::Context* mem, CodeBlock* code_block, size_t src_num, size_t dst_num,
	size_t struct_num, const std::string signature, const std::string dbg_name, Stream& stream,
	int64_t voila_func_tag)
{
#ifdef COMBINE_ALLOCS
	// We use a special allocator to get a nicer memory layout
	size_t bytes = sizeof(JitPrimArg) + alignof(JitPrimArg) +
		+ (dst_num + src_num)*(sizeof(CPrimArg::IO) + 8)
		+ (struct_num * sizeof(CPrimArg::Struct) + 8);

	char* data = static_cast<char*>(mem ?
			mem->aligned_alloc(bytes, alignof(JitPrimArg),
				memory::Context::kNoRealloc) :
			::aligned_alloc(alignof(JitPrimArg), bytes)
		);

	char* data_end = data + bytes;

	if (!data) {
		return nullptr;
	}

	// allocate struct
	auto arg = new (data)JitPrimArg(dbg_name);
#else
	auto arg = new JitPrimArg(dbg_name);
#endif
	if (!arg) {
		return nullptr;
	}
	arg->reset();

	auto& c_arg = arg->c_arg;
	c_arg.num_sources = src_num;
	c_arg.num_sinks = dst_num;
	c_arg.num_structs = struct_num;
	c_arg.code_block = code_block;
	c_arg.pointer_to_invalid_access_safe_space =
		stream.get_pointer_to_invalid_access_sandbox_space();
	c_arg.pointer_to_invalid_access_zero_safe_space =
		stream.get_pointer_to_invalid_access_zero_sandbox_space();

#ifdef COMBINE_ALLOCS
	arg->allocated_data = data;
	arg->allocated_size = bytes;
#else
	arg->allocated_data = nullptr;
	arg->allocated_size = 0;
#endif
	arg->mem_context = mem;
	arg->has_full_evaluation = adaptive::DecisionConfigGetter::get_bool_assert(
		code_block->adaptive_actions,
		*code_block->stream.query.config,
		adaptive::ConfigOption::kCanFullEval);
	arg->full_evaluation_bit_score_divisor =
		(double)adaptive::DecisionConfigGetter::get_long_assert(
			code_block->adaptive_actions,
			*code_block->stream.query.config,
			adaptive::ConfigOption::kBitScoreDivisor);
	ASSERT(arg->full_evaluation_bit_score_divisor > 0);

	// sources
#ifdef COMBINE_ALLOCS
	data += sizeof(JitPrimArg);
	c_arg.sources = (CPrimArg::IO*)(data);
#else
	c_arg.sources = new CPrimArg::IO[c_arg.num_sources];
#endif

	ASSERT(((size_t)c_arg.sources % alignof(CPrimArg::IO)) == 0);

	size_t source_bytes = sizeof(CPrimArg::IO) * c_arg.num_sources;
	memset(arg->c_arg.sources, 0, source_bytes);


#ifdef COMBINE_ALLOCS
	data += source_bytes;
	c_arg.sinks = (CPrimArg::IO*)(data);
#else
	c_arg.sinks = new CPrimArg::IO[c_arg.num_sinks];
#endif

	ASSERT(((size_t)c_arg.sinks % alignof(CPrimArg::IO)) == 0);

	size_t sink_bytes = sizeof(CPrimArg::IO) * c_arg.num_sinks;
	memset(arg->c_arg.sinks, 0, sink_bytes);


#ifdef COMBINE_ALLOCS
	data += sink_bytes;
	c_arg.structs = (CPrimArg::Struct*)(data);
#else
	c_arg.structs = new CPrimArg::Struct[c_arg.num_structs];
#endif

	ASSERT(((size_t)c_arg.structs % alignof(CPrimArg::IO)) == 0);

	size_t struct_bytes = sizeof(CPrimArg::Struct) * c_arg.num_structs;
	memset(c_arg.structs, 0, struct_bytes);


#ifdef COMBINE_ALLOCS
	data += struct_bytes;
	ASSERT(data <= data_end);
#endif

	arg->profile = std::make_shared<PrimitiveProfile>(signature);
	arg->profile->voila_func_tag = voila_func_tag;
	arg->c_arg.profile = arg->profile.get();

	arg->cpp_struct_signatures.reserve(c_arg.num_structs);
	return arg;
}

void
JitPrimArg::free(JitPrimArg* arg)
{
	if (!arg) {
		return;
	}

#ifdef COMBINE_ALLOCS
	void* memory = arg->allocated_data;
	size_t alloc_size = arg->allocated_size;
	ASSERT(memory && alloc_size > 0);

	// reset & deallocate the remaining C++ objects (shared_ptrs etc.)
	arg->reset();
	arg->~JitPrimArg();


	if (arg->mem_context) {
		arg->mem_context->free(memory);
	} else {
		::free(memory);
	}
#else
	delete[] arg->c_arg.sources;
	delete[] arg->c_arg.sinks;
	delete[] arg->c_arg.structs;
	arg->reset();
	delete arg;
#endif
}

void
JitPrimArg::reset()
{
	c_arg.reset();

	profile.reset();

	memset(&fake_space[0], 0, sizeof(fake_space));

	block_future.reset();
	allocated_data = nullptr;
	allocated_size = 0;
}

JitPrimArg::JitPrimArg(const std::string& dbg_name)
 : dbg_name(dbg_name)
{

}

JitPrimArg::~JitPrimArg()
{

}



template<bool ENABLE=true>
struct PrimProfileCollector {
	inline PrimProfileCollector(CPrimArg& arg) : arg(arg) {
		if (ENABLE) {
			__builtin_prefetch(&arg.profile, 1, 3);
		}
	}

	template<typename T>
	inline uint64_t operator()(const T& fun) {
		uint64_t prof_start = 0;
		uint64_t prof_end = 0;

		if (ENABLE) {
			prof_start = profiling::physical_rdtsc();
		}

		fun();

		if (ENABLE) {
			prof_end = profiling::physical_rdtsc();

			arg.profile->update(1, arg.num_in_tuples, arg.num_out_tuples,
				prof_end-prof_start);
			arg.num_in_tuples = 0;
			arg.num_out_tuples = 0;
		}
		return prof_end - prof_start;
	}

	CPrimArg& arg;
};

static CodeFragmentPtr
compiling_fragment_get(const CodeBlockFuture& bf)
{
	if (!bf.has_completed()) {
		LOG_TRACE("compiling_fragment_get: Wait until fragment is compiled");
			// wait indefinitely until fragment is compiled
		auto start_us = std::chrono::high_resolution_clock::now();
		auto start_cy = profiling::rdtsc();
		bf.wait();
		auto stop_cy = profiling::rdtsc();
		auto stop_us = std::chrono::high_resolution_clock::now();
		auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
			stop_us - start_us).count();
		LOG_TRACE("compiling_fragment_get: Fragment is compiled after waiting "
			"%lld ms",
			(int64_t)duration_us);

		if (bf.query_profile) {
			auto& prof = *bf.query_profile;
			prof.total_compilation_wait_time_us += duration_us;
			prof.total_compilation_wait_time_cy += stop_cy - start_cy;
		}
	}

	// set callback pointer
	CodeFragmentPtr fragment_ptr;
	try {
		fragment_ptr = bf.get();
	} catch (...) {
		LOG_ERROR("compiling_fragment_get: Caught exception");
		RETHROW();
	}
	ASSERT(fragment_ptr);
	return fragment_ptr;
}

template<bool PROFILING, int SKIP_ZERO, typename S, typename T>
FORCE_INLINE inline void
templated_compiled_fragment(CodeBlock::Context* context, const S& ip, T& data)
{
	LOG_TRACE("CALL [%llu] compiled_fragment with call %p",
		ip, data.call);

	if (SKIP_ZERO == 0 && data.source_predicate_num &&
			*data.source_predicate_num <= 0) {
		return;
	} else if (SKIP_ZERO == 1 && *data.source_predicate_num <= 0) {
		return;
	}

	__builtin_prefetch((void*)data.call, 0, 1);
	auto& arg = data.prim_arg;

	PrimProfileCollector<PROFILING> collector(*arg);

	int64_t r;
	collector([&] () {
		r = data.call(arg, context);
	});
	DBG_ASSERT(r == 0);
}

template<bool IS_TRUE, bool PROFILING>
FORCE_INLINE inline void
templated_seltruefalse(CodeBlock::Context* context)
{
	auto& item = *context->item;
	auto& data = item.data.selcond;
	auto& result = item.result;
	auto& predicate = data.predicate;

	auto jit_arg = (JitPrimArg*)data.prof_arg;

	DBG_ASSERT(data.prof_arg->num_sources == 1 && data.prof_arg->num_sinks == 1);

	auto values = data.prof_arg->sources[0].xvalue;
	auto tmp = data.prof_arg->sinks[0].xvalue;

	DBG_ASSERT(values && values->get_width() == 1);
	DBG_ASSERT(tmp && tmp->get_width() == sizeof(selvector_t));


	LOG_TRACE("CALL [%llu] %s with values=%p predicate=%p",
		context->block->ip, IS_TRUE ? "seltrue" : "selfalse",
		values, data.predicate);

	static_assert(sizeof(selvector_t) == 4, "Must be 32-bit selvector");

	typedef int32_t (*sel_prim_t)(int32_t* RESTRICT res, char* RESTRICT pred,
		int32_t* RESTRICT sel, int32_t num, int32_t* RESTRICT tmp);

	static sel_prim_t call_table[] = {
#ifdef __AVX512F__
		&avx512_selcond_i8_i32<IS_TRUE, false, false>,
		&avx512_selcond_i8_i32<IS_TRUE, false, true>,
		// &avx512_selcond_i8_i32<IS_TRUE, true, false>,
		// &avx512_selcond_i8_i32<IS_TRUE, true, true>,
#else
		&regular_selcond_i8_i32<IS_TRUE, false>,
		&regular_selcond_i8_i32<IS_TRUE, true>
#endif
	};

	typedef BanditStaticArms<sizeof(call_table)/sizeof(call_table[0])> BanditType;

	struct Extra : JitPrimArgExtra {
		uint64_t curr_cycles = 0;
		size_t curr_tuples = 0;
		size_t curr_runs = 0;
		size_t curr_arm = 0;

		BanditType bandit;
	};

	PrimProfileCollector<PROFILING> collector(*data.prof_arg);

	auto num = predicate->get_num();
	if (num) {
		if (UNLIKELY(!jit_arg->extra)) {
			jit_arg->extra = std::make_unique<Extra>();
		}

		auto& extra = (Extra&)*jit_arg->extra;
		if (UNLIKELY((extra.curr_runs % 4) == 0)) {
			if (extra.curr_runs > 0) {
				ASSERT(extra.curr_tuples);

				extra.bandit.record(extra.curr_arm,
					-(double)extra.curr_cycles,
					extra.curr_tuples);
			}

			extra.curr_cycles = 0;
			extra.curr_tuples = 0;
			extra.curr_runs = 0;
			extra.curr_arm = extra.bandit.choose_arm();
		}

		auto fun = call_table[extra.curr_arm];
		uint64_t cycles = collector([&] () {
			result->num = fun(
				result->get_first_as<selvector_t>(),
				values->get_first_as<char>(),
				predicate->get_first_as<selvector_t>(),
				num,
				tmp->get_first_as<selvector_t>());
		});

		extra.curr_cycles += cycles;
		extra.curr_tuples += num;
		extra.curr_runs++;
	} else {
		result->num = 0;
	}
	result->done = data.predicate->done; // || data.value->done;

	LOG_TRACE("CALL [%llu] %s with num=%d",
		context->block->ip, IS_TRUE ? "seltrue" : "selfalse",
		(int)result->num);
}

#define VM_PRIM_END(NAME) VM_PRIM_END_EX(NAME, true)



// Number the generated interpreter primitives
enum VmPrims {
#define VM_PRIM_BEGIN(NAME) prim_##NAME,
#define VM_PRIM_END_EX(NAME, NEXT)

#include "jit_funcs.hpp"
};

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX




// #undef HAS_COMPUTED_GOTO

#ifdef HAS_COMPUTED_GOTO
#define VM_PRIM_GET(NAME) (CodeBlock::call_t)(VmPrims::prim_##NAME)

#define VM_PRIM_ASSERT(x) if (ENABLE_DEBUG) { ASSERT(x); }
#define VM_PRIM_DEBUG(...) if (ENABLE_DEBUG) { LOG_DEBUG(__VA_ARGS__); }

static void
print_codes(std::ostream& out, CodeBlock* code_block)
{
	const auto num = code_block->b.size();
	CodeBlock::BlockItem* codes_arr = code_block->context_struct.codes_arr;

	static const char* pretty_names[] = {

#define VM_PRIM_BEGIN(NAME) #NAME,
#define VM_PRIM_END_EX(NAME, NEXT)
#define VM_PRIM_RETURN(NAME, RETURNCODE, NEW_IP) \
		code_block->ip = NEW_IP; \
		return RETURNCODE;

#include "jit_funcs.hpp"

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX

		nullptr
	};

	for (size_t i=0; i<num; i++) {
		size_t call_idx = (size_t)codes_arr[i].call;

		out << "[" << i << "]: " << call_idx << " ";
		bool is_code_fragment =
			VM_PRIM_GET(compiling_fragment) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_noprof) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_prof) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_noprof_spn) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_prof_spn) == codes_arr[i].call;

		out << pretty_names[call_idx];

		if (is_code_fragment) {
			auto& data = codes_arr[i].data.compiling_fragment;
			auto& profile = data.prim_arg->profile;


			if (profile) {
				out << " '" << profile->get_name() << "'";
			}
			out << " " << std::hex << (uint64_t)data.call << std::dec;
		}
		out << "\n";
	}
}

template<bool ENABLE_DEBUG>
inline static int
goto_interpret(CodeBlock* code_block)
{
	CodeBlock::Context* context = &code_block->context_struct;
	auto __codes_arr = context->codes_arr;

	size_t __ip = code_block->ip;

	// setup the LUT
	static void* dispatch_table[] = {

#define VM_PRIM_BEGIN(NAME) &&label_##NAME,
#define VM_PRIM_END_EX(NAME, NEXT)

#include "jit_funcs.hpp"

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX

		nullptr
	};

	// generate goto labels
#define DISPATCH(NEXT) \
	context->item = NEXT ? \
		&__codes_arr[++__ip] : \
		&__codes_arr[__ip]; \
	goto *dispatch_table[(size_t)context->item->call];




#define VM_PRIM_IMPL
#define VM_PRIM_BEGIN(NAME) \
	label_##NAME: {

#define VM_PRIM_END_EX(NAME, NEXT) \
		DISPATCH(NEXT); \
	}

	DISPATCH(false);

	while (1) {
#include "jit_funcs.hpp"
	}

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX
#undef VM_PRIM_IMPL
}

#else /* !HAS_COMPUTED_GOTO */

#define VM_PRIM_GET(NAME) (CodeBlock::call_t)(VmPrims::prim_##NAME)

#define VM_PRIM_ASSERT(x) DBG_ASSERT(x)
#define VM_PRIM_DEBUG(...) if (ENABLE_DEBUG) { LOG_DEBUG(__VA_ARGS__); }


static void
print_codes(std::ostream& out, CodeBlock* code_block)
{
	const auto num = code_block->b.size();
	CodeBlock::BlockItem* codes_arr = code_block->context_struct.codes_arr;

	static const char* pretty_names[] = {

#define VM_PRIM_BEGIN(NAME) #NAME,
#define VM_PRIM_END_EX(NAME, NEXT)

#include "jit_funcs.hpp"

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX

		nullptr
	};

	for (size_t i=0; i<num; i++) {
		size_t call_idx = (size_t)codes_arr[i].call;

		out << "[" << i << "]: " << call_idx << " ";
		bool is_code_fragment =
			VM_PRIM_GET(compiling_fragment) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_noprof) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_prof) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_noprof_spn) == codes_arr[i].call ||
			VM_PRIM_GET(compiled_fragment_prof_spn) == codes_arr[i].call;


		out << pretty_names[call_idx];

		if (is_code_fragment) {
			auto& data = codes_arr[i].data.compiling_fragment;
			auto& profile = data.prim_arg->profile;


			if (profile) {
				out << " '" << profile->get_name() << "'";
			}
			out << " " << std::hex << (uint64_t)data.call << std::dec;
		} else if (VM_PRIM_GET(goto_cond) == codes_arr[i].call ||
				VM_PRIM_GET(goto_uncond) == codes_arr[i].call) {
			auto& data = codes_arr[i].data.go;

			if (VM_PRIM_GET(goto_cond) == codes_arr[i].call) {
				if (data.value) {
					out << " true";
				} else {
					out << " false";
				}
			}
			out << " dest=" << data.offset;
		} else if (VM_PRIM_GET(scan_col) == codes_arr[i].call) {
			auto& data = codes_arr[i].data.scan_col;
			out << " col_idx=" << data.col_index;
		}
		out << "\n";
	}
}

template<bool ENABLE_DEBUG>
inline static int
goto_interpret(CodeBlock* code_block)
{
	CodeBlock::Context* context = &code_block->context_struct;
	auto codes_arr = context->codes_arr;

	// generate goto labels
#define DISPATCH(NEXT) \
	ctx.item = NEXT ? \
		&codes_arr[++code_block->ip] : \
		&codes_arr[code_block->ip]; \
	break;




#define VM_PRIM_IMPL
#define VM_PRIM_BEGIN(NAME) \
	case prim_##NAME: {

#define VM_PRIM_END_EX(NAME, NEXT) \
		DISPATCH(NEXT); \
	}

	ctx.item = &codes_arr[code_block->ip];

	if (ENABLE_DEBUG) {
		while (1) {
			switch ((size_t)ctx.item->call) {
#include "jit_funcs.hpp"

			default:
				ASSERT(false && "Invalid code");
				break;
			}
		}
	} else {
		while (1) {
			switch ((size_t)ctx.item->call) {
#include "jit_funcs.hpp"
			}
		}
	}

#undef VM_PRIM_BEGIN
#undef VM_PRIM_END_EX
#undef VM_PRIM_IMPL
}

#endif

int
CodeBlock::interpret()
{
	// save some overhead, if logging is disabled
	if (false && LOG_WILL_LOG(LogLevel::DEBUG)) {
		std::ostringstream s;
		print_byte_code(s);
		LOG_DEBUG("JitByteCode:\n%s", s.str().c_str());
		return goto_interpret<true>(this);
	} else {
		return goto_interpret<false>(this);
	}
}

void
CodeBlock::print_byte_code(std::ostream& out)
{
	print_codes(out, this);
}

bool
CodeBlock::is_compilation_done(bool get)
{
	size_t num_timeouts = 0;
	size_t num_done = 0;

	for (auto& extra : running_futures) {
		auto future = dynamic_cast<CodeBlockFuture*>(extra);
		ASSERT(extra && future);

		if (future->has_completed()) {
			if (get) {
				// possibly deliver exceptions
				future->get();
			}
			num_done++;
		} else {
			num_timeouts++;
		}
	}

	LOG_TRACE("is_compilation_done() returned %llu timeouts, %llu completed",
		num_timeouts, num_done);
	return !num_timeouts;
}

CodeBlock::CodeBlock(adaptive::Actions* actions, lolepop::Lolepop& op)
 : vector_size(adaptive::DecisionConfigGetter::get_long_assert(actions,
		*op.stream.query.config, adaptive::ConfigOption::kVectorSize)),
	morsel_size(op.stream.query.config->morsel_size()),
	stream(op.stream), mem_context(*op.mem_context),
	adaptive_actions(actions)
{
	op_output = std::make_unique<lolepop::Stride>();
}

CodeBlock::~CodeBlock()
{
	for (auto& obj : allocated_xvalues) {
		mem_context.deleteObj<XValue>(obj);
		obj = nullptr;
	}

	for (auto& block : allocated_codeblocks) {
		delete block;
		block = nullptr;
	}

	for (auto& arg : allocated_primargs) {
		JitPrimArg::free(arg);
		arg = nullptr;
	}
}

std::vector<std::shared_ptr<PrimitiveProfile>>
CodeBlock::get_prof_data() const
{
	std::vector<std::shared_ptr<PrimitiveProfile>> r;
	r.reserve(allocated_primargs.size());

	for (auto& prim_arg : allocated_primargs) {
		if (!prim_arg->profile) {
			continue;
		}
		r.push_back(prim_arg->profile);
	}

	return r;
}

void
CodeBlock::propagate_internal_profiling() const
{
	if (!stream.profile) {
		return;
	}

	for (auto& prim_arg : allocated_primargs) {
		if (!prim_arg->profile) {
			continue;
		}

		ASSERT(prim_arg->block_future);
		prim_arg->block_future->propagate_internal_profiling(
			prim_arg->profile.get(), stream.query_stage);
	}
}



GlobalCodeContext::GlobalCodeContext(memory::Context& mem_context)
 : mem_context(mem_context)
{

}

GlobalCodeContext::~GlobalCodeContext()
{
	for (auto& kv : globals) {
		auto& xval = kv.second;
		ASSERT(xval);
		mem_context.deleteObj<XValue>(xval);
		xval = nullptr;
	}
}



template<typename T>
static Type*
get_type(const T& infos, engine::voila::Node* node)
{
	// figoure out type
	auto it = infos.find(node);
	ASSERT(it != infos.end());

	auto node_infos = it->second;
	ASSERT(node_infos.size() == 1);

	return node_infos[0].type;
}

struct CopyElisonPass : VoilaTreePass {
	bool must_copy_value(Node* e) const {
		return modifying_assignment_values.find(e) != modifying_assignment_values.end();
	}

	CopyElisonPass(const CodeBlockBuilder& builder, BudgetUser* budget_user)
	 : voila_context(builder.voila_context), budget_user(budget_user)
	{

	}

private:
	void on_statement(const std::shared_ptr<Assign>& b) final {
		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "CopyElisonPass");
		}
		assigns[b->var.get()].push_back(b.get());
		assign_value_usage_counter[b->value.get()]++;
	}

	void handle(const Expr& s) final {
		// ignore all expressions
		return;
	}

	void finalize() final {
		for (auto& var_assign : assigns) {
			const auto& assign_vec = var_assign.second;
			if (assign_vec.size() <= 1) {
				// just an alias
				continue;
			}

			// more than 1 assignment inidicates updates
			for (auto& assign : assign_vec) {
				// for use in CreateXValuePass, we must tag the assign->value
				const auto val_ptr = assign->value.get();

				auto type = get_type(voila_context.infos, val_ptr);
				if (type->is_position()) {
					continue;
				}

				bool copy = true;
				if (dynamic_cast<Function*>(val_ptr)) {
					copy = assign_value_usage_counter[val_ptr] > 1;
				}

				if (copy) {
					LOG_DEBUG("CopyElisonPass: Must copy value assigned to '%s' value=%p",
						assign->var->dbg_name.c_str(), val_ptr);
					val_ptr->dump();
					modifying_assignment_values.insert(val_ptr);
				}
			}
		}
	}

	const Context& voila_context;
	BudgetUser* budget_user;
	std::unordered_map<Variable*, std::vector<Assign*>> assigns;
	std::unordered_map<Expression*, size_t> assign_value_usage_counter;
	std::unordered_set<Node*> modifying_assignment_values;
};

struct CreateXValuePass : VoilaTreePass {
	CreateXValuePass(CodeBlockBuilder& builder,
		const CopyElisonPass& copy_elision_pass,
		BudgetUser* budget_user)
	 : builder(builder),
		voila_global_context(builder.voila_global_context),
		voila_context(builder.voila_context),
		block(builder.block),
		copy_elision_pass(copy_elision_pass),
		budget_user(budget_user)
	{

	}

	struct VisitedNodeInfo {
		std::vector<XValue*> copy_into;
	};

	using visited_val_t = GlobalCodeContext::visited_val_t;
	std::unordered_map<void*, visited_val_t> visited;
	std::unordered_map<void*, VisitedNodeInfo> infos;

	XValue*
	get_xvalue_of_var(Variable* var) const
	{
		TRACE_DEBUG("Build ReadVariable %p '%s' global=%d",
			var, var->dbg_name.c_str(), var->global);

		auto read_from_map = [&] (const auto& map) {
			auto it = map.find(var);
			ASSERT(it != map.end() && "Variable must have been assigned");
			ASSERT(it->second && "Must have stored pointer");

			TRACE_DEBUG("ReadVariable yields %p '%s' %p",
				var, var->dbg_name.c_str(), it->second);
			return it->second;
		};

		if (var->global) {
			return read_from_map(voila_global_context.globals);
		} else {
			return read_from_map(visited);
		}
	}


private:
	XValue** inject_result = nullptr;
	CodeBlockBuilder& builder;
	GlobalCodeContext& voila_global_context;
	Context& voila_context;
	CodeBlock& block;
	const CopyElisonPass& copy_elision_pass;
	BudgetUser* budget_user;

	static constexpr uint32_t kHandleNodeCannotModifyInput = 1 << 1;

	template<typename T>
	XValue*
	handle_node(Node* e, XValue::Tag tag, const T& f,
		const char* dbg_name, uint32_t flags = 0,
		XValue* source = nullptr)
	{
		LOG_DEBUG("XValuePass: handle_node %p", e);
		XValue* copy_into = nullptr;

		{
			auto it = visited.find(e);
			if (it != visited.end()) {
				if (inject_result) {
					copy_into = *inject_result;
					infos[e].copy_into.push_back(copy_into);
				}
				LOG_DEBUG("XValuePass: handle_node %p: already done", e);
				return it->second;
			}
		}

		auto get_type = [] (XValue* x) -> int {
			if (!x) return -1;
			auto t = x->get_data_type();
			if (!t) return -2;
			return t->get_tag();
		};

		// try to avoid copying values by allowing to inject the result pointer
		// directly
		XValue* r;
		if (inject_result) {
			const bool must_copy = copy_elision_pass.must_copy_value(e);

			LOG_DEBUG("Inject into %p tag %d, dbg_name %s, must_copy %d",
				*inject_result, get_type(*inject_result), dbg_name, (int)must_copy);
			if (source) {
				r = source;
				copy_into = *inject_result;
				ASSERT(*inject_result);
				ASSERT(!copy_into->match_flags(XValue::kConstVector));
			} else {
				if (must_copy) {
					r = block.newXValue(tag);
					copy_into = *inject_result;
				} else {
					r = *inject_result;
					LOG_DEBUG("Modified tag from %d to %d",
						r->tag, tag);
					r->tag = tag;
				}
			}
			inject_result = nullptr;
		} else {
			r = source ? source : block.newXValue(tag);
		}

		LOG_DEBUG("XValue %p type_tag %d (source %p, type_tag %d) dbg_name %s",
			r, get_type(r),
			source, get_type(source), dbg_name);

		if (r && dbg_name) {
			r->dbg_name = dbg_name;
		}

		// e.g. go to children
		f(r);

		visited[e] = r;

		infos[e].copy_into.push_back(copy_into);

		return r;
	}

	void on_statement(const std::shared_ptr<Assign>& b) final {
		auto recurse = [&] (auto& dest) {
			ASSERT(!inject_result);
			ASSERT(dest);
			ASSERT(!dest->match_flags(XValue::kConstVector | XValue::kConstStruct));
			inject_result = &dest;
			handle(b->value);
			inject_result = nullptr;
		};

		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "CreateXValuePass");
		}

		auto assign = [&] (auto& map, bool global) {
			auto var_ptr = b->var.get();
			auto it = map.find(var_ptr);

			DUMP_VOILA(b);
			LOG_DEBUG("Build Assign %s to %p '%s'",
				it == map.end() ? "initial" : "reassign",
				var_ptr, b->var->dbg_name.c_str());

			XValue* dest = nullptr;
			if (it == map.end()) {
				// first assignment, create variable
				const auto node_info_it = voila_context.infos.find(b->value.get());
				ASSERT(node_info_it != voila_context.infos.end() && "Must be typed");

				const auto& node_infos = node_info_it->second;
				ASSERT(node_infos.size() == 1);

				auto tag = XValue::Tag::kVector;

				if (node_infos[0].type->is_position()) {
					tag = XValue::Tag::kPosition;
				} else if (node_infos[0].type->is_predicate()) {
					tag = XValue::Tag::kPredicate;
				}

				if (global) {
					dest = voila_global_context.newXValue(tag);
				} else {
					dest = block.newXValue(tag);
				}

				LOG_DEBUG("Assign %p '%s' with xval %p",
					var_ptr, b->var->dbg_name.c_str(), dest);

				recurse(dest);
				map.insert({var_ptr, dest});
			} else {
				// re-assignment, copy into variable
				dest = it->second;
				recurse(dest);
			}
		};

		if (b->var->global) {
			assign(voila_global_context.globals, true);
		} else {
			assign(visited, false);
		}
	}



	void on_expression(const std::shared_ptr<Input>& e) final {
		XValue* source = block.newXValue(XValue::Tag::kTuple);
		source->dbg_name = "Input";

		ASSERT(block.op_input);

		auto& inputs = block.op_input->col_data;
		auto& predicate = block.op_input->predicate;

		source->num = inputs.size()+1;
		source->alloc(sizeof(XValue*), source->num);

		auto values = source->get_first_as<XValue*>();
		memset(values, 0, sizeof(XValue*)*source->num);

		values[0] = predicate;

		for (size_t i=0; i<inputs.size(); i++) {
			values[i+1] = inputs[i];
		}

		source->add_flags(XValue::kConstVector | XValue::kConstStruct);

		handle_node(e.get(), XValue::Tag::kTuple,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Input",
			0,
			source
		);
	}

	void on_expression(const std::shared_ptr<InputPredicate>& e) final {
		ASSERT(block.op_input);
		auto source = block.op_input->predicate;
		source->dbg_name = "InputPredicate";

		handle_node(e.get(), XValue::Tag::kPredicate,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"InputPredicate",
			kHandleNodeCannotModifyInput,
			source
		);
	}

	void on_expression(const std::shared_ptr<ReadVariable>& e) final {
		auto xval = get_xvalue_of_var(e->var.get());
		ASSERT(xval);

		handle_node(e.get(), xval->tag,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			e->var->dbg_name.c_str(), // "ReadVariable",
			kHandleNodeCannotModifyInput,
			xval
		);
	}

	static XValue::Tag
	function_get_tag(Function& e)
	{
		switch (e.tag) {
		case Function::Tag::kTuple:			return XValue::Tag::kTuple;
		case Function::Tag::kWritePos:		return XValue::Tag::kPosition;
		case Function::Tag::kBucketInsert:	return XValue::Tag::kVector;

		default:
			if (e.creates_predicate()) {
				return XValue::Tag::kPredicate;
			} else {
				return XValue::Tag::kVector;
			}
		}

		ASSERT(false);
		return XValue::Tag::kUndefined;
	}

	void on_expression(const std::shared_ptr<Function>& e) final {
		if (e->tag == Function::Tag::kTGet) {
			ASSERT(e->args.size() == 2);
			ASSERT(!inject_result && "Todo");
			ASSERT(dynamic_cast<Input*>(e->args[0].get()));

			auto constant = dynamic_cast<Constant*>(e->args[1].get());
			ASSERT(constant);
			auto index = std::stoll(constant->value);

			ASSERT(block.op_input);
			auto& col_data = block.op_input->col_data;
			ASSERT(index >= 0 && index < (int64_t)col_data.size());

			auto result = col_data[index];
			ASSERT(result && "tget");
			visited[e.get()] = result;
		} else {
			handle_node(e.get(), function_get_tag(*e),
				[&] (auto& result) {
					VoilaTreePass::on_expression(e);
				},
				Function::tag2cstring(e->tag)
			);
		}
	}

	void on_expression(const std::shared_ptr<Constant>& e) final {
		auto result = handle_node(e.get(), XValue::Tag::kVector,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Constant"
		);

		ASSERT(result);
		result->dbg_name = "Constant";
		auto vector_size = block.vector_size;
		auto type = get_type(voila_context.infos, e.get());
		ASSERT(type);
		result->alloc(*type, vector_size);
	}

	void on_expression(const std::shared_ptr<GetScanPos>& e) final {
		handle_node(e.get(), XValue::Tag::kPosition,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"GetScanPos"
		);
	}

	void on_expression(const std::shared_ptr<Scan>& e) final {
		handle_node(e.get(), XValue::Tag::kVector,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Scan"
		);
	}

	void handle(const Stmt& s) final {
		s->dump();
		if (s->predicate) {
			handle(s->predicate);
		}
		VoilaTreePass::handle(s);
	}

	void handle(const Expr& s) final {
		s->dump();
		VoilaTreePass::handle(s);
	}
};


struct ByteCodePass : VoilaTreePass {
private:
	using visited_val_t = GlobalCodeContext::visited_val_t;

	std::unordered_set<void*> visited;
	std::unordered_set<void*> temp_marked; // to avoid cycles

	static constexpr size_t kMaxUnroll = 1;

	void emit_copy(XValue* dest, XValue* src, XValue* pred = nullptr);
	void emit_comment(const char* string);

	template<typename T>
	void comment(const T& fun)
	{
		// emit_comment(fun(block.mem_context));
	}

	CodeBlock& block;
	CodeCache& code_cache;
	Context& voila_context;
	GlobalCodeContext& voila_global_context;
	CreateXValuePass& create_xvalues_pass;
	const FuseGroups* const fuse_groups;
	adaptive::Actions* adaptive_actions;
	adaptive::ActionsContext* adaptive_actions_ctx;

	Expr emit_predicate;

	const std::shared_ptr<IBudgetManager> budget_manager;
	BudgetUser* budget_user;
	std::unordered_map<void*, MultiColScanState*> read_pos_scan_group;

public:
	ByteCodePass(CodeBlockBuilder& builder,
		CreateXValuePass& create_xvalues_pass,
		const FuseGroups* const fuse_groups,
		size_t operator_id,
		const std::shared_ptr<IBudgetManager>& budget_manager,
		BudgetUser* budget_user)
	 : block(builder.block),
	 	code_cache(builder.code_cache),
		voila_context(builder.voila_context),
		voila_global_context(builder.voila_global_context),
		create_xvalues_pass(create_xvalues_pass),
		fuse_groups(fuse_groups),
		adaptive_actions(block.stream.execution_context.adaptive_decision),
		adaptive_actions_ctx(block.stream.execution_context.adaptive_decision_ctx.get()),
		budget_manager(budget_manager),
		budget_user(budget_user)
	{

	}

private:
	template<typename T>
	void recursive_node(Node* p, const T& fun)
	{
		bool cycle = temp_marked.find(p) != temp_marked.end();
		if (cycle) {
			LOG_ERROR("ByteCodePass: Cycle detected at node p=%p", p);
			p->dump();
		}
		ASSERT(!cycle && "Cycle detected");
		temp_marked.insert(p);

		fun();

		temp_marked.erase(p);
	}

	void finalize() final {
		for (size_t i=0; i<kMaxUnroll; i++) {
			CodeBlock::BlockItem bi;
			bi.call = VM_PRIM_GET(end);
			block.b.emplace_back(bi);
		}
	}

	void handle(const Expr& s) final {
		if (!s) {
			return;
		}

		bool already_visited = visited.find(s.get()) != visited.end();
		TRACE_WARN("handle expr %p already_visited %d",
			s.get(), (int)already_visited);
		DUMP_VOILA(s);

		if (!already_visited) {
			recursive_node(s.get(), [&] () {
				if (in_fuse_group(s.get(), "expression")) {
					return;
				}

				TRACE_WARN("handle_expr visit %p", s.get());
				s->visit(s, *this);
			});
		}
	}

	void handle(const Stmt& s) final {
		TRACE_WARN("handle stmt %p", s.get());

		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget, "ByteCodePass");
		}

		recursive_node(s.get(), [&] () {
			if (in_fuse_group(s.get(), "statement")) {
				return;
			}

			VoilaTreePass::handle(s);
		});
	}

	void handle(const std::vector<Stmt>& stmts) final {
		VoilaTreePass::handle(stmts);
	}

	template<typename T, typename F>
	XValue* expr_wrapper(const T& ptr, const F& fun) {
		XValue* r;

		LOG_DEBUG("expr_wrapper %p", ptr.get());
		DUMP_VOILA(ptr);

		{
			auto it = create_xvalues_pass.visited.find(ptr.get());
			ASSERT(it != create_xvalues_pass.visited.end() &&
				"Must be created by CreateXValuePass");

			r = it->second;
		}

		{
			auto it = create_xvalues_pass.infos.find(ptr.get());
			ASSERT(it != create_xvalues_pass.infos.end() &&
				"Must have info attached");
			for (auto dest : it->second.copy_into) {
				if (!dest) {
					continue;
				}

				LOG_DEBUG("emit copy");
				emit_copy(dest, r);
			}
		}

		fun(r);

		return r;
	}

	template<typename T>
	XValue* expr_wrapper(const T& ptr) {
		return expr_wrapper<T>(ptr, [] (auto r) {});
	}

	visited_val_t handle_expr(const Expr& s, uint64_t flags = 0) {
		handle(s);

		auto xval_it = create_xvalues_pass.visited.find(s.get());
		ASSERT(xval_it != create_xvalues_pass.visited.end() &&
			"Must have generated something");
		return xval_it->second;
	}

	visited_val_t gen_predicate(const Expr& expr, uint64_t flags) {
		// LOG_DEBUG("gen_predicate");
		if (!expr) {
			return nullptr;
		}
		return handle_expr(expr, flags);
	}

	visited_val_t gen_expr(const Expr& expr) {
		return handle_expr(expr, 0);
	}

	bool in_fuse_group(Node* p, const char* dbg_path);


	// void on_statement(const std::shared_ptr<Assign>& b) final;
	void on_statement(const std::shared_ptr<Block>& b) final;
	void on_statement(const std::shared_ptr<Emit>& b) final;
	void on_statement(const std::shared_ptr<Effect>& b) final;
	void on_statement(const std::shared_ptr<Loop>& b) final;
	void on_statement(const std::shared_ptr<EndOfFlow>& b) final;
	void on_statement(const std::shared_ptr<Comment>& b) final;

	void on_expression(const std::shared_ptr<Input>& e) final;
	void on_expression(const std::shared_ptr<InputPredicate>& e) final;
	void on_expression(const std::shared_ptr<ReadVariable>& e) final;
	void on_expression(const std::shared_ptr<Function>& e) final;
	void on_expression(const std::shared_ptr<Constant>& e) final;
	void on_expression(const std::shared_ptr<GetScanPos>& e) final;
	void on_expression(const std::shared_ptr<Scan>& e) final;

	void jitted_fragment(const std::shared_ptr<CompileRequest>& request,
		CodeBlock::BlockItem& bi, FuseGroup* fuse_group, int64_t voila_func_tag,
		Node* single_node_ptr);

};

void
ByteCodePass::jitted_fragment(const std::shared_ptr<CompileRequest>& request,
	CodeBlock::BlockItem& bi, FuseGroup* fuse_group, int64_t voila_func_tag,
	Node* single_node_ptr)
{
	auto& query = block.stream.query;

	if (LOG_WILL_LOG(DEBUG)) {
		if (fuse_group) {
			LOG_DEBUG("jitted_fragment %p from group %p '%s'",
				request.get(), fuse_group, fuse_group->dbg_name.c_str());
		} else {
			LOG_DEBUG("jitted_fragment %p", request.get());
		}
		LOG_DEBUG("sources:");
		for (auto& s  : request->sources) {
			LOG_DEBUG("%p", s.get());
			s->dump();
		}
		LOG_DEBUG("sinks:");
		for (auto& s  : request->sinks) {
			LOG_DEBUG("%p", s.get());
			s->dump();
		}

		if (request->statements.size() > 0) {
			LOG_DEBUG("statements:");
			for (auto& s  : request->statements) {
				LOG_DEBUG("%p", s.get());
				s->dump();
			}
		}

		LOG_DEBUG("other:");
	}

	request->debug_validate();

	uint64_t flags = CodeCache::kGetFlagSimplePrimitive;

	if (adaptive_actions) {
		adaptive_actions->apply_fragment(adaptive_actions_ctx, *request,
			fuse_group);

		flags |= CodeCache::kIncrementalCompilation;
	}
#if 0
	if (!adaptive_actions || !request->flavor) {
		flags |= CodeCache::kGetFlagMatchAnyFlavor;
	}
#endif

	request->canonicalize();

	if (!request->flavor) {
		request->flavor = query.default_flavor_spec;
	}

	const size_t num_sources = request->sources.size();
	const size_t num_sinks = request->sinks.size();
	const size_t num_structs = request->structs.size();


	auto future = code_cache.get(request, &query, flags);

	auto& data = bi.data.compiling_fragment;

	// Setup JitPrimArg
	auto jit_arg = JitPrimArg::make(
		&block.mem_context, std::addressof(block),
		num_sources, num_sinks, num_structs, future->full_signature,
		fuse_group ? fuse_group->dbg_name : "atom",
		block.stream, voila_func_tag);

	if (jit_arg->profile) {
		auto& dest = jit_arg->profile->statement_range;
		if (fuse_group && fuse_group->statement_range) {
			dest = fuse_group->statement_range;
		} else if (!fuse_group) {
			ASSERT(single_node_ptr);
			auto it = voila_context.locations.find(single_node_ptr);
			if (it != voila_context.locations.end()) {
				dest = it->second.range;
			}
		}
	}
	data.prim_arg = &jit_arg->c_arg;
	auto& arg = data.prim_arg;
	block.allocated_primargs.push_back(jit_arg);

	size_t i;


	i = 0;
	{
		// setup sinks
		for (auto& sink_node_ptr : request->sinks) {
			auto sink_var_ptr = std::dynamic_pointer_cast<voila::Variable>(sink_node_ptr);
			auto sink_expr_ptr = std::dynamic_pointer_cast<voila::Expression>(sink_node_ptr);
			auto sink_assign_ptr = std::dynamic_pointer_cast<voila::Assign>(sink_node_ptr);
			ASSERT(sink_expr_ptr || sink_assign_ptr || sink_var_ptr);

			auto sink_func_ptr = std::dynamic_pointer_cast<voila::Function>(sink_expr_ptr);

			TRACE_WARN("jitted_fragment: sink[%d]: %s",
				i, (sink_func_ptr ? "func" : "var"));

			XValue* result_xval;
			Node* node = sink_node_ptr.get();
			if (sink_func_ptr) {
				result_xval = expr_wrapper(sink_func_ptr);
				ASSERT(result_xval);				
			} else if (sink_var_ptr) {
				result_xval = create_xvalues_pass.get_xvalue_of_var(sink_var_ptr.get());
			} else {
				ASSERT(sink_assign_ptr);
				auto& var = sink_assign_ptr->var;

				result_xval = create_xvalues_pass.get_xvalue_of_var(var.get());
			}

			TRACE_WARN("jitted_fragment: sink[%d]: %s: xval=%p",
				i, (sink_func_ptr ? "func" : "var"), result_xval);

			// create result
			auto type = get_type(voila_context.infos, node);
			ASSERT(type);
			result_xval->alloc(*type, block.vector_size);

			if (!result_xval->first) {
				TRACE_DEBUG("Sink %llu doesn't have ->first", i);
			} else {
				TRACE_DEBUG("result_xval->first %p, &result_xval->first %p",
					result_xval->first, &result_xval->first);
			}
			arg->sinks[i].base = result_xval->first ? &result_xval->first : nullptr;
			arg->sinks[i].num = (int64_t*)&result_xval->num;
			arg->sinks[i].offset = &result_xval->offset;
			arg->sinks[i].xvalue = result_xval;

			// manually update cache
			visited.insert(sink_node_ptr.get());

			i++;
		}

		ASSERT(i == num_sinks);
	}

	// ASSERT(!num_structs || num_structs == 1);
	arg->num_structs = num_structs;

	i = 0;
	for (auto& struct_ptr : request->structs) {
		auto voila_ht = std::dynamic_pointer_cast<voila::HashTable>(struct_ptr);

		ASSERT(voila_ht);

		auto& phys = voila_ht->physical_table;
		ASSERT(phys.get());

		auto& entry = arg->structs[i];
		entry.mask = &phys->hash_index_mask;
		entry.buckets = (void**)&phys->hash_index_head;
		entry.structure = phys.get();

		if (phys->bloom_filter) {
			phys->bloom_filter->ensure_built(budget_user);

			entry.bf_words = &phys->bloom_filter->word_array_ptr;
			entry.bf_mask = &phys->bloom_filter->mask;
		} else {
			entry.bf_words = (uint64_t**)&jit_arg->fake_space[0];
			entry.bf_mask = (uint64_t*)&jit_arg->fake_space[0];
		}

		jit_arg->cpp_struct_signatures.emplace_back(
			phys->get_layout().get_signature());

		i++;
	}

	i = 0;
	for (auto& stmt : request->statements) {
		visited.insert(stmt.get());
	}

	i = 0;
	for (auto& expr : request->expressions) {
		visited.insert(expr.get());
	}

	int64_t* source_predicate_num = nullptr;

	i = 0;
	{
		// setup sources
		for (auto& source_node_ptr : request->sources) {
			auto source_expr_ptr = std::dynamic_pointer_cast<voila::Expression>(source_node_ptr);
			ASSERT(source_expr_ptr);

			TRACE_WARN("source %lld", i);
			auto xval = handle_expr(source_expr_ptr);
			ASSERT(xval);

			if (!xval->first) {
				TRACE_DEBUG("Source %llu doesn't have ->first", i);
			}
			arg->sources[i].base = &xval->first;
			arg->sources[i].num = (int64_t*)&xval->num;
			arg->sources[i].offset = &xval->offset;
			arg->sources[i].xvalue = xval;

			if (xval->tag == XValue::Tag::kPredicate) {
				ASSERT(!source_predicate_num && "Only one predicate allowed");
				source_predicate_num = arg->sources[i].num;
			}

			TRACE_DEBUG("source base %p, first %p",
				arg->sources[i].base, xval->first);
			i++;
		}

		ASSERT(i == num_sources);
	}

	// Will be dealloc'd with JitPrimArg
	jit_arg->block_future = std::make_unique<CodeBlockFuture>(std::move(future),
		query.profile.get(), jit_arg);

	bi.call = VM_PRIM_GET(compiling_fragment);

	// Create future wrapper, deallocated in compiling_fragment
	data.call = nullptr;
	data.source_predicate_num = source_predicate_num;

	// Track BlockFuture
	block.running_futures.insert(jit_arg->block_future.get());
}

void
ByteCodePass::on_statement(const std::shared_ptr<Block>& pb)
{
	TRACE_DEBUG("Build Block")
	auto& b = *pb;
	CodeBlock::BlockItem bi;

	auto pred = gen_predicate(b.predicate, 0);

	ASSERT(kMaxUnroll == 1);
	const auto idx = block.b.size();
	bi.call = VM_PRIM_GET(goto_cond);
	bi.data.go.predicate = pred;
	bi.data.go.value = false;
	block.b.emplace_back(bi);

	VoilaTreePass::on_statement(pb);

	auto& br = block.b[idx];
	br.data.go.offset = block.b.size();
}

void
ByteCodePass::on_statement(const std::shared_ptr<Emit>& b)
{
	TRACE_DEBUG("Build Emit")
	XValue* pred = gen_predicate(b->predicate, 0);

	emit_predicate = b->predicate;
	VoilaTreePass::on_statement(b);
	emit_predicate.reset();


	TRACE_DEBUG("Emit: Value");
	auto tuples = handle_expr(b->value);
	ASSERT(tuples->tag == XValue::Tag::kTuple);

	LOG_DEBUG("emit, tuple: %p", tuples);

	ASSERT(block.op_output);
	auto& col_data = block.op_output->col_data;

	ASSERT(tuples->get_num() > 0 && "Needs some values");

	auto vals = tuples->get_first_as<XValue*>();

	col_data.resize(tuples->get_num()-1);
	block.op_output->predicate = pred ? pred : vals[0];
	TRACE_DEBUG("predicate %p", block.op_output->predicate);
	for (size_t i=0; i<col_data.size(); i++) {
		col_data[i] = vals[i+1];
		TRACE_DEBUG("col_data %p", vals[i+1]);
	}

	auto function = dynamic_cast<Function*>(b->value.get());
	if (!function || function->tag != voila::Function::kTuple) {
		CodeBlock::BlockItem bi;
		bi.call = VM_PRIM_GET(emit);
		block.b.emplace_back(bi);
	} else if (function && function->tag == voila::Function::kTuple) {

	}
}

void
ByteCodePass::on_statement(const std::shared_ptr<Effect>& b)
{
	TRACE_DEBUG("Build Effect");
	gen_expr(b->value);
}

void
ByteCodePass::on_statement(const std::shared_ptr<Loop>& pb)
{
	TRACE_DEBUG("Build Loop")
	auto& b = *pb;
	CodeBlock::BlockItem bi;

	const auto idx = block.b.size();
	auto pred = gen_predicate(b.predicate, 0);

	for (size_t i=0; i<kMaxUnroll; i++) {
		bi.call = VM_PRIM_GET(goto_cond);
		bi.data.go.predicate = pred;
		TRACE_DEBUG("Built Loop Header check %p == false", pred);
		bi.data.go.value = false;
		block.b.emplace_back(bi);
	}

	VoilaTreePass::on_statement(pb);

	for (size_t i=0; i<kMaxUnroll; i++) {
		bi.call = VM_PRIM_GET(goto_uncond);
		bi.data.go.offset = idx;
		bi.data.go.predicate = pred;
		block.b.emplace_back(bi);
	}

	for (size_t i=0; i<kMaxUnroll; i++) {
		auto& br = block.b[idx+i];
		br.data.go.offset = block.b.size();
	}
}

void
ByteCodePass::on_statement(const std::shared_ptr<EndOfFlow>& b)
{
	TRACE_DEBUG("Build EndOfFlow")
	CodeBlock::BlockItem bi;
	bi.call = VM_PRIM_GET(end_of_flow);
	block.b.emplace_back(bi);
}

void
ByteCodePass::on_statement(const std::shared_ptr<Comment>& b)
{
	TRACE_DEBUG("Build Comment");

	auto predicate = gen_predicate(b->predicate, false);

	CodeBlock::BlockItem bi;
	bi.call = VM_PRIM_GET(comment);


	auto& data = bi.data.comment;
	data.message = (CodeBlock::BlockExtra*)b->message.c_str();
	data.log_level = b->log_level;
	data.predicate = predicate;

	block.b.emplace_back(bi);
}

bool
ByteCodePass::in_fuse_group(Node* p, const char* dbg_path)
{
	LOG_TRACE("in_fuse_group: %s: node %p", dbg_path, p);
	if (!fuse_groups) {
		LOG_TRACE("in_fuse_group: %s: No FuseGroups", dbg_path);
		return false;
	}

	if (visited.find(p) != visited.end()) {
		LOG_TRACE("in_fuse_group: %s: Already visited %p",
			dbg_path, p);
		return true; // already done
	}

	auto fuse_group = fuse_groups->get_group_by_node(p);
	if (!fuse_group) {
		return false;
	}

	LOG_DEBUG("in_fuse_group: %s: Has FuseGroup %p (%s) in path '%s'",
		dbg_path, fuse_group, fuse_group->dbg_name.c_str(), dbg_path);
	fuse_group->request->dump();

	fuse_group->request->budget_manager = budget_manager;

	CodeBlock::BlockItem bi;
	jitted_fragment(fuse_group->request, bi, fuse_group, -1, nullptr);
	block.b.emplace_back(bi);

	ASSERT(visited.find(p) != visited.end());
	return true;
}

void
ByteCodePass::on_expression(const std::shared_ptr<Function>& pe)
{
	if (in_fuse_group(pe.get(), "function")) {
		return;
	}

	using Tag = Function::Tag;

	bool avoid_updating_cache = false;

	auto& e = *pe;
	TRACE_DEBUG("Build Function %s (%d)",
		Function::tag2cstring(e.tag), e.tag);
	CodeBlock::BlockItem bi;

	auto predicate = gen_predicate(e.predicate, false);

	switch (e.tag) {
	case Tag::kTGet:
		// already done in CreateXValuePass, bail out
		return;

	default:
		break;
	}

	std::vector<visited_val_t> args;
	args.reserve(e.args.size());
	for (auto& arg : e.args) {
		args.push_back(arg ? handle_expr(arg) : nullptr);
	}

	XValue* result = nullptr;

	bool matched = false;

	switch (e.tag) {
	case Tag::kTuple:
		ASSERT(!JitPreparePass::is_jittable_func(e));
		result = expr_wrapper(pe, [&] (auto& result) {
			bi.call = VM_PRIM_GET(tuple_emit);

			auto extra = new CodeBlock::BlockExtra();
			block.allocated_codeblocks.push_back(extra);

			bi.data.tuple_emit.args = extra;
			bi.data.tuple_emit.predicate = gen_predicate(emit_predicate, 0);
			extra->args.reserve(e.args.size());

			for (auto& arg : e.args) {
				extra->args.emplace_back(handle_expr(arg));
			}
		});
		ASSERT(result && "tuple");

		{
			result->num = e.args.size()+1;
			result->alloc(sizeof(XValue*), result->num);

			auto values = result->get_first_as<XValue*>();
			memset(values, 0, sizeof(XValue*)*result->num);

			values[0] = predicate;

			for (size_t i=0; i<e.args.size(); i++) {
				values[i+1] = handle_expr(e.args[i]);
			}

			result->add_flags(XValue::kConstVector | XValue::kConstStruct);
		}

		LOG_DEBUG("tuple: %p", result);
		matched = true;

		break;
	case Tag::kSelNum:
		ASSERT(!JitPreparePass::is_jittable_func(e));
		result = expr_wrapper(pe, [&] (auto& result) {
			bi.call = VM_PRIM_GET(selnum);

			ASSERT(e.args.size() == 1);

			auto a = handle_expr(e.args[0]);
			TRACE_DEBUG("arg %p", a);
			ASSERT(a->tag == XValue::Tag::kPosition);
			bi.data.selnum.pos = a;

			// bi.data.selnum.predicate = predicate;
		});
		ASSERT(result && "selnum");

		{
			result->alloc(*TypeSystem::new_predicate(), block.vector_size);

			auto arr = (selvector_t*)result->first;
			for (size_t i=0; i<block.vector_size; i++) {
				arr[i] = i;
			}

			result->add_flags(XValue::kConstVector);
		}

		matched = true;
		break;

	case Tag::kWritePos:
		// ASSERT(!JitPreparePass::is_jittable_func(e));
		result = expr_wrapper(pe, [&] (auto& result) {
			TRACE_DEBUG("Build WritePos");

			ASSERT(pe->data_structure);

			bi.call = VM_PRIM_GET(write_pos);
			bi.result = result;
			bi.data.write_pos.table = pe->data_structure->physical_table.get();
			bi.data.write_pos.predicate = predicate;
		});
		ASSERT(result && "WritePos");

		matched = true;
		break;

	case Tag::kBucketInsert:
		result = expr_wrapper(pe, [&] (auto& result) {
			TRACE_DEBUG("Build BucketInsert");

			ASSERT(pe->data_structure);
			auto ht = std::dynamic_pointer_cast<engine::table::IHashTable>(
				pe->data_structure->physical_table);
			ASSERT(ht);

			ASSERT(e.args.size() == 1);
			auto a = handle_expr(e.args[0]);
			TRACE_DEBUG("arg %p", a);
			ASSERT(a->tag == XValue::Tag::kVector);

			bi.call = VM_PRIM_GET(bucket_insert);
			bi.result = result;

			auto& data = bi.data.bucket_insert;
			data.table = ht.get();
			data.predicate = predicate;
			data.indices = a;

			result->alloc(*TypeSystem::new_bucket(), block.vector_size);
		});
		ASSERT(result && "BucketInsert");

		matched = true;
		break;

	case Tag::kSelUnion:
		ASSERT(!JitPreparePass::is_jittable_func(e));
		result = expr_wrapper(pe, [&] (auto& result) {
			TRACE_DEBUG("Build SelUnion");

			ASSERT(e.args.size() == 1);
			auto a = handle_expr(e.args[0]);
			TRACE_DEBUG("arg %p", a);
			ASSERT(a->tag == XValue::Tag::kPredicate);

			bi.call = VM_PRIM_GET(selunion);
			bi.result = result;

			auto& data = bi.data.selunion;
			data.predicate = predicate;
			data.other = a;
			data.tmp = block.newXValue(XValue::Tag::kVector);
			data.tmp->alloc(*TypeSystem::new_predicate(), 2*block.vector_size);
			
		});
		ASSERT(result && "SelUnion");

		matched = true;
		break;

	case Tag::kSelFalse:
	case Tag::kSelTrue:
		if (adaptive::DecisionConfigGetter::get_bool_assert(
				block.adaptive_actions,
				*block.stream.query.config,
				adaptive::ConfigOption::kEnableSimdOpts)) {
			result = expr_wrapper(pe, [&] (auto& result) {
				TRACE_DEBUG("Build SelTrue/False");

				ASSERT(e.args.size() == 1);
				auto a = handle_expr(e.args[0]);
				TRACE_DEBUG("arg %p", a);

				auto& data = bi.data.selcond;
				data.predicate = predicate;
				// data.values = a;

				// keep profiling info
				auto jit_arg = JitPrimArg::make(
					&block.mem_context, std::addressof(block),
					1, 1, 0, "", "atom",
					block.stream, (int64_t)e.tag);
				jit_arg->c_arg.sources[0].xvalue = a;

				auto tmp = block.newXValue(XValue::Tag::kVector);
				tmp->alloc(*TypeSystem::new_predicate(), block.vector_size);
				jit_arg->c_arg.sinks[0].xvalue = tmp;

				auto single_node_ptr = pe.get();

				if (jit_arg->profile) {
					auto& dest = jit_arg->profile->statement_range;
					ASSERT(single_node_ptr);
					auto it = voila_context.locations.find(single_node_ptr);
					if (it != voila_context.locations.end()) {
						dest = it->second.range;
					}
				}
				data.prof_arg = &jit_arg->c_arg;
				block.allocated_primargs.push_back(jit_arg);

				// set bytecode
				bool is_true = e.tag == Tag::kSelTrue;

				if (is_true) {
					bi.call = VM_PRIM_GET(seltrue);
				} else {
					bi.call = VM_PRIM_GET(selfalse);
				}
				bi.result = result;

				result->alloc(*TypeSystem::new_predicate(), block.vector_size);
			});

			ASSERT(result && "SelTrue/false");
			matched = true;
		}
		break;

	default:
		break;
	}

	if (!matched) {
		matched = true;

		ASSERT(JitPreparePass::is_jittable_func(e));

		TRACE_DEBUG("Function");

		auto& query = block.stream.query;

		// Will be deallocated by CodeCache, we need this ugly manual allocation
		// because scheduler::async does not working with unique_ptr<>.
		std::shared_ptr<CompileRequest> request(
			std::make_shared<CompileRequest>(query.sys_context,
				query.config.get(), voila_context));

		// fallback compilation has no budget
		request->budget_manager.reset();

		bool is_new;

		// add sources
		request->sources.reserve(e.args.size()+1);
		for (auto& arg : e.args) {
			is_new = request->add_source(arg);
			ASSERT(is_new);
		}
		is_new = request->add_source(e.predicate);
		ASSERT(is_new);

		// add sinks
		is_new = request->add_sink(pe);
		ASSERT(is_new);

		if (pe->data_structure) {
			is_new = request->add_struct(pe->data_structure);
			ASSERT(is_new);
		}

		request->debug_validate();

		request->canonicalize();

		avoid_updating_cache = true;
		ASSERT(e.tag >= 0);
		jitted_fragment(std::move(request), bi, nullptr, e.tag, pe.get());
	}

	bi.result = result;
	block.b.emplace_back(bi);

	if (result) {
		result->dbg_name = Function::tag2cstring(e.tag);
	}

	TRACE_DEBUG("produced %p");

	if (!avoid_updating_cache) {
		visited.insert({pe.get()});
	}
}

void
ByteCodePass::on_expression(const std::shared_ptr<GetScanPos>& e)
{
	TRACE_DEBUG("Build GetScanPos");

	XValue* result = expr_wrapper(e);
	CodeBlock::BlockItem bi;

	bi.call = VM_PRIM_GET(scan_pos);
	bi.result = result;
	bi.data.scan_pos.vector_size = block.vector_size;
	bi.data.scan_pos.read_pos = &e->pos;
	block.b.emplace_back(bi);

	visited.insert(e.get());
}


void
ByteCodePass::on_expression(const std::shared_ptr<engine::voila::Scan>& e)
{
	TRACE_DEBUG("Build Scan");

	CodeBlock::BlockItem bi;
	auto& data = bi.data.scan_col;
	data.read_pos = handle_expr(e->pos);
	data.col_index = e->index;
	XValue* result = expr_wrapper(e);
	result->dbg_name = "Scan";

	data.predicate = gen_predicate(e->predicate, false);
	result->set_data_type(get_type(voila_context.infos, e.get()));

	if (true) {
		// collect scans on same group
		auto& group = read_pos_scan_group[data.read_pos];
		if (!group) {
			CodeBlock::BlockItem group_bi;
			group_bi.call = VM_PRIM_GET(scan_multiple_cols);
			group_bi.result = nullptr;

			auto& gdata = group_bi.data.scan_multiple_cols;
			gdata.read_pos = data.read_pos;
			gdata.predicate = data.predicate;

			gdata.state = new MultiColScanState();
			block.allocated_codeblocks.push_back(gdata.state);

			// new group, emit
			group = (MultiColScanState*)gdata.state;

			block.b.emplace_back(group_bi);
		}
		// add to group
		group->add(result, data.col_index);
	} else {
		bi.call = VM_PRIM_GET(scan_col);
		bi.result = result;
		block.b.emplace_back(bi);
	}

	visited.insert(e.get());
}

void
ByteCodePass::on_expression(const std::shared_ptr<Constant>& e)
{
	TRACE_DEBUG("Constant");
	XValue* result = expr_wrapper(e);

	auto vector_size = block.vector_size;
	auto type = result->get_data_type();
	size_t width = type->get_width();

	std::vector<char> max_type;
	max_type.resize(width);

	type->parse_from_string(&max_type[0], block.stream.global_strings.get(),
		e->value);

	result->broadcast(&max_type[0], width, vector_size);

	LOG_TRACE("Constant -> %p, first %p", result, result->first);

	ASSERT(result->get_data_type());

	visited.insert(e.get());
}

void
ByteCodePass::on_expression(const std::shared_ptr<Input>& e)
{
	expr_wrapper(e);
	visited.insert(e.get());
}

void
ByteCodePass::on_expression(const std::shared_ptr<InputPredicate>& e)
{
	expr_wrapper(e);
	visited.insert(e.get());
}

void
ByteCodePass::on_expression(const std::shared_ptr<ReadVariable>& e)
{
	expr_wrapper(e);
	visited.insert(e.get());
}

void
ByteCodePass::emit_copy(XValue* dest, XValue* src, XValue* pred)
{
	CodeBlock::BlockItem bi;

	TRACE_DEBUG("emit_copy: from %p to %p with pred %p", src, dest, pred);

	if (!dest) {
		dest = block.newXValue(src->tag);
	}

	auto type = src->get_data_type();

	if (!type && src->tag == XValue::Tag::kPredicate) {
		type = TypeSystem::new_predicate();
	} else if (!type && src->tag == XValue::Tag::kPosition) {
		type = TypeSystem::new_position();
	}

	ASSERT(type);
	dest->alloc(*type, src->get_capacity());

	bi.call = VM_PRIM_GET(copy);
	bi.result = dest;
	bi.data.copy.predicate = pred;
	bi.data.copy.source = src;

	ASSERT(dest != src);
	ASSERT(dest->first && src->first);
	block.b.emplace_back(bi);
}

void
ByteCodePass::emit_comment(const char* string)
{
	CodeBlock::BlockItem bi;
	bi.call = VM_PRIM_GET(comment);
	bi.data.comment.message = (CodeBlock::BlockExtra*)string;
	block.b.emplace_back(bi);
}



CodeBlockBuilder::CodeBlockBuilder(CodeCache& code_cache, CodeBlock& block,
	Context& voila_context, GlobalCodeContext& voila_global_context,
	const std::string& dbg_name,
	const std::shared_ptr<IBudgetManager>& budget_manager,
	size_t operator_id, CodeBlockBuilderProfData* prof,
	BudgetUser* budget_user)
 : block(block), code_cache(code_cache), voila_context(voila_context),
	voila_global_context(voila_global_context), dbg_name(dbg_name),
	budget_manager(budget_manager), operator_id(operator_id), prof(prof),
	budget_user(budget_user)
{
	run = false;
}

void
CodeBlockBuilder::operator()(const std::vector<Stmt>& stmts)
{
	uint64_t t_start = profiling::rdtsc();

	ASSERT(!run);
	run = true;

	std::unique_ptr<FuseGroups> fuse_groups = std::make_unique<FuseGroups>();

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "CodeBlockBuilder");
	}

	auto actions = block.stream.execution_context.adaptive_decision;

	if (actions) {
		auto actions_ctx = block.stream.execution_context.adaptive_decision_ctx.get();
		actions->apply_jit(actions_ctx, fuse_groups.get(), *this, stmts, operator_id,
			budget_user);
	} else {
		auto config = block.stream.query.config.get();
		switch (config->compilation_strategy()) {
		case QueryConfig::CompilationStrategy::kAdaptive:
			// if no decision made, start with regular interpretation
			fuse_groups.reset();
			break;
		case QueryConfig::CompilationStrategy::kNone:
			fuse_groups.reset();
			break;

		case QueryConfig::CompilationStrategy::kStatements:
			{
				LOG_DEBUG("CodeBlockBuilder: %s: StatementFragmentSelectionPass",
					dbg_name.c_str());
				StatementFragmentSelectionPass jit_prepare_pass(
					block.stream.query.sys_context,
					config,
					voila_context,
					budget_manager,
					fuse_groups.get(),
					nullptr,
					operator_id);
				jit_prepare_pass(stmts);
			}
			break;

		case QueryConfig::CompilationStrategy::kExpressions:
			{
				// extract complex code fragments etc
				JitPrepareExpressionGraphPass jit_prepare_graph_pass;
				jit_prepare_graph_pass(stmts);

				LOG_DEBUG("CodeBlockBuilder: %s: JitPreparePass",
					dbg_name.c_str());

				JitPreparePass jit_prepare_pass(jit_prepare_graph_pass,
					block.stream.query.sys_context,
					config,
					voila_context,
					budget_manager,
					fuse_groups.get());
				jit_prepare_pass(stmts);
			}
			break;

		default:
			ASSERT(false);
			break;
		}
	}

	if (budget_user) {
		budget_user->ensure_sufficient(kDefaultBudget, "CodeBlockBuilder");
	}

	if (fuse_groups && fuse_groups->empty()) {
		fuse_groups.reset();
	}

	LOG_DEBUG("CodeBlockBuilder: %s: %lld FuseGroups",
		dbg_name.c_str(),
		(int64_t)(fuse_groups.get() ? fuse_groups->size() : 0));

	uint64_t t_prologue = 0;
	if (prof) {
		t_prologue = profiling::rdtsc();
		prof->t_prologue += t_prologue - t_start;
	}

	CopyElisonPass copy_elision_pass(*this, budget_user);
	copy_elision_pass(stmts);

	uint64_t t_copy_elision = 0;
	if (prof) {
		t_copy_elision = profiling::rdtsc();
		prof->t_copy_elision += t_copy_elision - t_prologue;
	}

	CreateXValuePass create_xvalues_pass(*this, copy_elision_pass, budget_user);
	create_xvalues_pass(stmts);

	uint64_t t_create_xvalue = 0;
	if (prof) {
		t_create_xvalue = profiling::rdtsc();
		prof->t_create_xvalue += t_create_xvalue - t_copy_elision;
	}

	// generate code and generate simple fragments
	LOG_DEBUG("ByteCodePass");
	ByteCodePass byte_code_pass(*this, create_xvalues_pass, fuse_groups.get(),
		operator_id, budget_manager, budget_user);
	byte_code_pass(stmts);

	uint64_t t_byte_code = 0;
	if (prof) {
		t_byte_code = profiling::rdtsc();
		prof->t_byte_code += t_byte_code - t_create_xvalue;
	}

	if (prof) {
		prof->t_total += t_byte_code - t_start;
		prof->calls++;
	}

	// set context
	block.prepare_interpretation();
}

void
CodeBlockBuilderProfData::to_string(std::ostream& s) const
{
	if (!calls) {
		return;
	}
	s << "prologue " << (t_prologue * 100 / t_total) << "%, ";
	s << "copy_elision " << (t_copy_elision * 100 / t_total) << "%, ";
	s << "create_xvalue " << (t_create_xvalue * 100 / t_total) << "%, ";
	s << "byte_code " << (t_byte_code * 100 / t_total) << "%, ";
	s << "#calls " << calls;
}

