#include "llvm_compiler.hpp"
#include "llvm_fragment.hpp"
#include "llvm_interface.hpp"

#include "system/build.hpp"

#include "engine/voila/compile_request.hpp"
#include "engine/voila/jit.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/query.hpp"
#include "engine/stream.hpp"
#include "engine/table.hpp"
#include "engine/bloomfilter.hpp"
#include "engine/string_utils.hpp"
#include "engine/date_utils.hpp"
#include "engine/util_kernels.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/budget.hpp"

#include "system/build.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"


#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IR/Attributes.h"
#include "llvm/IR/Intrinsics.h"
#include "llvm/IR/IntrinsicsX86.h"

#include "llvm/IR/Metadata.h"

#include <chrono>
#include <thread>
#include <sstream>


#define JIT_IMPROVEMENTS1
#define JIT_IMPROVEMENTS2

// #define USE_UNDEFINED // TODO: enable to get better LLVM optimizations

// #define USE_INTERNAL_KERNEL

using namespace engine;
using namespace voila;
using namespace llvmcompiler;

static llvm::Value* kInvalidPointer = (llvm::Value*)nullptr;

using FlavorSpec = engine::voila::FlavorSpec;

struct VoilaCollectVariables : VoilaTreePass {
	std::unordered_set<Var> assigned;

private:
	void on_statement(const std::shared_ptr<Assign>& b) final {
		assigned.insert(b->var);
		VoilaTreePass::on_statement(b);
	}
};

template<typename T>
static std::string
llvm_to_string(const T& t)
{
	std::string type_str;
	llvm::raw_string_ostream rso(type_str);
	t->print(rso);
	return rso.str();
}

LlvmCompiler::LlvmCompiler(const std::string& func_name,
	LlvmFragment& fragment, CompileRequest& req,
	const std::string& dbg_name)
 : func_name(func_name), dbg_name(dbg_name), fragment(fragment),
	context(fragment.get_context()), module(fragment.llvm_module.get()),
	builder(context), request(req)
{
	using Metadata = llvm::Metadata;
	using MDNode = llvm::MDNode;
	using MDString = llvm::MDString;

	// create noalias domain
	{
		llvm::SmallVector<Metadata*, 1> MDs;
		MDs.push_back(nullptr);

		meta_noalias_dom = MDNode::get(context, MDs);
		meta_noalias_dom->replaceOperandWith(0, meta_noalias_dom);
	}

	// create scope
	{
		llvm::SmallVector<Metadata*, 2> MDs;
		MDs.push_back(nullptr);
		MDs.push_back(meta_noalias_dom);

		meta_noalias_scope = MDNode::get(context, MDs);
		meta_noalias_scope->replaceOperandWith(0, meta_noalias_scope);
	}

	// create scope list
#if 0
	meta_noalias_set = llvm::MDNode::get(context, llvm::ConstantAsMetadata::get(
		llvm::ConstantInt::get(context, llvm::APInt(64, 0, true))));
#else
	{
		llvm::SmallVector<Metadata*, 2> MDs;
		MDs.push_back(meta_noalias_scope);

		meta_noalias_set = MDNode::get(context, MDs);
	}
#endif
}

void
LlvmCompiler::_loop(llvm::Value* var, llvm::Value* num,
	int64_t step, int64_t unroll, int64_t simdization,
	const std::string& name, llvm::Function* func,
	const std::function<void(llvm::Value*, int64_t)> f)
{
	auto task = scheduler::get_current_task();
	scheduler::yield(task);

	LOG_DEBUG("Generate loop with step=%lld unroll=%d", step, (int)unroll);
	ASSERT(step >= 1 && step <= 128);


	auto bb_entry = llvm::BasicBlock::Create(context, "loop_entry", func);
	auto bb_loop = llvm::BasicBlock::Create(context, "loop_body", func);
	auto bb_exit = llvm::BasicBlock::Create(context, "loop_exit", func);

	builder.CreateBr(bb_entry);
	builder.SetInsertPoint(bb_entry);

	auto index = set_noalias_scope(builder.CreateAlignedLoad(builder.getInt64Ty(), var, 8,
		"index_" + name));

	comment_int(builder, "index", index);

	auto check_index = step > 1 ?
		builder.CreateAdd(index, builder.getInt64(step), "", true, true) :
		index;

	ASSERT(check_index->getType()->isIntegerTy(64));
	ASSERT(num->getType()->isIntegerTy());
	ASSERT(num->getType()->isIntegerTy(64));

	auto loop_latch = builder.CreateCondBr(
		create_expect_mostly(builder.CreateICmpSGE(check_index, num), 0),
		bb_exit, bb_loop);

	{
		using Metadata = llvm::Metadata;
		using MDNode = llvm::MDNode;
		using MDString = llvm::MDString;

		llvm::SmallVector<Metadata*, 5> MDs;
		MDs.push_back(nullptr);
#if 0
		// add meta data
		{
			llvm::SmallVector<Metadata*, 1> DisableOperands;
			DisableOperands.push_back(
				MDString::get(context, "llvm.loop.unroll.runtime.disable"));
			MDNode *DisableNode = MDNode::get(context, DisableOperands);
			MDs.push_back(DisableNode);
		}

		{
			llvm::SmallVector<Metadata*, 1> DisableOperands;
			DisableOperands.push_back(
				MDString::get(context, "llvm.loop.unroll.disable"));
			MDNode *DisableNode = MDNode::get(context, DisableOperands);
			MDs.push_back(DisableNode);
		}
#endif
		{
			llvm::SmallVector<Metadata*, 2> DisableOperands;
			DisableOperands.push_back(
				MDString::get(context, "llvm.loop.vectorize.enable"));
			DisableOperands.push_back(
				llvm::ConstantAsMetadata::get(
					llvm::ConstantInt::get(context,
						llvm::APInt(1, simdization != 1 ? 1 : 0, true))));
			MDNode *DisableNode = MDNode::get(context, DisableOperands);
			MDs.push_back(DisableNode);
		}

		{
			llvm::SmallVector<Metadata*, 2> DisableOperands;
			DisableOperands.push_back(
				MDString::get(context, "llvm.loop.vectorize.width"));

			int width = request.flavor->get_int(FlavorSpec::Entry::kUnrollNoSel);
			DisableOperands.push_back(
				llvm::ConstantAsMetadata::get(
					llvm::ConstantInt::get(context,
						llvm::APInt(32, simdization, true))));
			MDNode *DisableNode = MDNode::get(context, DisableOperands);
			MDs.push_back(DisableNode);
		}

		{
			llvm::SmallVector<Metadata*, 2> DisableOperands;
			DisableOperands.push_back(
				MDString::get(context, "llvm.loop.distribute.enable"));
			DisableOperands.push_back(
				llvm::ConstantAsMetadata::get(
					llvm::ConstantInt::get(context, llvm::APInt(1, 0, true))));
			MDNode *DisableNode = MDNode::get(context, DisableOperands);
			MDs.push_back(DisableNode);
		}

		auto NewLoopID = MDNode::get(context, MDs);
		// Set operand 0 to refer to the loop id itself.
		NewLoopID->replaceOperandWith(0, NewLoopID);
		loop_latch->setMetadata("llvm.loop", NewLoopID);
	}
	builder.SetInsertPoint(bb_loop);

	// comment(builder, "loop_body");

	ASSERT(unroll >= 1);
	if (unroll > 1) {
		for (int64_t i=0; i<unroll; i++) {
			f(builder.CreateAdd(index, builder.getInt64(i), "", true, true), step);
			scheduler::yield(task);
		}
	} else {
		f(index, step);
	}

	ASSERT(unroll <= step);

	// index++
	set_noalias(builder.CreateStore(
		builder.CreateAdd(index, builder.getInt64(step), "", true, true),
		var));

	// goto entry
	builder.CreateBr(bb_entry);

	builder.SetInsertPoint(bb_exit);
	// comment(builder, "loop_exit");
}

void
LlvmCompiler::loop(llvm::Value* var, llvm::Value* num,
	int64_t unroll, int64_t simdization,
	const std::string& name, llvm::Function* func,
	const std::function<void(llvm::Value*, int64_t)> f)
{
	ASSERT(num->getType()->isIntegerTy());

	if (unroll > 1) {
		_loop(var, num,
			unroll, unroll,
			simdization,
			name, func, f);

		_loop(var, num, 1, 1,
			1, // do not SIMDize epilogue
			name, func, f);
	} else {
		_loop(var, num, 1, 1,
			simdization,
			name, func, f);
	}
}

static engine::Type*
get_single_type(const std::vector<voila::Context::NodeInfo>& tuple)
{
	ASSERT(tuple.size() == 1);

	return tuple[0].type;
}

template<typename BuilderT>
static llvm::Type*
compile_type(BuilderT& builder, const engine::Type& type,
	bool intermediate = false)
{
	using Tag = engine::Type::Tag;

	auto tag = type.get_tag();

	if (tag == Tag::kPredicate) {
		if (intermediate) {
			return builder.getInt1Ty();
		}

		switch (sizeof(selvector_t)) {
		case sizeof(int64_t):
			tag = Tag::kInt64;
			break;
		case sizeof(int32_t):
			tag = Tag::kInt32;
			break;
		default:
			ASSERT(false && "Invalid predicate base type");
			break;
		}
	}

	switch (tag) {
	case Tag::kInt8:
		return builder.getInt8Ty();
	case Tag::kInt16:
		return builder.getInt16Ty();
	case Tag::kInt32:
		return builder.getInt32Ty();
	case Tag::kPosition:
	case Tag::kBucket:
	case Tag::kString:
		return builder.getInt8PtrTy();
	case Tag::kInt64:
	case Tag::kHash:
		return builder.getInt64Ty();

	case Tag::kInt128:
		return builder.getInt128Ty();
	case Tag::kEmpty:
		return nullptr;
	default:
		LOG_ERROR("Compiler does not support type '%s'",
			type.to_cstring());
		THROW_MSG(CompilerException, "Unsupported type");
		return nullptr;
	}
}

static void
check_ios(CPrimArg* arg, CPrimArg::IO& io, XValue* xval, bool print, bool source)
{
	const char* prefix = source ? "source" : "sink";

	ASSERT(xval);
	bool is_vector = xval->tag == XValue::Tag::kVector;
	bool is_predicate = xval->tag == XValue::Tag::kPredicate;
	bool is_position = xval->tag == XValue::Tag::kPosition;

	std::ostringstream s;
	if (false && print) {
		xval->dbg_print(s);
	}
	LOG_DEBUG("%s: xval=%p  %s base %p, num %lld, xval type '%s', predicate %d, vector %d, '%s', %s",
		prefix, xval, (io.base ? "" : " NO"),
		(io.base ? *io.base : nullptr), *io.num,
		(xval->get_data_type() ? xval->get_data_type()->to_cstring() : "?"),
		is_predicate, is_vector, xval->dbg_name,
		s.str().c_str());
	ASSERT(*io.num <= (int64_t)arg->code_block->vector_size);
	ASSERT(is_vector || is_predicate || is_position);

	xval->validate();

	if (!source) {
		ASSERT(!xval->match_flags(XValue::kConstVector));
	}
}

#if 1
static void
check_source(CPrimArg* arg, CPrimArg::IO& io, XValue* xval, bool print)
{
	check_ios(arg, io, xval, print, true);
}

static void
check_sink(CPrimArg* arg, CPrimArg::IO& io, XValue* xval, bool print)
{
	check_ios(arg, io, xval, print, false);
}
#endif

extern "C" void
llvm_compiler_print_hash_table(CPrimArg* arg,
	const char* fun_name, const char* dbg_name)
{
	LOG_TRACE("%s(%s): begin arg=%p", fun_name, dbg_name, arg);

	for (int64_t s=0; s<arg->num_structs; s++) {
		const auto& ds = arg->structs[s];
		LOG_DEBUG("Struct %llu, mask %p, buckets %p", s,
			*ds.mask, *ds.buckets);

		void** buckets = (void**)(*ds.buckets);
		for (size_t i=0; i<*ds.mask+1; i++) {
			if (!buckets[i]) {
				continue;
			}
			LOG_DEBUG("Bucket[%lli] = %p", i, buckets[i]);

			auto p = (int64_t*)(buckets[i]);
			LOG_DEBUG("ival %lld", *p);
		}
	}
}

extern "C" void
llvm_compiler_begin_prim(CPrimArg* arg,
	const char* fun_name, const char* dbg_name)
{
	JitPrimArg* jit_arg = (JitPrimArg*)arg;
	LOG_DEBUG("%s(%s): begin arg=%p '%s'",
		fun_name, dbg_name, arg, jit_arg->dbg_name.c_str());

#if 1
	bool print = true;
	for (int64_t i=0; i<arg->num_sources; i++) {
		check_source(arg, arg->sources[i], arg->sources[i].xvalue, print);
	}

	for (int64_t i=0; i<arg->num_sinks; i++) {
		check_sink(arg, arg->sinks[i], arg->sinks[i].xvalue, print);
	}

	for (int64_t i=0; i<arg->num_structs; i++) {
		const auto& s = arg->structs[i];
		ASSERT(s.buckets && s.mask);
		// ASSERT(*s.buckets);

		LOG_DEBUG("struct %lld: buckets %p, mask %p",
			i, *s.buckets, *s.mask);
	}
#endif
}

extern "C" void
llvm_compiler_end_prim(CPrimArg* arg,
	const char* fun_name, const char* dbg_name)
{
	JitPrimArg* jit_arg = (JitPrimArg*)arg;
	LOG_DEBUG("%s(%s): end arg=%p '%s'",
		fun_name, dbg_name, arg, jit_arg->dbg_name.c_str());

#if 1
	bool print = false;
	for (int64_t i=0; i<arg->num_sources; i++) {
		check_source(arg, arg->sources[i], arg->sources[i].xvalue, print);
	}

	for (int64_t i=0; i<arg->num_sinks; i++) {
		check_sink(arg, arg->sinks[i], arg->sinks[i].xvalue, print);
	}
#endif
}

extern "C" int64_t
llvm_compiler_can_full_eval(CPrimArg* carg, selvector_t* predicate, int64_t num,
	int64_t complexity, int64_t sum_bits)
{
	auto arg = (JitPrimArg*)carg;
	bool config_enabled = arg->has_full_evaluation;

	if (!num || !config_enabled) {
		LOG_TRACE("can_full_eval: no");
		return -1;
	}

	DBG_ASSERT(predicate);
	auto start_idx = predicate[0];
	auto stop_idx = predicate[num-1];

	DBG_ASSERT(start_idx <= stop_idx && complexity >= 0);

	// dense, no speculation required
	if (stop_idx+1 == num) {
		LOG_TRACE("can_full_eval: yes, dense");
		return num;
	}

	if (complexity <= 0) {
		LOG_TRACE("can_full_eval: impossible");
		return -1;
	}

	if (arg->min_full_evaluation_vectorsize <= 0) {
		auto& config = *carg->code_block->stream.query.config;
		int64_t vsize = carg->code_block->vector_size;
		double score_divisor = arg->full_evaluation_bit_score_divisor;
		ASSERT(score_divisor >= 0.0 && std::isfinite(score_divisor));

		double avg_bits = (double)sum_bits / (double)complexity;

		double score = avg_bits / score_divisor;

		score = std::max(1.0, score);

		int64_t vmin = (score * (double)vsize) / (score + 1.0);
		vmin = std::min(vsize, vmin);

		ASSERT(vmin <= vsize && vmin >= 0);
		arg->min_full_evaluation_vectorsize = vmin;

		LOG_DEBUG("can_full_eval: set vmin=%lld with num=%lld, vsize=%lld, "
			"avg_bits=%f, score=%f, complexity=%lld",
			vmin, num, vsize,
			avg_bits, score, complexity);
	}

	// sparse
	if (num >= arg->min_full_evaluation_vectorsize) {
		LOG_TRACE("can_full_eval: yes, sparse");
		return stop_idx+1;
	}

	return -1;
}

extern "C" char*
llvm_compiler_htappend1(CPrimArg* arg, void** out_flag, uint64_t* out_mask,
	char* structure, uint64_t hash)
{
	auto table = (engine::table::IHashTable*)structure;
	const size_t num = 1;
	uint64_t ptr_begin = 0;
	uint64_t ptr_end = 0;
	int64_t numa_node = arg->code_block->next_context->numa_node;
	auto old_hash_index = table->get_hash_index();
	auto old_mask = table->get_hash_index_mask();

	ASSERT(out_flag && out_mask);
	ASSERT(old_hash_index == *out_flag);
	ASSERT(old_mask == *out_mask);

	LOG_DEBUG("llvm_compiler_htappend1: struct %p, hash %llu, numa_node %lld, "
		"out_flag %p, hash_index %p",
		structure, hash, numa_node, out_flag, old_hash_index);

	// allocate new bucket
	auto block = table->hash_append_prealloc(num, numa_node);
	table->get_current_block_range((char**)&ptr_begin, (char**)&ptr_end, num);
	table->hash_append_prune(block, num);

	// add new group to hash table
	void** hash_index = table->get_hash_index();
	auto mask = table->get_hash_index_mask();

	*out_flag = hash_index;
	*out_mask = mask;

	auto& value = hash_index[hash & mask];

	char* bucket = (char*)ptr_begin;
	void** next = (void**)((uintptr_t)bucket + table->get_next_offset());

	*next = value;
	value = bucket;

	return bucket;
}

extern "C" char*
llvm_compiler_writepos1(CPrimArg* arg, char* structure)
{
	auto table = (engine::table::IHashTable*)structure;

	const auto thread_id = arg->code_block->next_context->thread_id;
	const auto numa_node = arg->code_block->next_context->numa_node;

	size_t num = 1;

	LOG_TRACE("llvm_compiler_writepos1: struct %p, numa_node %lld, thread_id %lld, num %p",
		structure, numa_node, thread_id, num);

	size_t offset = 0;
	char* block_data = nullptr;
	table->get_write_pos(offset, block_data, num, thread_id, numa_node);
	ASSERT(block_data);

	LOG_TRACE("llvm_compiler_writepos1: block_data %p, offset %llu, num %llu", block_data, offset, num);

	return block_data + offset;
}

static engine::table::Block*
_table_prealloc(engine::table::IHashTable& table, size_t num,
	size_t thread_id, int64_t numa_node,
	int64_t** out_curr_num, int64_t* out_max_num)
{
	auto& space = table.m_write_partitions[thread_id];
	auto b = space->prealloc(num, numa_node);

	ASSERT(b);

	*out_curr_num = (int64_t*)&b->num;
	*out_max_num = (int64_t)b->capacity;

	ASSERT(num > 0);
	ASSERT(b->capacity >= b->num + num);

	return b;
}

extern "C" char*
llvm_compiler_alloc_write_pos_chunk(CPrimArg* arg, char* structure,
	int64_t** out_curr_num, int64_t* out_max_num)
{
	auto table = (engine::table::IHashTable*)structure;
	size_t num = 1;

	const auto thread_id = arg->code_block->next_context->thread_id;
	const auto numa_node = arg->code_block->next_context->numa_node;

	LOG_DEBUG("llvm_compiler_alloc_write_pos_chunk: struct %p, numa_node %lld, thread_id %lld",
		structure, numa_node, thread_id);

	auto b = _table_prealloc(*table, num, thread_id, numa_node, out_curr_num, out_max_num);

	LOG_DEBUG("llvm_compiler_alloc_write_pos_chunk: data %p, num %llu, pnum %p, "
		"capacity %llu, width %p",
		b->data, b->num, &b->num, b->capacity, b->width);

	return b->data;
}


extern "C" char*
llvm_compiler_alloc_bucket_insert(CPrimArg* arg, char* structure,
	int64_t** out_ht_count, int64_t* out_ht_capacity,
	int64_t** out_curr_num, int64_t* out_max_num,
	void** out_flag, uint64_t* out_mask)
{
	auto table = (engine::table::IHashTable*)structure;
	size_t num = 1;

	const auto thread_id = arg->code_block->next_context->thread_id;
	const auto numa_node = arg->code_block->next_context->numa_node;

	LOG_DEBUG("llvm_compiler_alloc_bucket_insert: struct %p, numa_node %lld, thread_id %lld",
		structure, numa_node, thread_id);

	table->potentially_rebuild(num, numa_node);

	void** hash_index = table->get_hash_index();
	auto mask = table->get_hash_index_mask();

	*out_flag = hash_index;
	*out_mask = mask;
	*out_ht_count = (int64_t*)&table->hash_index_tuple_counter_seq;
	*out_ht_capacity =
		table->hash_index_capacity/engine::table::IHashTable::kFillFactorMul + 1;

	auto b = _table_prealloc(*table, num, 0, numa_node, out_curr_num, out_max_num);

	LOG_DEBUG("llvm_compiler_alloc_bucket_insert: data %p, num %llu, pnum %p, "
		"capacity %llu, width %p, hash_index %p, hash_mask %p",
		b->data, b->num, &b->num, b->capacity, b->width, hash_index, (void*)mask);

	return b->data;
}

extern "C" void
llvm_compiler_alloc_bucket_insert_dbg_update(CPrimArg* arg, char* structure,
	uint64_t hash, void* bucket, uint64_t mask, void** buckets)
{
	auto table = (engine::table::IHashTable*)structure;

	void** exp_hash_index = table->get_hash_index();
	auto exp_mask = table->get_hash_index_mask();

	ASSERT(mask == exp_mask);
	ASSERT(buckets == exp_hash_index);

	auto& value = buckets[hash & mask];

	void** next = (void**)((uintptr_t)bucket + table->get_next_offset());

	*next = value;
	value = bucket;
}


extern "C" void
llvm_compiler_check_xvalue(CPrimArg* arg,
	XValue* xvalue, int64_t type_tag, const char* msg,
	const char* fun_name, const char* dbg_name)
{
	ASSERT(xvalue);

	auto type = xvalue->get_data_type();
	if (!type) {
		return;
	}
	ASSERT(type);

	if (type_tag != type->get_tag()) {
		LOG_ERROR("%s(%s): compiled tag %d does not match xvalue %d in %s",
			fun_name, dbg_name, type_tag, type->get_tag(), msg);
		arg->fail = true;
	}

	if (type->is_predicate()) {
		engine::util_kernels::assert_valid_selection_vector<selvector_t>(
			(selvector_t*)xvalue->first,
			xvalue->num,
			[&] (auto k) {
				LOG_ERROR("Invalid index in selection vector at %lld",
					(long)k);
				ASSERT(false);
		});
	}
}

extern "C" void
llvm_compiler_check_struct(CPrimArg* arg,
	int64_t index, const char* signature,
	const char* fun_name, const char* dbg_name)
{
	JitPrimArg* jit_arg = (JitPrimArg*)arg;
	ASSERT(index >= 0);
	ASSERT(index <= arg->num_structs);
	ASSERT(index < jit_arg->cpp_struct_signatures.size());

	ASSERT(arg->structs[index].structure);

	const char* c_str = jit_arg->cpp_struct_signatures[index].c_str();
	if (strcmp(c_str, signature)) {
		LOG_ERROR("%s(%s): Invalid row_type. compiled = '%s', given = '%s'",
			fun_name, dbg_name, signature, c_str);
		arg->fail = true;
	}
}

extern "C" void
llvm_compiler_check_prim(CPrimArg* arg,
	int64_t read_num_sources, int64_t read_num_sinks,
	int64_t read_num_structs,

	int64_t gen_num_sources, int64_t gen_num_sinks,
	int64_t gen_num_source_preds, int64_t gen_num_sink_preds,
	const char* func_name, const char* dbg_name)
{
	if (arg->fail) {
		LOG_ERROR("%s(%s): failed", func_name, dbg_name);
		ASSERT(!arg->fail);
	}

	LOG_DEBUG("%s(%s): llvm prim with %lld sources (%lld predicates) "
		"%lld sinks (%lld predicates)",
		func_name, dbg_name, gen_num_sources, gen_num_source_preds,
		gen_num_sinks, gen_num_sink_preds);

	ASSERT(arg->num_sources == read_num_sources);
	ASSERT(arg->num_sinks == read_num_sinks);

	ASSERT(read_num_sources == gen_num_sources);
	ASSERT(read_num_sinks == gen_num_sinks);

	ASSERT(gen_num_source_preds <= 1);

	selvector_t* sel = nullptr;
	for (int64_t i=0; i<arg->num_sources; i++) {
		auto xval = arg->sources[i].xvalue;
		if (!xval) {
			continue;
		}
		if (xval->tag != XValue::Tag::kPredicate) {
			continue;
		}
		ASSERT(xval->get_data_type());
		ASSERT(xval->get_data_type()->is_predicate());
		sel = (selvector_t*)xval->first;

		engine::util_kernels::debug_assert_valid_selection_vector<selvector_t>(
			sel, xval->num);
	}

	for (int64_t i=0; i<arg->num_sources; i++) {
		auto xval = arg->sources[i].xvalue;
		if (!xval) {
			continue;
		}
		if (xval->tag != XValue::Tag::kVector) {
			continue;
		}
		if (!xval->get_data_type()) {
			LOG_ERROR("llvm_compiler: xvalue %p (first %p) does not have data type",
				xval, xval ? xval->first : nullptr);
		}
		ASSERT(xval->get_data_type());
		if (xval->get_data_type()->is_var_len()) {
			char** data = (char**)xval->first;
			if (sel) {
				for (size_t i=0; i<xval->num; i++) {
					ASSERT(data[i]);
				}
			} else {
				for (size_t i=0; i<xval->num; i++) {
					ASSERT(data[sel[i]]);
				}
			}
		}
	}
}

extern "C" int32_t
llvm_compiler_strcmp(const char* a, const char* b)
{
	LOG_TRACE("llvm_compiler_strcmp: %s (%p) vs %s (%p)", a, a, b, b);
	return StringUtils::compare(a, b);
}

extern "C" int32_t
llvm_compiler_strncmp(const char* a, const char* b, uint32_t num)
{
	LOG_TRACE("llvm_compiler_strncmp: %s vs %s", a, b);
	static_assert(sizeof(uint32_t) == sizeof(StringHeap::length_t), "Expect int32");
	return StringUtils::compare_with_len(a, b, num);
}

extern "C" uint64_t
llvm_compiler_strhash(const char* a)
{
	auto phash = StringHeap::str_get_hash_ptr(a);

#ifdef IS_DEBUG_BUILD
	size_t len = StringHeap::heap_strlen(a);
	auto h2 = StringUtils::hash(a, len);
	ASSERT(*phash == h2);
#endif
	if (*phash == 0) {
		size_t len = StringHeap::heap_strlen(a);
		*phash = StringUtils::hash(a, len);
	}

	return *phash;
}

extern "C" int32_t
llvm_compiler_strcontains(const char* a, uint32_t len_a, const char* b,
	uint32_t len_b)
{
	static_assert(sizeof(uint32_t) == sizeof(StringHeap::length_t), "Expect int32");

	return StringUtils::contains(a, len_a, b, len_b);
}

extern "C" int16_t
llvm_compiler_extract_year(int32_t date)
{
	unsigned year,month,day;
	DateUtils::splitJulianDay(date, year, month, day);
	return year;
}

extern "C" void
llvm_compiler_comment(CPrimArg* arg, const char* comment)
{
	LOG_INFO("comment %s", comment);
}

extern "C" void
llvm_compiler_comment_ptr(CPrimArg* arg, const char* comment, void* ptr)
{
	int64_t* pval = (int64_t*)ptr;
	LOG_INFO("comment pointer %s, %p", comment, ptr);
	if (false && pval) {
		LOG_INFO("comment deref pointer %s, %p", comment, *pval);
	}
}

extern "C" void
llvm_compiler_comment_ptr_nonull(CPrimArg* arg, const char* comment, void* ptr)
{
	LOG_INFO("comment pointer %s, %p", comment, ptr);
	ASSERT(ptr);
}

extern "C" void
llvm_compiler_comment_int(CPrimArg* arg, const char* comment, int64_t val)
{
	LOG_INFO("comment int %s, %lld", comment, val);
}

template<typename T>
static bool
arith_cast2(T& builder, llvm::Value*& a_val, llvm::Value*& b_val,
	llvm::Type* r_tpe = nullptr, bool* casted_to_result_type = nullptr)
{
	auto a_tpe = a_val->getType();
	auto b_tpe = b_val->getType();

	int64_t final_bits;
	llvm::Type* final_type;

	ASSERT(a_tpe->isIntegerTy() && b_tpe->isIntegerTy());
	auto a_bits = a_tpe->getIntegerBitWidth();
	auto b_bits = b_tpe->getIntegerBitWidth();

	final_type = a_tpe;
	final_bits = a_bits;
	if (b_bits > final_bits) {
		final_type = b_tpe;
		final_bits = b_bits;
	}

	if (r_tpe) {
		ASSERT(r_tpe->isIntegerTy());
		auto result_bits = r_tpe->getIntegerBitWidth();

		if (result_bits > final_bits) {
			final_type = r_tpe;
			final_bits = result_bits;
		}
	}

	if (casted_to_result_type) {
		*casted_to_result_type = final_type == r_tpe;
	}

	// cast
	a_val = builder.CreateSExtOrTrunc(a_val, final_type);
	b_val = builder.CreateSExtOrTrunc(b_val, final_type);
	return true;
}

template<typename T>
static llvm::Type*
getPointerFittingIntegerType(T& builder)
{
	switch (sizeof(void*)) {
	case 8:
		return builder.getInt64Ty();
	case 4:
		return builder.getInt32Ty();

	default:
		ASSERT(false && "Does not support pointer type");
		return nullptr;
	}
}

template<typename T>
static llvm::Value*
castPointerToInteger(T& builder, llvm::Value* val)
{
	if (val->getType()->isPointerTy()) {
		return builder.CreateBitOrPointerCast(val,
			getPointerFittingIntegerType(builder));
	}
	return val;
}

template<typename T>
static bool
compare_cast2(T& builder, llvm::Value*& a_val, llvm::Value*& b_val)
{
	// get rid of pointers due to BucketType -> i8*
	a_val = castPointerToInteger(builder, a_val);
	b_val = castPointerToInteger(builder, b_val);

	return arith_cast2(builder, a_val, b_val);
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_print_hash_table()
{
	if (!llvm_compiler_print_hash_table) {
		llvm_compiler_print_hash_table =
			_fun_props(llvm::Function::Create(prim_func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_print_hash_table", module));
	}

	return llvm_compiler_print_hash_table;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_comment()
{
	if (!llvm_compiler_comment) {
		auto func_type = llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt8PtrTy() }, false);

		llvm_compiler_comment =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_comment", module));
	}

	return llvm_compiler_comment;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_comment_ptr()
{
	if (!llvm_compiler_comment_ptr) {
		auto func_type = llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt8PtrTy(), builder.getInt8PtrTy()}, false);

		llvm_compiler_comment_ptr =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
			"llvm_compiler_comment_ptr", module));
	}

	return llvm_compiler_comment_ptr;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_comment_ptr_nonull()
{
	if (!llvm_compiler_comment_ptr_nonull) {
		auto func_type = llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt8PtrTy(), builder.getInt8PtrTy()}, false);

		llvm_compiler_comment_ptr_nonull =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
			"llvm_compiler_comment_ptr_nonull", module));
	}

	return llvm_compiler_comment_ptr_nonull;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_comment_int()
{
	if (!llvm_compiler_comment_int) {
		auto func_type = llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt8PtrTy(), builder.getInt64Ty()}, false);

		llvm_compiler_comment_int =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_comment_int", module));
	}

	return llvm_compiler_comment_int;
}


llvm::Function*
LlvmCompiler::get_llvm_compiler_strcmp()
{
	auto create_strcmp = [&] (const auto& name, auto func_type) {
		auto func = llvm::Function::Create(func_type, llvm::Function::PrivateLinkage,
			name, module);

		auto arg0 = func->getArg(0);
		auto arg1 = func->getArg(1);

		auto bb_entry = llvm::BasicBlock::Create(context, "strcmp_entry", func);
		builder.SetInsertPoint(bb_entry);

		auto bb_header = llvm::BasicBlock::Create(context, "strcmp_header", func);
		builder.CreateBr(bb_header);

		builder.SetInsertPoint(bb_header);
		auto phi = builder.CreatePHI(builder.getInt64Ty(), 2, "index");
		phi->addIncoming(builder.getInt64(0), bb_entry);

#if 1
		auto cpu_info = build::CpuInfo::get();
#ifdef __SSE4_2__
		if (cpu_info.has_x86_sse42) {
			// adapted from https://github.com/WojciechMula/simd-string/blob/master/strcmp.cpp
			using X86Intrinsics = llvm::Intrinsic::X86Intrinsics;
			auto cmpistrc = llvm::Intrinsic::getDeclaration(module, X86Intrinsics::x86_sse42_pcmpistric128);
			auto cmpistri = llvm::Intrinsic::getDeclaration(module, X86Intrinsics::x86_sse42_pcmpistri128);
			auto cmpistrz = llvm::Intrinsic::getDeclaration(module, X86Intrinsics::x86_sse42_pcmpistriz128);

			auto mode = builder.getInt8(_SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH |
				_SIDD_NEGATIVE_POLARITY | _SIDD_LEAST_SIGNIFICANT);

			auto llvm_v16i8_ty = llvm::VectorType::get(builder.getInt8Ty(), 16);

			auto bb_next_iter = llvm::BasicBlock::Create(context, "strcmp_next_iter", func);
			builder.SetInsertPoint(bb_next_iter);
			phi->addIncoming(builder.CreateAdd(phi,
				builder.getInt64(16)), bb_next_iter);
			builder.CreateBr(bb_header);

			builder.SetInsertPoint(bb_header);
			auto load = [&] (llvm::Value* ptr) -> llvm::Value* {
				return builder.CreateAlignedLoad(builder.CreateBitOrPointerCast(ptr,
					llvm::PointerType::getUnqual(llvm_v16i8_ty)), 1);
			};

			auto a = load(builder.CreateGEP(arg0, phi));
			auto b = load(builder.CreateGEP(arg1, phi));
			auto c = builder.CreateCall(cmpistrc, {a, b, mode});

			auto bb_c_true = llvm::BasicBlock::Create(context, "c_true", func);
			auto bb_c_false = llvm::BasicBlock::Create(context, "c_false", func);

			builder.CreateCondBr(builder.CreateIsNotNull(c), bb_c_true, bb_c_false);

			builder.SetInsertPoint(bb_c_true);

			{
				auto idx = builder.CreateAdd(
					builder.CreateSExtOrTrunc(phi, builder.getInt32Ty()),
					builder.CreateCall(cmpistri, {a, b, mode}));

				auto b0 = builder.CreateGEP(arg0, idx);
				auto b1 = builder.CreateGEP(arg1, idx);

				builder.CreateRet(builder.CreateSExtOrTrunc(
					builder.CreateSub(builder.CreateLoad(b0), builder.CreateLoad(b1)),
					builder.getInt32Ty()));
			}

			auto bb_ret0 = llvm::BasicBlock::Create(context, "bb_ret0", func);
			builder.SetInsertPoint(bb_ret0);
			builder.CreateRet(builder.getInt32(0));


			builder.SetInsertPoint(bb_c_false);
			auto d = builder.CreateCall(cmpistrz, {a, b, mode});
			builder.CreateCondBr(builder.CreateIsNotNull(d),
				bb_ret0, bb_next_iter);

			return func;
		}
#endif
#endif

		auto bb_body = llvm::BasicBlock::Create(context, "strcmp_body", func);
		auto bb_footer = llvm::BasicBlock::Create(context, "strcmp_footer",	func);



		auto deref0 = builder.CreateLoad(builder.CreateGEP(arg0, phi));
		auto deref1 = builder.CreateLoad(builder.CreateGEP(arg1, phi));

		auto cond = builder.CreateAnd(builder.CreateIsNotNull(deref0),
			builder.CreateICmpEQ(deref0, deref1));

		builder.CreateCondBr(cond, bb_body, bb_footer);



		builder.SetInsertPoint(bb_body);

		auto increment = builder.CreateAdd(phi, builder.getInt64(1));
		phi->addIncoming(increment, bb_body);

		builder.CreateBr(bb_header);



		builder.SetInsertPoint(bb_footer);

		builder.CreateRet(builder.CreateSExtOrTrunc(
			builder.CreateSub(deref0, deref1),
			builder.getInt32Ty()));
		return func;
	};


	if (!llvm_compiler_strcmp) {
		auto func_type = llvm::FunctionType::get(builder.getInt32Ty(),
			{ str_type, str_type }, false);
#if 0
		llvm_compiler_strcmp =
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_strcmp", module);
#else
		auto old = builder.GetInsertBlock();
		llvm_compiler_strcmp = create_strcmp("jit_strcmp", func_type);
		builder.SetInsertPoint(old);
#endif
		_fun_props(llvm_compiler_strcmp);
	}

	return llvm_compiler_strcmp;
}


llvm::Function*
LlvmCompiler::get_llvm_compiler_streq()
{
	auto create_streq = [&] (const auto& name, auto func_type) {
		auto func = llvm::Function::Create(func_type, llvm::Function::PrivateLinkage,
			name, module);

		auto a_str = func->getArg(0);
		auto b_str = func->getArg(1);
		auto a_len = func->getArg(2);

		auto unroll_val = builder.getInt32(4);
		auto remainder_val = builder.getInt32(1);

		auto bb_entry = llvm::BasicBlock::Create(context, "streq_entry", func);
		builder.SetInsertPoint(bb_entry);

		auto bb_unequal = llvm::BasicBlock::Create(context, "streq_ne", func);
		builder.SetInsertPoint(bb_unequal);
		builder.CreateRet(builder.getInt32(1));

		auto bb_equal = llvm::BasicBlock::Create(context, "streq_eq", func);
		builder.SetInsertPoint(bb_equal);
		builder.CreateRet(builder.getInt32(0));

		builder.SetInsertPoint(bb_entry);

#if 1
		auto bb_header_unr = llvm::BasicBlock::Create(context, "streq_header_unr", func);
		auto bb_loop_unr = llvm::BasicBlock::Create(context, "streq_loop_unr", func);
#endif

#if 0
		auto bb_header_rem = llvm::BasicBlock::Create(context, "streq_header_rem", func);
		auto bb_loop_rem = llvm::BasicBlock::Create(context, "streq_loop_rem", func);
#endif
		builder.CreateCondBr(
			builder.CreateICmpSLT(a_len, builder.getInt32(0)),
			bb_unequal, bb_header_unr);

#if 1
		builder.SetInsertPoint(bb_header_unr);
		auto phi = builder.CreatePHI(builder.getInt32Ty(), 2, "index");
		phi->addIncoming(builder.getInt32(0), bb_entry);

		builder.CreateCondBr(
			builder.CreateICmpSLT(phi, a_len),
			bb_loop_unr, bb_equal);

		builder.SetInsertPoint(bb_loop_unr);
		{
			auto a = builder.CreateAlignedLoad(
				builder.CreatePointerCast(builder.CreateGEP(a_str, phi),
					llvm::PointerType::getUnqual(builder.getInt32Ty())),
				1);
			auto b = builder.CreateAlignedLoad(
				builder.CreatePointerCast(builder.CreateGEP(b_str, phi),
					llvm::PointerType::getUnqual(builder.getInt32Ty())),
				1);

			phi->addIncoming(builder.CreateAdd(unroll_val, phi),
				bb_loop_unr);

			builder.CreateCondBr(builder.CreateICmpNE(a, b),
				bb_unequal, bb_header_unr);
		}
#endif

#if 0
		builder.SetInsertPoint(bb_header_rem);
		auto phi2 = builder.CreatePHI(builder.getInt32Ty(), 2, "index");
		phi2->addIncoming(phi, bb_header_unr);
		phi = phi2;

		builder.CreateCondBr(
			builder.CreateICmpSLT(builder.CreateAdd(phi, remainder_val), a_len),
			bb_loop_rem, bb_equal);

		builder.SetInsertPoint(bb_loop_rem);
		{
			auto a = builder.CreateLoad(builder.CreateGEP(a_str, phi));
			auto b = builder.CreateLoad(builder.CreateGEP(b_str, phi));

			phi->addIncoming(builder.CreateAdd(remainder_val, phi),
				bb_loop_rem);

			builder.CreateCondBr(builder.CreateICmpNE(a, b),
				bb_unequal, bb_header_rem);
		}
#endif

		return func;
	};


	if (!llvm_compiler_streq) {
		auto func_type = llvm::FunctionType::get(builder.getInt32Ty(),
			{ str_type, str_type, builder.getInt32Ty() }, false);

		auto old = builder.GetInsertBlock();
		llvm_compiler_streq = create_streq("jit_streq", func_type);
		builder.SetInsertPoint(old);

		_fun_props(llvm_compiler_streq);
	}

	return llvm_compiler_streq;
}


llvm::Function*
LlvmCompiler::get_llvm_compiler_strncmp()
{
	if (!llvm_compiler_strncmp) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt32Ty(),
				{ str_type, str_type, get_heap_length_t(builder) }, false);

		llvm_compiler_strncmp =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_strncmp", module));

		_fun_props(llvm_compiler_strncmp);
	}

	return llvm_compiler_strncmp;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_strhash()
{
	if (!llvm_compiler_strhash) {
		auto func_type = llvm::FunctionType::get(builder.getInt64Ty(),
			{ str_type }, false);

		llvm_compiler_strhash =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_strhash", module));
	}

	return llvm_compiler_strhash;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_strcontains()
{
	if (!llvm_compiler_strcontains) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt32Ty(),
				{ str_type, get_heap_length_t(builder),
					str_type, get_heap_length_t(builder) }, false);

		llvm_compiler_strcontains =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_strcontains", module));
	}

	return llvm_compiler_strcontains;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_memmem()
{
	if (!llvm_compiler_memmem) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt8PtrTy(),
				{ str_type, builder.getInt64Ty(),
					str_type, builder.getInt64Ty() }, false);

		llvm_compiler_memmem =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"memmem", module));
	}

	return llvm_compiler_memmem;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_extract_year()
{
	if (!llvm_compiler_extract_year) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt16Ty(),
				{ builder.getInt32Ty() }, false);

		llvm_compiler_extract_year =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_extract_year", module));
	}
	return llvm_compiler_extract_year;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_htappend1()
{
	if (!llvm_compiler_htappend1) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt8PtrTy(), {
					prim_arg_ptr_type,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),
					llvm::PointerType::getUnqual(
						builder.getInt64Ty()),
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),
					builder.getInt64Ty() },
				false);

		llvm_compiler_htappend1 = _fun_props(
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_htappend1", module),
			false);
	}
	return llvm_compiler_htappend1;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_writepos1()
{
	if (!llvm_compiler_writepos1) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt8PtrTy(), {
					prim_arg_ptr_type,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy()))
				},
				false);

		llvm_compiler_writepos1 = _fun_props(
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_writepos1", module),
			false);
	}
	return llvm_compiler_writepos1;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_alloc_write_pos_chunk()
{
	if (!llvm_compiler_alloc_write_pos_chunk) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt8PtrTy(), {
					prim_arg_ptr_type,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(builder.getInt64Ty())),
					llvm::PointerType::getUnqual(builder.getInt64Ty())
				},
				false);

		llvm_compiler_alloc_write_pos_chunk = _fun_props(
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_alloc_write_pos_chunk", module),
			false);
	}
	return llvm_compiler_alloc_write_pos_chunk;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_alloc_bucket_insert()
{
	if (!llvm_compiler_alloc_bucket_insert) {
		auto func_type =
			llvm::FunctionType::get(builder.getInt8PtrTy(), {
					prim_arg_ptr_type,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),

					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(builder.getInt64Ty())),
					llvm::PointerType::getUnqual(builder.getInt64Ty()),

					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(builder.getInt64Ty())),
					llvm::PointerType::getUnqual(builder.getInt64Ty()),

					llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),
					llvm::PointerType::getUnqual(
						builder.getInt64Ty()),
				},
				false);

		llvm_compiler_alloc_bucket_insert = _fun_props(
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_alloc_bucket_insert", module),
			false);
	}
	return llvm_compiler_alloc_bucket_insert;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_alloc_bucket_insert_dbg_update()
{
	if (!llvm_compiler_alloc_bucket_insert_dbg_update) {
		auto func_type =
			llvm::FunctionType::get(builder.getVoidTy(), {
					prim_arg_ptr_type,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							builder.getInt8PtrTy())),

					builder.getInt64Ty(),
					builder.getInt8PtrTy(),

					builder.getInt64Ty(),
					llvm::PointerType::getUnqual(builder.getInt8PtrTy())
				},
				false);

		llvm_compiler_alloc_bucket_insert_dbg_update = _fun_props(
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_alloc_bucket_insert_dbg_update", module),
			false);
	}
	return llvm_compiler_alloc_bucket_insert_dbg_update;
}

llvm::Function*
LlvmCompiler::get_llvm_compiler_can_full_eval()
{
	if (!llvm_compiler_can_full_eval) {
		auto char_array_type = llvm::PointerType::getUnqual(builder.getInt8Ty());

		auto func_type =
			llvm::FunctionType::get(builder.getInt64Ty(),
				{ prim_arg_ptr_type, char_array_type, builder.getInt64Ty(),
					builder.getInt64Ty(), builder.getInt64Ty() }, false);

		llvm_compiler_can_full_eval =
			_fun_props(llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_can_full_eval", module));
	}
	return llvm_compiler_can_full_eval;
}

void
LlvmCompiler::_fun_add_properties(llvm::Function& func, bool readonly) const
{
	if (readonly) {
		func.setOnlyReadsMemory();
	}
	// func.setOnlyAccessesArgMemory();
	func.setOnlyAccessesInaccessibleMemOrArgMem();
	func.setDoesNotFreeMemory();
	func.setDoesNotRecurse();
}

void
LlvmCompiler::install_functions()
{
	auto int64_ptr = llvm::PointerType::getUnqual(builder.getInt64Ty());

	auto char_array_type = llvm::PointerType::getUnqual(builder.getInt8Ty());

	str_type = char_array_type;

	prim_arg_io_type = llvm::StructType::create(context, {
		llvm::PointerType::getUnqual(char_array_type), // &base
		int64_ptr, // &num
		int64_ptr, // &offset
		char_array_type, // xvalue
	}, "CPrimArg_IO");
	LOG_TRACE("CPrimArg_IO %s", llvm_to_string(prim_arg_io_type).c_str());
	prim_arg_io_ptr_type = llvm::PointerType::getUnqual(prim_arg_io_type);

	prim_arg_struct_type = llvm::StructType::create(context, {
		int64_ptr, // mask
		llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(builder.getInt8PtrTy())), // buckets
		llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(builder.getInt8PtrTy())), // struct

		int64_ptr, // bf_mask
		llvm::PointerType::getUnqual(int64_ptr), // bf_words
	}, "CPrimArg_Struct");
	LOG_TRACE("CPrimArg_Struct %s", llvm_to_string(prim_arg_struct_type).c_str());
	prim_arg_struct_ptr_type = llvm::PointerType::getUnqual(prim_arg_struct_type);

	prim_arg_type = llvm::StructType::create(context, {
		prim_arg_io_ptr_type, // sources
		prim_arg_io_ptr_type, // sinks
		prim_arg_struct_ptr_type, // structs
		builder.getInt64Ty(), // num_sources
		builder.getInt64Ty(), // num_sinks
		builder.getInt64Ty(),  // num_sources
		builder.getInt64Ty(), // #in
		builder.getInt64Ty(),  // #out
		// pointer_to_invalid_access_safe_space
		llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(builder.getInt8Ty())),
		// pointer_to_invalid_access_zero_safe_space
		llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(builder.getInt8Ty()))
	}, "CPrimArg");
	LOG_TRACE("CPrimArg %s", llvm_to_string(prim_arg_type).c_str());
	prim_arg_ptr_type = llvm::PointerType::getUnqual(prim_arg_type);

	auto func_type =
		llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt64Ty(),
			  builder.getInt64Ty(), builder.getInt64Ty(),
			  builder.getInt64Ty(), builder.getInt64Ty(),
			  builder.getInt64Ty(), builder.getInt64Ty(),
			  builder.getInt8PtrTy(), builder.getInt8PtrTy() }, false);

	if (request.paranoid) {
		llvm_compiler_check_prim =
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_check_prim", module);

		func_type =
			llvm::FunctionType::get(builder.getVoidTy(),
				{ prim_arg_ptr_type, builder.getInt8PtrTy(),
					builder.getInt64Ty(), builder.getInt8PtrTy(),
					builder.getInt8PtrTy(), builder.getInt8PtrTy() }, false);

		llvm_compiler_check_xvalue =
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_check_xvalue", module);


		func_type =
			llvm::FunctionType::get(builder.getVoidTy(),
				{ prim_arg_ptr_type, builder.getInt64Ty(),
					builder.getInt8PtrTy(), builder.getInt8PtrTy(),
					builder.getInt8PtrTy() }, false);

		llvm_compiler_check_struct =
			llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_check_struct", module);
	}

	prim_func_type =
		llvm::FunctionType::get(builder.getVoidTy(),
			{ prim_arg_ptr_type, builder.getInt8PtrTy(),
			builder.getInt8PtrTy() }, false);

	if (request.paranoid) {
		llvm_compiler_begin_prim =
			llvm::Function::Create(prim_func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_begin_prim", module);

		llvm_compiler_end_prim =
			llvm::Function::Create(prim_func_type, llvm::Function::ExternalLinkage,
				"llvm_compiler_end_prim", module);
	}

	if (llvm_compiler_check_prim) _fun_add_properties(*llvm_compiler_check_prim);
	if (llvm_compiler_check_xvalue) _fun_add_properties(*llvm_compiler_check_xvalue);
	if (llvm_compiler_begin_prim) _fun_add_properties(*llvm_compiler_begin_prim);
	if (llvm_compiler_end_prim) _fun_add_properties(*llvm_compiler_end_prim);
}

llvm::Value*
LlvmCompiler::CreateComparison(voila::Function::Tag tag,
	llvm::Value* a, llvm::Value* b,
	Type* a_type, Type* b_type)
{
	const bool is_eq = tag == Function::Tag::kCmpEq;
	const bool is_ne = tag == Function::Tag::kCmpNe;

	ASSERT(a_type);

	switch (a_type->get_tag()) {
	case Type::Tag::kString:
		{
			ASSERT(b_type);
			ASSERT(b_type->get_tag() == a_type->get_tag());
		}
		a = generate_strcmp(builder, a, b, nullptr, is_eq || is_ne);
		b = builder.getInt32(0);
		break;

	default:
		compare_cast2(builder, a, b);
		// comment_int(builder, fun.tag2string(fun.tag), a);
		// comment_int(builder, fun.tag2string(fun.tag), b);
		break;
	}

	switch (tag) {
	case Function::Tag::kCmpEq: return builder.CreateICmpEQ(a, b);
	case Function::Tag::kCmpLt: return builder.CreateICmpSLT(a, b);
	case Function::Tag::kCmpGt: return builder.CreateICmpSGT(a, b);
	case Function::Tag::kCmpNe: return builder.CreateICmpNE(a, b);
	case Function::Tag::kCmpLe: return builder.CreateICmpSLE(a, b);
	case Function::Tag::kCmpGe: return builder.CreateICmpSGE(a, b);
	default: 					break;
	}

	ASSERT(false);
	return nullptr;
}
void
LlvmCompiler::operator()() {
	LOG_TRACE("Generating LLVM");

	install_functions();

	can_full_eval = request.can_evaluate_speculatively();
	request.dump();

	LOG_DEBUG("Generate primitive '%s'", func_name.c_str());
	auto func_type =
		llvm::FunctionType::get(builder.getInt64Ty(), {
				prim_arg_ptr_type, // arg
				builder.getInt64Ty() // context
			}, false);

	auto jit_func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage,
		func_name, module);
	jit_func->setOnlyAccessesArgMemory();
	jit_func->setDoesNotFreeMemory();
	jit_func->setDoesNotRecurse();

	llvm_function = jit_func;

#ifndef USE_INTERNAL_KERNEL
	// jit_func->addParamAttr(1, llvm::Attribute::NoAlias);
#endif
	/* jit_func->setCallingConv(llvm::CallingConv::Fast); */

	bb_entry = llvm::BasicBlock::Create(context, "entry", jit_func);
	builder.SetInsertPoint(bb_entry);


	auto func_name_str = builder.CreatePointerCast(
		builder.CreateGlobalString(func_name),
		builder.getInt8PtrTy());
	auto dbg_name_str = builder.CreatePointerCast(
		builder.CreateGlobalString(dbg_name),
		builder.getInt8PtrTy());

	llvm_func_name_str = func_name_str;
	llvm_dbg_name_str = dbg_name_str;
	func_arg = jit_func->arg_begin();


	if (llvm_compiler_begin_prim) {
		builder.CreateCall(llvm_compiler_begin_prim, {
			func_arg, func_name_str, dbg_name_str });
	}

	// collect input/outputs
	LOG_DEBUG("Collecting in/outputs %llu sources, %llu sinks",
		request.sources.size(), request.sinks.size());
	std::vector<voila::Node*> out_predicates;

	std::map<std::pair<int, int>, llvm::Value*> collect_cache;

	auto collect = [&] (auto& io_info, bool source) {
		const auto& array =     source  ? request.sources    : request.sinks;
		const auto& this_set =  source  ? request.source_set : request.sink_set;
		const auto& other_set = !source ? request.source_set : request.sink_set;

		const size_t section_idx = source ? 0 : 1;

		size_t i=0;
		for (auto& source_node : array) {
			if (!source_node) {
				continue;
			}
			// ASSERT(source_node->flags & voila::Node::kAllExprs);
			auto node_ptr = source_node.get();
			auto expr_ptr = dynamic_cast<voila::Expression*>(node_ptr);
			auto var_ptr = dynamic_cast<voila::Variable*>(node_ptr);
			auto assign_ptr = dynamic_cast<voila::Assign*>(node_ptr);

			// translate type
			ASSERT(expr_ptr || assign_ptr || var_ptr);
			const auto type = get_type(node_ptr);

			ASSERT(io_info.find(node_ptr) == io_info.end() && "No duplicates allowed");
			ASSERT(this_set.find(source_node) != this_set.end());
			ASSERT(other_set.find(source_node) == other_set.end());

			static_assert(sizeof(void*) == 8, "Assumes 64-bit pointers");

			char* class_str = "?";
			if (expr_ptr) {
				class_str = "expr";
			} else if (assign_ptr) {
				class_str = "assign";
			} else if (var_ptr) {
				class_str = "var";
			}

			LOG_DEBUG("Generating %s (predicate=%d) for i=%llu, class=%s",
				source ? "load" : "store",
				(int)type->is_predicate(), i, class_str);

			node_ptr->dump();

			auto get_ptr = [&] (size_t io_index, size_t io_select) {
				llvm::Value* ptr = func_arg;

				const std::pair<int, int> key = { section_idx, -1 };
				auto sec_it = collect_cache.find(key);
				if (sec_it == collect_cache.end()) {
					ptr = builder.CreateGEP(
						set_noalias_scope(builder.CreateLoad(builder.CreateGEP(ptr,
							llvm::ArrayRef<llvm::Value*> {
								builder.getInt32(0), // pointer
								builder.getInt32(section_idx),
							}))),
					llvm::ArrayRef<llvm::Value*> {
						builder.getInt32(0),
					});

					collect_cache[key] = ptr;
				} else {
					ptr = sec_it->second;
				}

				ptr = builder.CreateGEP(ptr,
					llvm::ArrayRef<llvm::Value*> {
						builder.getInt32(io_index),
						builder.getInt32(io_select)
					});

				return ptr;
			};

			std::ostringstream ss;
			ss << "io_";
			if (source) {
				ss << "src_";
			} else {
				ss << "dst_";
			}
			ss << i;

			auto ptr = get_ptr(i, 0);
			auto num = get_ptr(i, 1);
			auto offset = get_ptr(i, 2);
			auto xvalue = get_ptr(i, 3);

			LOG_DEBUG("%s: type str '%s' type tag %d",
				ss.str().c_str(), type->to_cstring(), type->get_tag());

			auto llvm_type = compile_type(builder, *type);

			if (llvm_type) {
				ptr = builder.CreatePointerCast(ptr,
					llvm::PointerType::getUnqual(
						llvm::PointerType::getUnqual(
							llvm::PointerType::getUnqual(llvm_type))));
				LOG_DEBUG("prim_arg_ptr_type %s, base_ptr_type %s, num_ptr_type %s, casted base_ptr_type %s",
					llvm_to_string(prim_arg_ptr_type).c_str(),
					llvm_to_string(ptr).c_str(),
					llvm_to_string(num).c_str(),
					llvm_to_string(ptr).c_str());
			} else {
				ptr = nullptr;
			}

			if (ptr && llvm_compiler_check_xvalue) {
				builder.CreateCall(llvm_compiler_check_xvalue, {
					func_arg,
					set_noalias_scope(builder.CreateLoad(xvalue)),
					builder.getInt64(type->get_tag()),
					builder.CreatePointerCast(
						builder.CreateGlobalString(ss.str()),
						builder.getInt8PtrTy()),
					llvm_func_name_str,
					llvm_dbg_name_str
				});
			}

			ASSERT(node_ptr);

			std::string dbg_name(source ? "source_" : "sink_");
			if (var_ptr) {
				dbg_name = dbg_name + "var_" + var_ptr->dbg_name;
			} else if (assign_ptr) {
				dbg_name = dbg_name + "assign_" + assign_ptr->var->dbg_name;
			} else if (expr_ptr) {
				dbg_name = dbg_name + "expr";
			}

			size_t width = type->get_width();

			llvm::Value* llvm_pointer =  llvm_type ?
				set_align(set_deref(set_noalias_scope(builder.CreateLoad(
					set_noalias_scope(builder.CreateLoad(ptr, ss.str() + std::string("_ptr")))))),
				width) :
				nullptr;

			llvm::Value* llvm_num = llvm_type ?
				set_deref(set_noalias_scope(builder.CreateLoad(num, ss.str() + std::string("_num")))) :
				nullptr;

			llvm::Value* llvm_offset = llvm_type ?
				set_deref(set_noalias_scope(builder.CreateLoad(offset, ss.str() + std::string("_offset")))) :
				nullptr;

			ASSERT(fragment.llvm_jit);

			if (llvm_pointer) {

#if 0
				builder.CreateAlignmentAssumption(module->getDataLayout(),
					llvm_pointer, width);
#endif
			}

			io_info[node_ptr] = IOInfo {
				// pointer
				llvm_pointer,
				// num
				llvm_num,
				// offset
				llvm_offset,

				// type
				llvm_type,
				// input
				source,
				// output
				!source,
				// predicate
				type->is_predicate(),

				std::move(dbg_name)
			};

			if (source && type->is_predicate()) {
				LOG_TRACE("in predicate");
				ASSERT(expr_ptr);
				ASSERT(!assign_ptr);
				in_predicates.push_back(expr_ptr);
			}
			if (!source && type->is_predicate()) {
				LOG_TRACE("out predicate");
				ASSERT(expr_ptr || assign_ptr || var_ptr);
				out_predicates.push_back(node_ptr);
			}

			i++;
		}
	};

	scheduler::yield();

	collect(io_info, true);
	collect(io_info, false);

	LOG_DEBUG("Collected %llu in_predicates, %llu out_predicates",
		in_predicates.size(), out_predicates.size());

	// setup profiling
	auto prof_in_tuples = builder.CreateGEP(func_arg,
			llvm::ArrayRef<llvm::Value*> {
				builder.getInt32(0), // pointer
				builder.getInt32(6),
			}
		);

	auto prof_out_tuples = builder.CreateGEP(func_arg,
			llvm::ArrayRef<llvm::Value*> {
				builder.getInt32(0), // pointer
				builder.getInt32(7),
			}
		);


	// setup safe space
	{
		stream_access_safe_space = builder.CreateGEP(func_arg,
			llvm::ArrayRef<llvm::Value*> {
				builder.getInt32(0), // pointer
				builder.getInt32(8),
			}
		);

		stream_access_safe_space_zero = builder.CreateGEP(func_arg,
			llvm::ArrayRef<llvm::Value*> {
				builder.getInt32(0), // pointer
				builder.getInt32(9),
			}
		);

		stream_access_safe_space = set_noalias_scope(builder.CreateLoad(
			set_noalias_scope(builder.CreateLoad(stream_access_safe_space))));
		stream_access_safe_space_zero = set_noalias_scope(builder.CreateLoad(
			set_noalias_scope(builder.CreateLoad(stream_access_safe_space_zero))));
	}

	ASSERT(in_predicates.size() <= 1 && "Todo: Lift limitation");

	LOG_DEBUG("Compile data structs");
	{
		size_t i = 0;
		for (auto& ds : request.structs) {
			auto voila_struct = std::dynamic_pointer_cast<voila::DataStructure>(ds);
			ASSERT(voila_struct.get());

			compile_struct(voila_struct, i);

			i++;
		}
	}

	LOG_DEBUG("Generating input validation code");

	int64_t num_sources = 0, num_sinks = 0;
	int64_t num_source_preds = 0, num_sink_preds = 0;
	for (const auto& kv : io_info) {
		const auto& info = kv.second;
		if (info.input) {
			num_sources++;
			if (info.predicate) {
				num_source_preds++;
			}
		}
		if (info.output) {
			num_sinks++;
			if (info.predicate) {
				num_sink_preds++;
			}
		}
	}

	if (llvm_compiler_check_prim) {
		auto read_num_sources =
			builder.CreateGEP(func_arg,
				llvm::ArrayRef<llvm::Value*> {
					builder.getInt32(0), // pointer
					builder.getInt32(3),
				}
			);
		auto read_num_sinks =
			builder.CreateGEP(func_arg,
				llvm::ArrayRef<llvm::Value*> {
					builder.getInt32(0), // pointer
					builder.getInt32(4),
				}
			);
		auto read_num_structs =
			builder.CreateGEP(func_arg,
				llvm::ArrayRef<llvm::Value*> {
					builder.getInt32(0), // pointer
					builder.getInt32(5),
				}
			);

		read_num_sources = set_noalias_scope(builder.CreateLoad(read_num_sources, "read_num_sources"));
		read_num_sinks = set_noalias_scope(builder.CreateLoad(read_num_sinks, "read_num_sinks"));
		read_num_structs = set_noalias_scope(builder.CreateLoad(read_num_structs, "read_num_structs"));

		ASSERT(read_num_sources->getType()->isIntegerTy());
		ASSERT(read_num_sinks->getType()->isIntegerTy());
		ASSERT(read_num_structs->getType()->isIntegerTy());

		builder.CreateCall(llvm_compiler_check_prim, { func_arg,
			read_num_sources, read_num_sinks, read_num_structs,
			builder.getInt64(num_sources), builder.getInt64(num_sinks),
			builder.getInt64(num_source_preds), builder.getInt64(num_sink_preds),
			func_name_str, dbg_name_str });
	}

	llvm::Function* kernel_func;
	llvm::Value* kernel_func_call_result;
#ifdef USE_INTERNAL_KERNEL
	LOG_DEBUG("create internal kernel");
	llvm::BasicBlock* jit_func_curr_bb = builder.GetInsertBlock();
	{
		// collect everything we want to overwrite
		std::vector<llvm::Value**> kernel_arguments_ptrs = {
			&func_arg,
			&stream_access_safe_space, &stream_access_safe_space_zero,
			&prof_in_tuples, &prof_out_tuples
		};

		auto add = [&] (llvm::Value** p) {
			ASSERT(p);
			if (*p) kernel_arguments_ptrs.push_back(p);
		};

		for (auto& key_val : io_info) {
			auto& io = key_val.second;
			add(&io.ptr);
			add(&io.num);
			add(&io.offset);
		}

		for (auto& key_val : data_structs) {
			auto& ds = key_val.second;
			add(&ds.mask);
			add(&ds.buckets);
			add(&ds.structure);
			add(&ds.pbuckets);
			add(&ds.pmask);
			add(&ds.bf_words);
			add(&ds.bf_mask);
		}

		// create function stuff, arguments and types
		std::vector<llvm::Type*> kernel_types;
		kernel_types.reserve(kernel_arguments_ptrs.size());
		std::vector<llvm::Value*> call_args;
		call_args.reserve(kernel_arguments_ptrs.size());
		for (auto& arg_ptr : kernel_arguments_ptrs) {
			auto arg = *arg_ptr;
			kernel_types.push_back(arg->getType());
			call_args.push_back(arg);
		}

		auto kernel_func_type = llvm::FunctionType::get(
			builder.getInt64Ty(), kernel_types, false);
		kernel_func = llvm::Function::Create(kernel_func_type,
			llvm::Function::PrivateLinkage, "kernel", module);

		// add attributes
		#if 1
		for (size_t i=0; i<kernel_arguments_ptrs.size(); i++) {
			if (kernel_types[i]->isPointerTy()) {
				kernel_func->addParamAttr(i, llvm::Attribute::NoCapture);
				kernel_func->addParamAttr(i, llvm::Attribute::NoAlias);
				kernel_func->addParamAttr(i, llvm::Attribute::NoFree);
			}
		}
		#endif
		kernel_func->setOnlyAccessesArgMemory();
		kernel_func->setDoesNotFreeMemory();
		kernel_func->setDoesNotRecurse();

		// call internal kernel
		kernel_func_call_result = builder.CreateCall(kernel_func,
			std::move(call_args));

		auto bb_kernel_entry = llvm::BasicBlock::Create(context, "kernel_entry",
			kernel_func);
		builder.SetInsertPoint(bb_kernel_entry);

		// UGLY: overwrite entry block with kernel, so variables end up here
		bb_entry = bb_kernel_entry;
		llvm_function = kernel_func;

		// read internal arguments back
		for (size_t i=0; i<kernel_arguments_ptrs.size(); i++) {
			auto& arg_ptr = kernel_arguments_ptrs[i];
			auto fun_arg = kernel_func->getArg(i);
			*arg_ptr = fun_arg;
		}

	}
#else
	kernel_func_call_result = builder.getInt64(0);
	kernel_func = jit_func;
#endif /* USE_INTERNAL_KERNEL */

	// create internal variables
	{
		VoilaCollectVariables collector;
		for (auto& node : request.statements) {
			auto stmt = std::dynamic_pointer_cast<voila::Statement>(node);
			ASSERT(stmt);
			collector({ stmt });
		}

		if (!is_extended()) {
			ASSERT(collector.assigned.empty());
		}

		for (auto& var : collector.assigned) {
			auto type = get_type((Node*)var.get());
			ASSERT(type);

			llvm::Type* llvm_type = compile_type(builder, *type,
					true /* internal data types */);

			auto var_ptr = create_alloca_var(llvm_type, "var_" + var->dbg_name);

			LOG_DEBUG("LlvmCompiler: Variable %s with voila type %s, llvm type %s",
				var->dbg_name.c_str(), type->to_string().c_str(),
				llvm_to_string(llvm_type).c_str());

			variables[var.get()] = CompiledValue { { var_ptr } };
		}
	}

	// create loop variables
	auto intTy = builder.getInt64Ty();
	auto k_ptr = set_noalias_scope(builder.CreateAlloca(intTy, nullptr, "k_ptr"));
	auto k_ptr_copy = set_noalias_scope(builder.CreateAlloca(intTy, nullptr, "k_ptr_copy"));
	set_noalias(builder.CreateStore(builder.getInt64(0), k_ptr));

	// compute overlap
	LOG_DEBUG("Generate overlap");
	llvm::Value* min_num = nullptr;
	ASSERT(!in_predicates.empty());

	bb_epilogue = llvm::BasicBlock::Create(context, "epilogue", kernel_func);

	for (auto& pred : in_predicates) {
		auto num = io_info[pred].num;
		ASSERT(num);

		auto n = set_noalias_scope(builder.CreateLoad(num, "overlap_num"));
		ASSERT(n->getType()->isIntegerTy());

		if (min_num) {
			min_num = builder.CreateMinimum(min_num, n);
		} else {
			min_num = n;
		}
	}

	{
		// we need to set out->num to 0 after reading input ->num,
		// because out=in is possible

		// set output predicate nums to 0
		for (auto& sink : request.sinks) {
#if WhoCares
			auto expr = std::dynamic_pointer_cast<voila::Expression>(sink);
			if (!expr) {
				continue;
			}
#endif
			auto it = io_info.find(sink.get());
			ASSERT(it != io_info.end());
			auto& info = it->second;

			if (!info.type) {
				continue;
			}
			LOG_TRACE("num type = %s",
				llvm_to_string(info.num->getType()).c_str());

			ASSERT(info.num);
			set_noalias(builder.CreateStore(builder.getInt64(0), info.num));
		}
		for (auto& n : out_predicates) {
			ASSERT(n);
			ASSERT(io_info[n].num);
			set_noalias(builder.CreateStore(builder.getInt64(0), io_info[n].num));
		}


		generate(k_ptr, min_num, kernel_func, "overlap");


		builder.SetInsertPoint(bb_epilogue);

		// save iterator position
		set_noalias(builder.CreateStore(set_noalias_scope(builder.CreateLoad(k_ptr)), k_ptr_copy));

		bb_epilogue = nullptr;
	}

	scheduler::yield();

	set_noalias(builder.CreateStore(min_num, prof_in_tuples));


	auto int_max = [] (auto& builder, auto a, auto b) {
		auto a_smaller = builder.CreateICmpSLT(a, b);
		return builder.CreateSelect(a_smaller, b, a);
	};

	if (out_predicates.empty() && num_sinks) {
		set_noalias(builder.CreateStore(min_num, prof_out_tuples));
	} else {
		llvm::Value* max_num = builder.getInt64(0);
		for (auto& p : out_predicates) {
			llvm::Value* val = set_noalias_scope(builder.CreateLoad(io_info[p].num));
			max_num = max_num ? int_max(builder, max_num, val) : val;
		}

		set_noalias(builder.CreateStore(max_num, prof_out_tuples));
	}


	if (llvm_compiler_end_prim) {
		builder.CreateCall(llvm_compiler_end_prim,
			{ func_arg, func_name_str, dbg_name_str });
	}
#ifdef USE_INTERNAL_KERNEL
	// terminate kernel
	builder.CreateRet(builder.getInt64(0));

	// return to outer function
	builder.SetInsertPoint(jit_func_curr_bb);
#endif /* USE_INTERNAL_KERNEL */

	builder.CreateRet(kernel_func_call_result);

	func_arg = nullptr;
}

void
LlvmCompiler::if_then_else(llvm::Value* val, const std::string& dbg_name,
	const std::function<void(bool branch)>& fun, bool has_else)
{
	auto val_type = val->getType();
	ASSERT(val_type->isIntegerTy() && val_type->getIntegerBitWidth() == 1);

	auto func = builder.GetInsertBlock()->getParent();
	std::string s_then(dbg_name + std::string("_then"));
	std::string s_else(dbg_name + std::string("_else"));
	auto bb_then = llvm::BasicBlock::Create(context, s_then, func);
	auto bb_else = has_else ? llvm::BasicBlock::Create(context, s_else, func) : nullptr;
	auto bb_after = llvm::BasicBlock::Create(context,
		dbg_name + std::string("_after"), func);

	builder.CreateCondBr(val, bb_then, has_else ? bb_else : bb_after);

	builder.SetInsertPoint(bb_then);
	comment(builder, s_then);
	fun(true);
	builder.CreateBr(bb_after);

	if (bb_else) {
		builder.SetInsertPoint(bb_else);
		comment(builder, s_else);
		fun(false);
		builder.CreateBr(bb_after);
	}

	builder.SetInsertPoint(bb_after);
}

void
LlvmCompiler::predicated(const std::function<void()>& f,
	const voila::Expr& voila_predicate, const std::string& dbg_name,
	const PredicationOpts& opts)
{
	create_predicated(translate_predicate(voila_predicate),
		[&] (llvm::Type*& result_type, auto args) {
			f(); return kInvalidPointer;
		},
		voila_predicate, dbg_name, opts);
}

static bool
is_predicate_llvm_type(llvm::Type* t, int64_t* width = nullptr)
{
	if (auto int_type = llvm::dyn_cast<llvm::IntegerType>(t)) {
		auto w = int_type->getBitWidth();
		if (width) {
			*width = w;
		}

		return w == 1 || w == 32;
	}
	return false;
}

llvm::Value*
LlvmCompiler::translate_predicate(const voila::Expr& voila_predicate)
{
	llvm::Value* pred = nullptr;
	if (is_in_predicate(voila_predicate.get())) {
		// ignore predicate
	} else if (voila_predicate) {
		auto compiled_predicate = get(voila_predicate);
		ASSERT(compiled_predicate.value.size() == 1);

		pred = compiled_predicate.value[0];
	}
	if (pred) {
		int64_t bit_width = 0;
		if (!is_predicate_llvm_type(pred->getType(), &bit_width)) {
			LOG_ERROR("Is not predicate type or has invalid bit width (%lld)",
				bit_width);
			// ASSERT(false && "Invalid type for predicate");
		}
		if (bit_width != 1) {
			LOG_ERROR("Invalid predicate?");
			pred = builder.CreateIsNotNull(pred);
		}
		ASSERT(bit_width == 1);
	}

	return pred;
}

llvm::Value*
LlvmCompiler::predicated_expr(const std::function<llvm::Value*(llvm::Type*& result_type, const PredGenArgs&)>& generator,
	const voila::Expr& voila_predicate, const std::string& dbg_name, const PredicationOpts& opts)
{
	return create_predicated(voila_predicate ?
		translate_predicate(voila_predicate) : nullptr,
		generator, voila_predicate, dbg_name, opts);
}

llvm::Value*
LlvmCompiler::get_undef(llvm::Type* type)
{
	ASSERT(type);
#ifdef USE_UNDEFINED
	return llvm::UndefValue::get(type);
#endif
	if (auto int_type = llvm::dyn_cast<llvm::IntegerType>(type)) {
		return builder.getIntN(int_type->getBitWidth(), 0);
	}
	if (auto ptr_type = llvm::dyn_cast<llvm::PointerType>(type)) {
		return builder.CreateIntToPtr(builder.getInt64(0), ptr_type);
	}
	ASSERT(false && "invalid type");
	return nullptr;
}

void
LlvmCompiler::store_in_sink(voila::Node* p, const voila::Expr& expr,
	bool allow_predicate)
{
	auto it = io_info.find(p);
	ASSERT(it != io_info.end());
	auto& info = it->second;


	if (!info.ptr) {
		return;// continue;
	}
	auto compiled = get(expr);

	comment_ptr(builder, "sink1_store", info.ptr);

	if (info.predicate) {
		ASSERT(allow_predicate);
		ASSERT(compiled.value.size() == 1);
		auto val0 = compiled.value[0];

		llvm::Value* idx = loop_index;

		LOG_DEBUG("CreateStore: predicate move result: res tpe=%s, idx tpe=%s, dest_ptr_tpe=%s, dest_tpe=%s",
			llvm_to_string(val0->getType()).c_str(),
			llvm_to_string(idx->getType()).c_str(),
			llvm_to_string(info.ptr->getType()).c_str(),
			llvm_to_string(info.type).c_str());


		auto store_sel = [&] (const auto& val) {
			llvm::Value* num = set_noalias_scope(builder.CreateLoad(info.num));
			llvm::Value* num_casted = builder.CreateSExtOrTrunc(num, info.type);
			llvm::Value* idx_casted = builder.CreateSExtOrTrunc(idx, info.type);

			set_noalias(builder.CreateStore(idx_casted,
				builder.CreateInBoundsGEP(info.type, info.ptr,
					num_casted)));

			auto increment = val ?
				// builder.CreateSelect(val, builder.getInt64(1), builder.getInt64(0)) :
				builder.CreateZExtOrTrunc(val, builder.getInt64Ty()) :
				builder.getInt64(1);

			comment_int(builder, "pred_store inc", increment);

			num = builder.CreateAdd(num, increment, "", true, true);

			comment_int(builder, "pred_store num", num);
			set_noalias(builder.CreateStore(num, info.num));
		};

		if (request.flavor->get_int(FlavorSpec::Entry::kPredicated) > 0) {
			comment_int(builder, "pred_pred_store num", val0);
			store_sel(val0);
		} else {
			if_then_else(val0, "sink1_pred_store", [&] (bool then) {
				if (then) {
					store_sel(nullptr);
				} else {
					comment(builder, "pred_else");
				}
			}, enable_comment);
		}
	} else if (info.type) {
		ASSERT(compiled.value.size() == 1);
		auto val0 = compiled.value[0];

		llvm::Value* idx = loop_index;

		LOG_DEBUG("CreateStore: regular move result: res tpe=%s, idx tpe=%s, dest_ptr_tpe=%s, dest_tpe=%s",
			llvm_to_string(val0->getType()).c_str(),
			llvm_to_string(idx->getType()).c_str(),
			llvm_to_string(info.ptr->getType()).c_str(),
			llvm_to_string(info.type).c_str());


		if (val0->getType()->isIntegerTy()) {
			val0 = builder.CreateSExtOrTrunc(val0, info.type,
				"sink_store_cast");
		} else {
			val0 = val0;
		}

		auto type = get_type(expr.get());
		ASSERT(type);

		unsigned width = type->get_width();

		ASSERT(val0);

		set_noalias(CreateAlignedStore(val0,
			builder.CreateInBoundsGEP(info.type, info.ptr, idx),
			width));
	} else {
		comment(builder, "store1_nothing");
	}
}

void
LlvmCompiler::generate(llvm::Value* k_ptr, llvm::Value* num, llvm::Function* func,
	const std::string& twine)
{
	ASSERT(!loop_index);
	cache.clear();

	auto bb_sel = llvm::BasicBlock::Create(context, "sel", func);
	auto bb_nosel =
		request.flavor->get_int(FlavorSpec::Entry::kUnrollNoSel) > 0 ?
		llvm::BasicBlock::Create(context, "nosel", func) :
		nullptr;

	ASSERT(in_predicates.size() == 1);

	if (bb_nosel) {
		for (auto& p : in_predicates) {
			auto it = io_info.find(p);
			ASSERT(it != io_info.end());
			auto& info = it->second;

			ASSERT(request.complexity > 0);
			bool local_can_full_eval = can_full_eval;

			if (local_can_full_eval && in_predicates.size() == 1) {
				auto new_num = builder.CreateCall(get_llvm_compiler_can_full_eval(),
					{
						func_arg,
						builder.CreatePointerCast(info.ptr, builder.getInt8PtrTy()),
						builder.CreateSExtOrTrunc(num, builder.getInt64Ty()),
						builder.getInt64(local_can_full_eval ? request.complexity : 0),
						builder.getInt64(local_can_full_eval ? request.sum_bits : 0)
					});

				auto cond = builder.CreateICmpSGE(new_num, builder.getInt64(0));
				num = builder.CreateSelect(cond, new_num, num);

				builder.CreateCondBr(cond, bb_nosel, bb_sel);
			} else {
				auto cond = builder.CreateIsNull(info.ptr);
				builder.CreateCondBr(cond, bb_nosel, bb_sel);
			}
		}
	} else {
		builder.CreateBr(bb_sel);
	}

	for (auto& has_selection : {true, false}) {
		cache.clear();

		if (!has_selection && !bb_nosel) {
			continue;
		}
		builder.SetInsertPoint(has_selection ? bb_sel : bb_nosel);
		comment(builder, has_selection ? "sel" : "nosel");

		for (auto& source : request.sources) {
			auto type = get_type(source.get());
			ASSERT(type);

			if (type->is_position()) {
				auto it = io_info.find(source.get());
				ASSERT(it != io_info.end());
				auto& info = it->second;

				llvm::Value* initial_offset = set_noalias_scope(builder.CreateLoad(info.offset));
				llvm::Value* alloc_offset = set_noalias_scope(
					builder.CreateAlloca(builder.getInt64Ty(),
					nullptr, "write_offset"));
				set_noalias(builder.CreateStore(initial_offset, alloc_offset));

				vars_to_increment_post_loop_body[source.get()] = alloc_offset;
			}
		}

		ASSERT(request.flavor);
		int64_t unroll_factor = request.flavor->get_int(has_selection ?
			FlavorSpec::Entry::kUnrollSel : FlavorSpec::Entry::kUnrollNoSel);
		ASSERT(unroll_factor >= 0);

		int64_t simdization_factor = request.flavor->get_int(has_selection ?
			FlavorSpec::Entry::kSimdizeSel : FlavorSpec::Entry::kSimdizeNoSel);
		ASSERT(simdization_factor >= 0);

		loop(k_ptr, num, unroll_factor, simdization_factor,
				twine, func,
				[&] (llvm::Value* __i, int64_t step) {

			comment(builder, "iteration");
			cache.clear();
			
			// load positions from selection vectors
			loop_index = __i;
			for (auto& p : in_predicates) {
				auto it = io_info.find(p);
				ASSERT(it != io_info.end());
				auto& info = it->second;

				ASSERT(info.predicate && info.input);

				LOG_TRACE("Info type %s, Info ptr type %s",
					llvm_to_string(info.type).c_str(),
					llvm_to_string(info.ptr->getType()).c_str());

				llvm::Value* data;

				if (has_selection) {
					data = set_noalias_scope(builder.CreateLoad(
						builder.CreateInBoundsGEP(info.type, info.ptr, __i),
						"in_pred_val" + info.dbg_name));
				} else {
					data = __i;
				}

				ASSERT(in_predicates.size() == 1);
				loop_index = data;

				comment_int(builder,
					has_selection ? "in_pred(sel) data" : "in_pred(nosel) data",
					data);
				put_simple(p, builder.getInt1(1));
			}

			comment_int(builder, "loop idx", loop_index);

			LOG_DEBUG("request.sources");
			for (auto& source : request.sources) {
				source->dump();
			}

			// set up non predicate inputs
			for (auto& source : request.sources) {
				auto expr = std::dynamic_pointer_cast<voila::Expression>(source);
				auto src_it = io_info.find(expr.get());

				ASSERT(source->flags & voila::Node::kAllExprs);
				ASSERT(expr && "Source must be expression");
				ASSERT(src_it != io_info.end());

				auto& info = src_it->second;
				ASSERT(info.input);
#if 0
				if (info.predicate) {
					continue;
				}
#endif
				llvm::Value* idx = loop_index;
				comment_ptr(builder, "source", info.ptr);
				comment_int(builder, "source idx", idx);

				auto type = get_type(source.get());
				ASSERT(type);

				unsigned width = type->get_width();

				llvm::Value* data = nullptr;
				if (type->is_position()) {
					ASSERT(expr->data_structure.get());

					CompiledStruct cstruct;
					bool exists = get_compiled_struct(cstruct, expr->data_structure);
					ASSERT(exists && cstruct.row_type);

					LOG_TRACE("info:ptr %s, row ptr %s",
						llvm_to_string(info.ptr->getType()).c_str(),
						llvm_to_string(llvm::PointerType::getUnqual(cstruct.row_type)).c_str());
					// data = builder.CreateLoad(info.ptr);
					data = builder.CreatePointerCast(info.ptr,
						 llvm::PointerType::getUnqual(cstruct.row_type));

					auto it = vars_to_increment_post_loop_body.find(source.get());
					ASSERT(it != vars_to_increment_post_loop_body.end());

					auto alloc_offset = it->second;

					put(expr.get(), CompiledValue { {data, alloc_offset}} );
				} else if (type->is_predicate()) {
					if (!is_cached(expr)) {
						put_simple(expr.get(), builder.getInt1(1));
					}
				} else {
					auto data_ptr = builder.CreateInBoundsGEP(info.type, info.ptr, idx);
					comment_ptr(builder, "source data ptr", data_ptr);
					data = set_noalias_scope(CreateAlignedLoad(data_ptr, width, "in_src_val"));
					put_simple(expr.get(), data);
				}

				if (auto read_var = dynamic_cast<voila::ReadVariable*>(expr.get())) {
					if (type->is_position()) {
						LOG_DEBUG("Is position");
						expr->dump();
					} else {
						auto var_ptr = read_var->var.get();
						LOG_DEBUG("Insert source variable %p", var_ptr);
						read_var->dump();

						auto compiled_value = get(expr);
						ASSERT(compiled_value.value.size() == 1);

						auto llvm_val = compiled_value.value[0];
						auto llvm_type = llvm_val->getType();

						if (type->is_predicate()) {
							llvm_val = builder.getInt1(1);
							llvm_type = builder.getInt1Ty();
						}

						auto var = create_alloca_var(llvm_type,
							"readvar_" + var_ptr->dbg_name);

						set_noalias(builder.CreateStore(llvm_val, var));
						variables[var_ptr] = CompiledValue {{ var }};

						// remove ReadVariable
						cache.erase(expr.get());
					}
				} else {
					LOG_DEBUG("Other source?");
					expr->dump();
				}
			}

			// generate statements
			for (auto& sink : request.statements) {
				ASSERT(sink->flags & voila::Node::kAllStmts);

				if (auto stmt = std::dynamic_pointer_cast<voila::Statement>(sink)) {
					handle(stmt);
				} else {
					ASSERT(false && "unhandled");
				}

			}

			// generate sink expressions
			for (auto& sink : request.sinks) {
				ASSERT(sink->flags & (voila::Node::kAllExprs | voila::Node::kAllStmts | voila::Node::kVariable));

				if (auto expr = std::dynamic_pointer_cast<voila::Expression>(sink)) {
					handle(expr);
				} else if (auto stmt = std::dynamic_pointer_cast<voila::Statement>(sink)) {
					// handle(stmt);
				} else if (auto var = std::dynamic_pointer_cast<voila::Variable>(sink)) {
					// nothing to do
				} else {
					ASSERT(false && "unhandled");
				}
			}

			// move data into outputs
			for (auto& sink : request.sinks) {
				auto p = sink.get();
				if (auto expr = std::dynamic_pointer_cast<voila::Expression>(sink)) {
					store_in_sink(p, expr);
				} else if (auto assign = std::dynamic_pointer_cast<voila::Assign>(sink)) {
					// LOG_WARN("TODO: What about predicate, do we assume that input predicate will cover this?");
					// store_in_sink(p, assign->value);
				} else if (auto var = std::dynamic_pointer_cast<voila::Variable>(sink)) {
					// nothing to do, should have already happend in Assign
				} else {
					ASSERT(false);
				}
			}

			// increment offsets
			for (auto& keyval : vars_to_increment_post_loop_body) {
				auto var_ptr = keyval.second;
				auto v =  builder.CreateAdd(builder.getInt64(1),
					set_noalias_scope(builder.CreateLoad(var_ptr)),
					"", true, true);
				set_noalias(builder.CreateStore(v, var_ptr));

			}
		}); // loop
		builder.CreateBr(bb_epilogue);

		loop_index = nullptr;
	}
}

struct CannotCompileException : CompilerException {
	CannotCompileException(const std::string& msg) : CompilerException(msg) {}
};

void
LlvmCompiler::stmt(const voila::Stmt& e)
{
	LOG_TRACE("LlvmCompiler::stmt");
	handle(e);
}

CompiledValue
LlvmCompiler::get(const voila::Expr& ptr)
{
	auto it = cache.find(ptr.get());
	if (it == cache.end()) {
		LOG_TRACE("Not compiled yet, compiling ...");
		handle(ptr);
		LOG_TRACE("Done");
		it = cache.find(ptr.get());
	}
	ASSERT(it != cache.end() && "Must have been already compiled");
	return it->second;
}

void
LlvmCompiler::put(voila::Expression* ptr, const CompiledValue& val)
{
	auto it = cache.find(ptr);
	ASSERT(it == cache.end() && "Must not recompile same expression");

	cache.insert({ptr, val});
}

engine::Type*
LlvmCompiler::get_type(voila::Node* ptr) const
{
	ASSERT(ptr);
	ASSERT(request.context);
	const auto& ctx = *request.context;

	auto type_it = ctx.infos.find(ptr);
	if (type_it == ctx.infos.end()) {
		ASSERT(false);
		THROW_MSG(CompilerException, "Node must be typed");
	}

	return get_single_type(type_it->second);
}


static bool
predication_is_just_conjunction_func(const Function& f)
{
	switch (f.tag) {
	case Function::Tag::kSelTrue:
	case Function::Tag::kSelFalse:
	case Function::Tag::kSelUnion:
		return true;

	default:
		return false;
	}
}

static bool
predication_is_just_conjunction(const Expression& expr)
{
	auto e = &expr;
	if (auto func = dynamic_cast<const Function*>(e)) {
		return predication_is_just_conjunction_func(*func);
	}
	return false;
}

static bool
can_be_predicated(const Function& f)
{
	switch (f.tag) {
	// can be predicated manually
	case Function::Tag::kBucketGather:
	case Function::Tag::kBucketNext:
	case Function::Tag::kBucketScatter:
	case Function::Tag::kBucketAggrSum:
	case Function::Tag::kBucketAggrCount:
	case Function::Tag::kBucketAggrMin:
	case Function::Tag::kBucketAggrMax:
	case Function::Tag::kGlobalAggrSum:
	case Function::Tag::kGlobalAggrCount:
	case Function::Tag::kGlobalAggrMin:
	case Function::Tag::kGlobalAggrMax:
		return true;

		// doesn't matter
	case Function::Tag::kBucketLookup:
	case Function::Tag::kBloomFilterLookup1:
	case Function::Tag::kBloomFilterLookup2:
		return true;


	default:
		return false;
	}
}

void
LlvmCompiler::on_expression(const std::shared_ptr<Function>& pfun)
{
	VoilaTreePass::on_expression(pfun);

	auto& fun = *pfun;

	pfun->dump();
	// children + function must have same predicate or NULL
	{
		auto pred = fun.predicate;
		pred->dump();
		for (auto& arg : fun.args) {
			if (arg->predicate) {
				arg->predicate->dump();
			}
			if (!(arg->predicate == pred || !arg->predicate)) {
#if 0
				ASSERT(false);
				THROW_MSG(CannotCompileException, "Cannot mix different predicates "
					"in expression tree");
#endif
			}
		}
	}

	if (fun.tag == voila::Function::Tag::kSelUnion) {
		LOG_DEBUG("TODO: move into regular code path, since we have predication_is_just_conjunction()");
		ASSERT(fun.args.size() == 1);

		auto value_a = get(fun.args[0]);
		ASSERT(value_a.value.size() == 1);

		auto pred = translate_predicate(fun.predicate);
		ASSERT(pred);

		bool force = false;

		comment_msg(builder, "kSelUnion a", value_a.value[0], force);
		comment_msg(builder, "kSelUnion b", pred, force);
		auto r = builder.CreateOr(value_a.value[0], pred);
		comment_msg(builder, "kSelUnion", r, false);
		put_simple(&fun, r);

		LOG_DEBUG("TODO: llvm compiler(kSelUnion): check, whether we compile the whole surrounding statement");
		return;
	}

	auto result_type = compile_type(builder, *get_type(pfun));

	const bool pred_just_conjuction = predication_is_just_conjunction_func(fun);

	auto gen = [&] (const PredGenArgs& predication_args) -> llvm::Value* {
		comment(builder, fun.tag2string(fun.tag));

		switch (fun.tag) {
		case voila::Function::Tag::kHash:
		case voila::Function::Tag::kRehash:
			{
				bool rehash = fun.tag == voila::Function::Tag::kRehash;
				size_t index_of_val;
				if (rehash) {
					// combine hashes
					ASSERT(fun.args.size() == 2);
					index_of_val = 1;
				} else {
					ASSERT(fun.args.size() == 1);
					index_of_val = 0;
				}
				auto type = get_type(fun.args[index_of_val]);

				ASSERT(type->num_bits() <= 64 && "Todo: high bit types");

				auto value = get(fun.args[index_of_val]);
				ASSERT(value.value.size() == 1);

				llvm::Value* hash = nullptr;
				if (type->is_integer()) {
					hash = builder.CreateIntCast(value.value[0],
						builder.getInt64Ty(), false);
#if 0
	#if 1
					// https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key#12996028

					// x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
					hash = builder.CreateXor(hash,
						builder.CreateLShr(hash, builder.getInt64(30)));
					hash = builder.CreateMul(hash,
						builder.getInt64(0xbf58476d1ce4e5b9));

					// x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
					hash = builder.CreateXor(hash,
						builder.CreateLShr(hash, builder.getInt64(27)));
					hash = builder.CreateMul(hash,
						builder.getInt64(0x94d049bb133111eb));

					// x = x ^ (x >> 31);
					hash = builder.CreateXor(hash,
						builder.CreateLShr(hash, builder.getInt64(31)));
	#else
					hash = builder.CreateMul(hash,
						builder.getInt64(18446744073709551557ull));
	#endif
#else
					auto x = hash;
					auto c1 = builder.getInt64(16);
					auto c2 = builder.getInt64(0x45d9f3b);
					x = builder.CreateXor(x, builder.CreateLShr(x, c1));
					x = builder.CreateMul(x, c2);
					x = builder.CreateXor(x, builder.CreateLShr(x, c1));
					x = builder.CreateMul(x, c2);
					x = builder.CreateXor(x, builder.CreateLShr(x, c1));
					hash = x;
#endif
				} else if (type->is_string()) {
#if 1
					auto cache_val = heap_strhash(builder, value.value[0]);

					auto bb_previous = builder.GetInsertBlock();
					auto bb_noval = llvm::BasicBlock::Create(context, "strhash_null",
						llvm_function);
					auto bb_post = llvm::BasicBlock::Create(context, "strhash_post",
						llvm_function);

					builder.CreateCondBr(
						create_expect_mostly(builder.CreateIsNull(cache_val), 0),
						bb_noval, bb_post);

					builder.SetInsertPoint(bb_noval);
					auto new_val = builder.CreateCall(get_llvm_compiler_strhash(), value.value[0]);
					builder.CreateBr(bb_post);

					builder.SetInsertPoint(bb_post);
					auto phi = builder.CreatePHI(builder.getInt64Ty(), 2, "strhash");
					phi->addIncoming(cache_val, bb_previous);
					phi->addIncoming(new_val, bb_noval);

					hash = phi;
#else
					hash = builder.CreateCall(get_llvm_compiler_strhash(), value.value[0]);
#endif
				} else {
					ASSERT(false && "must be integer");
				}

				ASSERT(hash);

				if (rehash) {
					// https://stackoverflow.com/questions/8513911/how-to-create-a-good-hash-combine-with-64-bit-output-inspired-by-boosthash-co
					auto other_hash = get(fun.args[0]);
					ASSERT(other_hash.value.size() == 1);

					auto seed = other_hash.value[0];
#if 0
	#if 1
					auto const1 = builder.getInt64(0x9ddfea08eb382d69ULL);
					auto a = builder.CreateMul(builder.CreateXor(hash, seed), const1);

					a = builder.CreateXor(a,
						builder.CreateLShr(a, builder.getInt64(47)));

					auto b = builder.CreateMul(builder.CreateXor(seed, a), const1);
					b = builder.CreateXor(b,
						builder.CreateLShr(b, builder.getInt64(47)));

					hash = builder.CreateMul(b, const1);
	#else
					hash = builder.CreateXor(seed, hash);
	#endif
#else
					hash = builder.CreateXor(seed,
						builder.CreateMul(hash, builder.getInt64(18446744073709551557ull)));
#endif
				}

				// comment_int(builder, rehash ? "kRehash" : "kHash", hash);

				return hash;
			}

		case Function::Tag::kExtractYear:
			{
				ASSERT(fun.args.size() == 1);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);

#if 0
				auto a = value_a.value[0];
				return builder.CreateCall(get_llvm_compiler_extract_year(), {a});
#else
				auto jd = value_a.value[0];

				auto a = builder.CreateAdd(jd, builder.getInt32(32044));
				auto b = builder.CreateSDiv(
					builder.CreateAdd(
						builder.CreateMul(a, builder.getInt32(4)),
						builder.getInt32(3)),
					builder.getInt32(146097));

				auto c = builder.CreateSub(
					a,
					builder.CreateSDiv(
						builder.CreateMul(builder.getInt32(146097), b),
						builder.getInt32(4)
						));

				auto d = builder.CreateSDiv(
					builder.CreateAdd(
						builder.getInt32(3),
						builder.CreateMul(builder.getInt32(4), c)),
					builder.getInt32(1461));

				auto e = builder.CreateSub(
					c,
					builder.CreateSDiv(
						builder.CreateMul(builder.getInt32(1461), d),
						builder.getInt32(4)));

				auto m = builder.CreateSDiv(
					builder.CreateAdd(
						builder.CreateMul(e, builder.getInt32(5)),
						builder.getInt32(2)),
					builder.getInt32(153));

				auto x = builder.CreateAdd(
					builder.CreateMul(b, builder.getInt32(100)),
					d);
				x = builder.CreateSub(x, builder.getInt32(4800));
				x = builder.CreateAdd(
					x,
					builder.CreateSDiv(m, builder.getInt32(10)));

				x = builder.CreateSExtOrTrunc(x, builder.getInt16Ty());
				return x;
#endif
			}

		case Function::Tag::kAritAdd:
		case Function::Tag::kAritSub:
		case Function::Tag::kAritMul:
			{
				ASSERT(fun.args.size() == 2);

				auto value_a = get(fun.args[0]);
				auto value_b = get(fun.args[1]);

				ASSERT(value_a.value.size() == 1);
				ASSERT(value_b.value.size() == 1);

				auto a = value_a.value[0];
				auto b = value_b.value[0];

				bool already_casted_to_result = false;
				arith_cast2(builder, a, b, result_type,
					&already_casted_to_result);

				llvm::Value* r = nullptr;
				switch (fun.tag) {
				case Function::Tag::kAritAdd:
					r = builder.CreateAdd(a, b, "", false, true);
					break;
				case Function::Tag::kAritSub:
					r = builder.CreateSub(a, b, "", false, true);
					break;
				case Function::Tag::kAritMul:
					r = builder.CreateMul(a, b, "", false, true);
					break;
				default:
					ASSERT(false);
					break;
				}

				return already_casted_to_result ?
					r : builder.CreateSExtOrTrunc(r, result_type);
			}

		case Function::Tag::kContains:
			{
				ASSERT(fun.args.size() == 2);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);
				auto value_b = get(fun.args[1]);
				ASSERT(value_b.value.size() == 1);

				auto a = value_a.value[0];
				auto b = value_b.value[0];

				auto type_0 = get_type(fun.args[0]);
				auto type_1 = get_type(fun.args[1]);
				ASSERT(type_0->is_string() && type_1->is_string());

				auto len_a = heap_strlen(builder, a);
				auto len_b = heap_strlen(builder, b);
#if 1
				llvm::Value* r = builder.CreateCall(get_llvm_compiler_strcontains(),
					{a, len_a, b, len_b});

				return builder.CreateICmpNE(r, builder.getInt32(0));
#else
				auto r = builder.CreateCall(get_llvm_compiler_memmem(), {
					a,
					builder.CreateSExtOrTrunc(len_a, builder.getInt64Ty()),
					b,
					builder.CreateSExtOrTrunc(len_b, builder.getInt64Ty())});
				return builder.CreateIsNotNull(r);
#endif
			}

		case Function::Tag::kIfThenElse:
			{
				ASSERT(fun.args.size() == 3);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);
				auto value_b = get(fun.args[1]);
				ASSERT(value_b.value.size() == 1);
				auto value_c = get(fun.args[2]);
				ASSERT(value_c.value.size() == 1);

				auto a = value_a.value[0];
				auto b = value_b.value[0];
				auto c = value_c.value[0];

				return builder.CreateSelect(builder.CreateIsNotNull(a), b, c);
			}

		case Function::Tag::kCmpEq:
		case Function::Tag::kCmpLt:
		case Function::Tag::kCmpGt:
		case Function::Tag::kCmpNe:
		case Function::Tag::kCmpLe:
		case Function::Tag::kCmpGe:
			{
				ASSERT(fun.args.size() == 2);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);
				auto value_b = get(fun.args[1]);
				ASSERT(value_b.value.size() == 1);

				auto a = value_a.value[0];
				auto b = value_b.value[0];
				auto a_type = get_type(fun.args[0]);
				auto b_type = get_type(fun.args[1]);

				return CreateComparison(fun.tag, a, b, a_type, b_type);
			}

		case Function::Tag::kBetweenBothIncl:
		case Function::Tag::kBetweenUpperExcl:
		case Function::Tag::kBetweenLowerExcl:
		case Function::Tag::kBetweenBothExcl:
			{
				bool excl_lo = fun.tag == Function::Tag::kBetweenBothExcl ||
					fun.tag == Function::Tag::kBetweenLowerExcl;
				bool excl_hi = fun.tag == Function::Tag::kBetweenBothExcl ||
					fun.tag == Function::Tag::kBetweenUpperExcl;

				ASSERT(fun.args.size() == 3);

				auto value_a = get(fun.args[0]);
				auto value_lo = get(fun.args[1]);
				auto value_hi = get(fun.args[2]);

				ASSERT(value_a.value.size() == 1);
				ASSERT(value_lo.value.size() == 1);
				ASSERT(value_hi.value.size() == 1);

				auto a = value_a.value[0];
				auto hi = value_hi.value[0];
				auto lo = value_lo.value[0];

				arith_cast2(builder, hi, lo);
				arith_cast2(builder, a, lo);
				arith_cast2(builder, a, hi);

				llvm::Value* less_than_hi = excl_hi ?
					builder.CreateICmpSLT(a, hi) :
					builder.CreateICmpSLE(a, hi);

				llvm::Value* greater_than_lo = excl_lo ?
					builder.CreateICmpSGT(a, lo) :
					builder.CreateICmpSGE(a, lo);

				return builder.CreateAnd(less_than_hi, greater_than_lo);
			}

		case Function::Tag::kLogAnd:
		case Function::Tag::kLogOr:
			{
				ASSERT(fun.args.size() == 2);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);
				auto value_b = get(fun.args[1]);
				ASSERT(value_b.value.size() == 1);

				auto a = value_a.value[0];
				auto b = value_b.value[0];

				// TODO: cast 'a' and 'b' to type of result
				switch (fun.tag) {
				case Function::Tag::kLogAnd:return builder.CreateAnd(a, b);
				case Function::Tag::kLogOr: return builder.CreateOr(a, b);
				default:
					ASSERT(false);
					break;
				}
				break;
			}

		case Function::Tag::kLogNot:
			{
				ASSERT(fun.args.size() == 1);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);

				return builder.CreateIsNull(value_a.value[0]);
			}

		case Function::Tag::kSelTrue:
		case Function::Tag::kSelFalse:
			{
				ASSERT(fun.args.size() == 1);

				auto value_a = get(fun.args[0]);
				ASSERT(value_a.value.size() == 1);

				auto val = value_a.value[0];
				llvm::Value* r = nullptr;

				switch (fun.tag) {
				case Function::Tag::kSelTrue:
					r = builder.CreateIsNotNull(val, "seltrue_val");
					break;
				case Function::Tag::kSelFalse:
					r = builder.CreateIsNull(val, "selfalse_val");
					break;
				default:
					ASSERT(false);
					break;
				}

				comment_int(builder, fun.tag2string(fun.tag), r, false);

				if (pred_just_conjuction) {
					ASSERT(fun.predicate);
					if (auto pred = translate_predicate(fun.predicate)) {
						r = builder.CreateAnd(pred, r);
					}
				}

				comment_int(builder, fun.tag2string(fun.tag) + "_pred", r, false);

				ASSERT(r && is_predicate_llvm_type(r->getType()));
				return r;
			}
			break;

		case Function::Tag::kSelUnion:
			ASSERT(false && "Handled at a different place");
			break;

		case Function::Tag::kWriteCol:
		case Function::Tag::kReadCol:
			{
				const bool is_read = fun.tag == Function::Tag::kReadCol;

				if (is_read) {
					ASSERT(fun.args.size() == 1);
				} else {
					ASSERT(fun.args.size() == 2);
				}

				ASSERT(fun.data_structure.get() && fun.data_structure_column.get());

				auto pos = get(fun.args[0]);
				ASSERT(pos.value.size() == 1 || pos.value.size() == 2);

				CompiledStruct cstruct;
				bool compiled = get_compiled_struct(cstruct, fun.data_structure);
				ASSERT(compiled && cstruct.row_type);

				auto phy_col_idx = fun.data_structure->get_phy_column_index(fun.data_structure_column->name);
				auto pos_struct = pos.value[0];

				if (pos.value.size() == 1) {
					pos_struct = builder.CreatePointerCast(pos_struct,
						llvm::PointerType::getUnqual(cstruct.row_type));
				} else {
					ASSERT(pos.value.size() == 2);

					auto pos_offset = pos.value[1];
					ASSERT(pos_struct && pos_offset);
					pos_offset = set_noalias_scope(builder.CreateLoad(pos_offset));

					comment_ptr(builder, "kWrite base ptr", pos_struct);
					comment_int(builder, "kWrite base off", pos_offset);
					pos_struct = builder.CreateGEP(pos_struct, pos_offset);

					comment_ptr(builder, "kWrite dest ptr", pos_struct);
				}

				auto gep = builder.CreateStructGEP(pos_struct, phy_col_idx);

				if (is_read) {
					return set_noalias_scope(builder.CreateLoad(gep));
				} else {
					auto value = get(fun.args[1]);
					ASSERT(value.value.size() == 1);

					set_noalias(builder.CreateStore(value.value[0], gep));
					return kInvalidPointer;
				}
			}
			break;

		case Function::Tag::kWritePos:
			{
				llvm::Value* r = nullptr;
				const bool has_result = fun.tag != Function::Tag::kBucketScatter;

				fun.dump();

				CompiledStruct cstruct;
				bool compiled = get_compiled_struct(cstruct, fun.data_structure);
				ASSERT(fun.data_structure.get());
				ASSERT(compiled && cstruct.row_type);

				auto row_type_ptr = llvm::PointerType::getUnqual(cstruct.row_type);
#ifndef JIT_IMPROVEMENTS1
				return builder.CreateCall(get_llvm_compiler_writepos1(), {
					func_arg, cstruct.structure
				});
#else
				auto init_bucket_curr_num = create_alloca_var(
					builder.getInt64Ty(),
					"write_pos_0",
					[&] (auto& builder, auto& var_ptr) {
						builder.CreateStore(builder.getInt64(0), var_ptr);
					}, AllocaVarKey { pfun.get(), 0 });

				auto bucket_curr_num = create_alloca_var(
					llvm::PointerType::getUnqual(builder.getInt64Ty()),
					"write_pos_curr_num",
					[&] (auto& builder, auto& var_ptr) {
						builder.CreateStore(init_bucket_curr_num, var_ptr);
					}, AllocaVarKey { pfun.get(), 1 });
				auto bucket_max_num = create_alloca_var(
					builder.getInt64Ty(),
					"write_pos_max_num",
					[&] (auto& builder, auto& var_ptr) {
						builder.CreateStore(builder.getInt64(0), var_ptr);
					}, AllocaVarKey { pfun.get(), 2 });

				auto bucket_data = create_alloca_var(
					row_type_ptr,
					"write_pos_data",
					[&] (auto& builder, auto& var_ptr) {
						builder.CreateStore(
							builder.CreateIntToPtr(builder.getInt64(0), row_type_ptr),
							var_ptr);
					}, AllocaVarKey { pfun.get(), 3 });

				auto out_of_space = create_expect_mostly(
					builder.CreateICmpSGE(
						builder.CreateLoad(builder.CreateLoad(bucket_curr_num)),
						builder.CreateLoad(bucket_max_num)),
					0);

				if_then_else(out_of_space, "write_pos_out_of_space",
					[&] (bool branch) {
						ASSERT(branch);

						builder.CreateStore(
							builder.CreatePointerCast(
								builder.CreateCall(get_llvm_compiler_alloc_write_pos_chunk(), {
									func_arg, cstruct.structure,
									bucket_curr_num, bucket_max_num
								}),
								row_type_ptr),
							bucket_data);
					}, false);

				auto pcurr = builder.CreateLoad(bucket_curr_num);
				auto curr = builder.CreateLoad(pcurr);

				ASSERT(cstruct.row_width);

				auto row_ptr = set_noalias_scope(builder.CreateLoad(bucket_data));
				auto dest_ptr = builder.CreateGEP(row_ptr, curr);

				bool force = false;
				comment_int(builder, "write_pos_curr_num", curr, force);
				comment_ptr(builder, "write_pos_row_ptr", row_ptr, false, force);
				comment_ptr(builder, "write_pos_dest_ptr", dest_ptr, false, force);

				// curr++
				set_noalias(builder.CreateStore(builder.CreateAdd(curr, builder.getInt64(1)),
					pcurr));

				comment_ptr(builder, "bucket_curr_num", bucket_curr_num, false, force);
				comment_ptr(builder, "pcurr", pcurr, false, force);
				comment_int(builder, "curr", curr, force);

				return builder.CreatePointerCast(dest_ptr, builder.getInt8PtrTy());
#endif
			}
			break;

		case Function::Tag::kBloomFilterLookup1:
		case Function::Tag::kBloomFilterLookup2:
			{
				using BloomFilter = engine::table::BloomFilter;

				size_t used_bits = fun.tag == Function::Tag::kBloomFilterLookup1 ? 1 : 2;
				ASSERT(used_bits <= BloomFilter::kNumBits);

				CompiledStruct cstruct;
				bool compiled = get_compiled_struct(cstruct, fun.data_structure);
				ASSERT(fun.data_structure.get());
				ASSERT(compiled && cstruct.row_type);

				auto array = cstruct.bf_words;
				auto mask = cstruct.bf_mask;

				auto hash = get(fun.args[0]);
				ASSERT(hash.value.size() == 1 && hash.value[0]);

				auto one = builder.getInt64(1);

				auto shift0 = hash.value[0];
				auto bit0_idx = builder.CreateAnd(shift0,
					builder.getInt64(BloomFilter::kCellBitMask));
				auto bit0_mask = builder.CreateShl(one, bit0_idx);
				auto bit_mask = bit0_mask;

				if (used_bits == 2) {
					auto shift1 = builder.CreateLShr(hash.value[0],
						builder.getInt64(1*BloomFilter::kCellWordShift));
					auto bit1_idx = builder.CreateAnd(shift1,
						builder.getInt64(BloomFilter::kCellBitMask));
					auto bit1_mask = builder.CreateShl(one, bit1_idx);
					bit_mask = builder.CreateOr(bit_mask, bit1_mask);

					auto shift2 = builder.CreateLShr(hash.value[0],
						builder.getInt64(2*BloomFilter::kCellWordShift));

					auto gep = builder.CreateInBoundsGEP(array,
						builder.CreateAnd(shift2, mask));

					return builder.CreateICmpEQ(
						bit_mask,
						builder.CreateAnd(set_noalias_scope(builder.CreateLoad(gep)),
							bit_mask));
				} else {
					ASSERT(used_bits == 1);

					auto shift2 = builder.CreateLShr(hash.value[0],
						builder.getInt64(2*BloomFilter::kCellWordShift));

					auto gep = builder.CreateInBoundsGEP(array,
						builder.CreateAnd(shift2, mask));

					return builder.CreateIsNotNull(
						builder.CreateAnd(set_noalias_scope(builder.CreateLoad(gep)),
							bit_mask));
				}
			}
			break;

		case Function::Tag::kBucketLookup:
		case Function::Tag::kBucketInsert:
		case Function::Tag::kBucketGather:
		case Function::Tag::kBucketCheck:
		case Function::Tag::kBucketScatter:
		case Function::Tag::kBucketNext:

		case Function::Tag::kBucketAggrSum:
		case Function::Tag::kBucketAggrMin:
		case Function::Tag::kBucketAggrMax:
		case Function::Tag::kBucketAggrCount:

		case Function::Tag::kGlobalAggrSum:
		case Function::Tag::kGlobalAggrCount:
		case Function::Tag::kGlobalAggrMin:
		case Function::Tag::kGlobalAggrMax:
			{
				llvm::Value* r = nullptr;
				const bool has_result = fun.tag != Function::Tag::kBucketScatter;

				fun.dump();

				CompiledStruct cstruct;
				bool compiled = get_compiled_struct(cstruct, fun.data_structure);
				ASSERT(fun.data_structure.get());
				ASSERT(compiled && cstruct.row_type);

				if (fun.tag == Function::Tag::kBucketLookup ||
						fun.tag == Function::Tag::kBucketInsert) {
					// lookup bucket
					ASSERT(cstruct.buckets && cstruct.pbuckets);
					LOG_DEBUG("TODO: Use old 'auto array = cstruct.buckets;' when "
						"absence of kBucketInsert can be proven. Same for cstruct.mask");

					ASSERT(fun.args.size() == 1);
					auto hash = get(fun.args[0]);
					ASSERT(hash.value.size() == 1);

					ASSERT(hash.value[0]);

					if (fun.tag == Function::kBucketLookup) {
						llvm::Value* array = fun.data_structure_can_change ?
							builder.CreateLoad(cstruct.pbuckets) : cstruct.buckets;
						llvm::Value* mask = fun.data_structure_can_change ?
							builder.CreateLoad(cstruct.pmask) : cstruct.mask;
						ASSERT(array && mask);

						auto index = builder.CreateAnd(hash.value[0], mask);
						auto gep = builder.CreateInBoundsGEP(array, index);

						comment_int(builder, "kBucketLookup hash", hash.value[0]);
						comment_int(builder, "kBucketLookup index", index);

						r = set_noalias_scope(builder.CreateLoad(gep));
						comment_ptr(builder, "kBucketLookup result", r, true, false);


						LOG_TRACE("bucket_type %s", llvm_to_string(r).c_str());
						comment_ptr(builder, "kBucketLookup ptr", r);
					} else {
						ASSERT(fun.tag == Function::Tag::kBucketInsert);
#ifndef JIT_IMPROVEMENTS2
						r = builder.CreateCall(get_llvm_compiler_htappend1(), {
							func_arg, cstruct.pbuckets, cstruct.pmask,
							cstruct.structure, hash.value[0]
						});
#else
						r = nullptr;

						auto row_type_ptr = llvm::PointerType::getUnqual(cstruct.row_type);

						auto init0 = create_alloca_var(
							builder.getInt64Ty(),
							"bucket_insert_num0",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(builder.getInt64(0), var_ptr);
							}, AllocaVarKey { pfun.get(), 0 });

						auto bucket_curr_num = create_alloca_var(
							llvm::PointerType::getUnqual(builder.getInt64Ty()),
							"bucket_insert_curr_num",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(init0, var_ptr);
							}, AllocaVarKey { pfun.get(), 1 });
						auto bucket_max_num = create_alloca_var(
							builder.getInt64Ty(),
							"bucket_insert_max_num",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(builder.getInt64(0), var_ptr);
							}, AllocaVarKey { pfun.get(), 2 });
						auto bucket_data = create_alloca_var(
							row_type_ptr,
							"bucket_insert_data",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(
									builder.CreateIntToPtr(builder.getInt64(0), row_type_ptr),
									var_ptr);
							}, AllocaVarKey { pfun.get(), 3 });

						auto table_count = create_alloca_var(
							llvm::PointerType::getUnqual(builder.getInt64Ty()),
							"bucket_insert_table_count",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(init0, var_ptr);
							}, AllocaVarKey { pfun.get(), 4 });
						auto table_max = create_alloca_var(
							builder.getInt64Ty(),
							"bucket_insert_table_max",
							[&] (auto& builder, auto& var_ptr) {
								builder.CreateStore(builder.getInt64(0), var_ptr);
							}, AllocaVarKey { pfun.get(), 5 });

						auto bb_out_of_space = llvm::BasicBlock::Create(context,
							"bucket_insert_out_of_space", llvm_function);
						auto bb_check = llvm::BasicBlock::Create(context,
							"bucket_insert_check", llvm_function);
						auto bb_update = llvm::BasicBlock::Create(context,
							"bucket_insert_update", llvm_function);


						auto block_out_of_space = create_expect_mostly(
							builder.CreateICmpSGE(
								builder.CreateLoad(builder.CreateLoad(bucket_curr_num)),
								builder.CreateLoad(bucket_max_num)),
							0);
						builder.CreateCondBr(block_out_of_space, bb_out_of_space, bb_check);



						builder.SetInsertPoint(bb_check);
						auto table_out_of_space = create_expect_mostly(
							builder.CreateICmpSGE(
								builder.CreateLoad(builder.CreateLoad(table_count)),
								builder.CreateLoad(table_max)),
							0);
						builder.CreateCondBr(table_out_of_space, bb_out_of_space, bb_update);



						builder.SetInsertPoint(bb_out_of_space);
						builder.CreateStore(
							builder.CreatePointerCast(
								builder.CreateCall(get_llvm_compiler_alloc_bucket_insert(), {
									func_arg, cstruct.structure,
									table_count, table_max,
									bucket_curr_num, bucket_max_num,
									cstruct.pbuckets, cstruct.pmask,
								}),
								row_type_ptr),
							bucket_data);
						builder.CreateBr(bb_update);



						builder.SetInsertPoint(bb_update);
						auto pcurr = builder.CreateLoad(bucket_curr_num);
						auto curr = builder.CreateLoad(pcurr);
						auto pcount = builder.CreateLoad(table_count);
						auto count = builder.CreateLoad(pcount);

						ASSERT(cstruct.row_width);

						auto row_ptr = builder.CreateLoad(bucket_data);
						auto dest_ptr = builder.CreateGEP(row_ptr, curr);

						bool force = false;
						comment_int(builder, "insert_curr_num", curr, force);
						comment_ptr(builder, "insert_row_ptr", row_ptr, false, force);
						comment_ptr(builder, "insert_dest_ptr", dest_ptr, false, force);

						// curr++
						builder.CreateStore(builder.CreateAdd(curr, builder.getInt64(1)),
							pcurr);
						builder.CreateStore(builder.CreateAdd(count, builder.getInt64(1)),
							pcount);

						comment_ptr(builder, "bucket_curr_num", bucket_curr_num, false, force);
						comment_ptr(builder, "pcurr", pcurr, false, force);
						comment_int(builder, "curr", curr, force);
						comment_ptr(builder, "pcount", pcount, false, force);
						comment_int(builder, "count", count, force);

						// update values
						auto mask = builder.CreateLoad(cstruct.pmask);
						auto buckets = builder.CreateLoad(cstruct.pbuckets);

						r = builder.CreatePointerCast(dest_ptr, builder.getInt8PtrTy());
#if 0
						builder.CreateCall(get_llvm_compiler_alloc_bucket_insert_dbg_update(), {
							func_arg, cstruct.structure,
							hash.value[0], r, mask, buckets
						});
#else
						auto index = builder.CreateAnd(hash.value[0], mask);
						auto head_ptr = builder.CreateInBoundsGEP(buckets, index);
						auto next_ptr = builder.CreateStructGEP(dest_ptr,
							fun.data_structure->get_phy_next_column_index());

						auto old_ptr = builder.CreateLoad(head_ptr);

						builder.CreateStore(old_ptr, next_ptr);
						builder.CreateStore(r, head_ptr);
#endif
#endif
					}
				} else {
					// use looked up bucket
					ASSERT(fun.args.size() > 0);
					auto bucket = get(fun.args[0]);
					ASSERT(bucket.value.size() == 1);

					ASSERT(fun.predicate);

					auto is_read = fun.get_flags() & voila::Function::kReadsFromMemory;
					llvm::Value* pointer = bucket.value[0];

					switch (predication_args.predicated) {
					case PredGenArgs::Predication::kNone:
						break;

					case PredGenArgs::Predication::kSelect:
						ASSERT(predication_args.compiled_predicate);
						pointer = builder.CreateSelect(predication_args.compiled_predicate,
							pointer,
							is_read ? stream_access_safe_space_zero : stream_access_safe_space);
						break;

					case PredGenArgs::Predication::kMultiply:
						ASSERT(predication_args.compiled_predicate);
						{
							auto int_ptr_type = builder.getInt64Ty();
							static_assert(sizeof(void*) == 8, "assumes 64-bit");

							auto dst_ptr_type = pointer->getType();

							auto mul_val = builder.CreateAdd(
								builder.CreateMul(
									builder.CreateZExt(
										predication_args.compiled_predicate,
										int_ptr_type),
									builder.CreatePtrToInt(pointer, int_ptr_type)),
								builder.CreateMul(
									builder.CreateZExt(
										builder.CreateNot(predication_args.compiled_predicate),
										int_ptr_type),
									builder.CreatePtrToInt(is_read ? stream_access_safe_space_zero : stream_access_safe_space,
										int_ptr_type))
								);

							pointer = builder.CreateIntToPtr(mul_val, dst_ptr_type);
						}
						break;

					default:
						ASSERT(false && "Unhandled");
						break;
					}

					auto row_ptr = builder.CreatePointerCast(pointer,
						llvm::PointerType::getUnqual(cstruct.row_type));

					const bool is_next = fun.tag == Function::Tag::kBucketNext;
					comment_ptr(builder,
							fun.tag2string(fun.tag) + " row", row_ptr,
							false);


					ASSERT(is_next || fun.data_structure_column);

					size_t phy_col_idx = is_next ?
						fun.data_structure->get_phy_next_column_index() :
						fun.data_structure->get_phy_column_index(fun.data_structure_column->name);

					auto col_ptr = builder.CreateStructGEP(row_ptr, phy_col_idx);

					switch (fun.tag) {
					case Function::Tag::kBucketGather:
					case Function::Tag::kBucketNext:
						ASSERT(fun.args.size() == 1);
						comment_ptr(builder,
							is_next ? "kBucketNext ptr" : "kBucketGather ptr", col_ptr,
							false);
						r = set_noalias_scope(builder.CreateLoad(col_ptr));
						if (is_next) {
							comment_ptr(builder, "kBucketNext r", r);
						}
						break;

					case Function::Tag::kBucketCheck:
						ASSERT(fun.args.size() == 2);
						{
							auto value = get(fun.args[1]);
							ASSERT(value.value.size() == 1);

							auto a_type = get_type(fun.args[1]);

							r = set_noalias_scope(builder.CreateLoad(col_ptr));
							r = CreateComparison(Function::Tag::kCmpEq,
								r, value.value[0],
								a_type, a_type);
						}
						break;

					case Function::Tag::kBucketScatter:
						ASSERT(fun.args.size() == 2);
						{
							auto value = get(fun.args[1]);
							ASSERT(value.value.size() == 1);

							comment_ptr(builder, "kBucketScatter ptr", col_ptr, true);

							set_noalias(builder.CreateStore(value.value[0], col_ptr));
						}
						break;

					case Function::Tag::kBucketAggrSum:
					case Function::Tag::kBucketAggrCount:
					case Function::Tag::kBucketAggrMin:
					case Function::Tag::kBucketAggrMax:
					case Function::Tag::kGlobalAggrSum:
					case Function::Tag::kGlobalAggrCount:
					case Function::Tag::kGlobalAggrMin:
					case Function::Tag::kGlobalAggrMax:
						aggregate(builder, fun, col_ptr, [&] () {
							ASSERT(fun.args.size() == 2);
							auto value = get(fun.args[1]);
							ASSERT(value.value.size() == 1);
							return value.value[0];
						});
						break;

					default:
						ASSERT(false && "Invalid");
						break;
					}
				}
				return has_result ? r : kInvalidPointer;
			}
			break;

		default:
			LOG_ERROR("Unhandled function %s", Function::tag2cstring(fun.tag));
			THROW_MSG(CannotCompileException, "Cannot compile unhandled function tag");
			return nullptr;
		}

	};

	size_t num_calls = 0;

	PredicationOpts predication_opts(*request.flavor);
	predication_opts.can_be_fully_evaluated = fun.can_evaluate_speculatively(*request.context) &&
		request.flavor->get_int(FlavorSpec::Entry::kPredicated) > 0;
	predication_opts.simple = 0;
	if (can_be_predicated(fun)) {
		predication_opts.simple =
			request.flavor->get_int(FlavorSpec::Entry::kPredicated);
		ASSERT(predication_opts.simple >= 0 && predication_opts.simple <= 2);
	}

	auto r = predicated_expr(
		[&] (llvm::Type*& out_result_type, auto args) {
			ASSERT(!num_calls);
			auto r = gen(args);

			if (r != kInvalidPointer) {
				out_result_type = r->getType();
			}

			num_calls++;
			return r;
		},
		pred_just_conjuction ? nullptr : fun.predicate,
		fun.tag2string(fun.tag),

		std::move(predication_opts));

	if (r && r != kInvalidPointer) {
		put_simple(&fun, r);
	}
}

static llvm::PHINode* as_phi_node(llvm::Instruction* i)
{
	return llvm::dyn_cast<llvm::PHINode>(i);
}

llvm::Value*
LlvmCompiler::create_predicated(llvm::Value* compiled_predicate,
	const std::function<llvm::Value*(llvm::Type*& result_type, const PredGenArgs& args)>& generate,
	const voila::Expr& voila_predicate, const std::string& prefix,
	const PredicationOpts& opts)
{
	PredGenArgs args;
	args.compiled_predicate = compiled_predicate;
	args.predicated = PredGenArgs::Predication::kNone;

	llvm::Type* result_type = nullptr;
	if (!compiled_predicate ||
			(opts.can_be_fully_evaluated && opts.allow_full_eval)) {
		args.predicated = PredGenArgs::Predication::kNone;
		return generate(result_type, args);
	}

	if (opts.simple) {
		switch (opts.simple) {
		case 1:
			args.predicated = PredGenArgs::Predication::kMultiply;
			break;
		case 2:
			args.predicated = PredGenArgs::Predication::kSelect;
			break;
		default:
			ASSERT(false && "invalid value for opts.simple");
			break;
		}
		args.predicated = PredGenArgs::Predication::kMultiply;
		return generate(result_type, args);
	}

	auto func = llvm_function;

	auto bb_previous = builder.GetInsertBlock();
	auto bb_true = llvm::BasicBlock::Create(context, prefix + "_ptrue", func);
	auto bb_merge = llvm::BasicBlock::Create(context, prefix + "_merge", func);
	builder.CreateCondBr(compiled_predicate, bb_true, bb_merge);

	builder.SetInsertPoint(bb_true);
	llvm::Value* compiled_result_true = generate(result_type, args);
	auto bb_new_true = builder.GetInsertBlock();
	builder.CreateBr(bb_merge);

	builder.SetInsertPoint(bb_merge);
	bool has_result = compiled_result_true && compiled_result_true != kInvalidPointer;
	llvm::Value* result = nullptr;
	if (has_result) {
		result_type = compiled_result_true->getType();
		auto phi = builder.CreatePHI(result_type, 2, prefix);
		phi->addIncoming(compiled_result_true, bb_new_true);
		phi->addIncoming(get_undef(result_type), bb_previous);
		result = phi;
	} else {
		// no value returned, create if-then and avoid else path
		result = compiled_result_true;
	}

	return result;
}


void
LlvmCompiler::on_expression(const std::shared_ptr<Input>& e)
{
	THROW_MSG(CannotCompileException, "Cannot compile Input");
}

void
LlvmCompiler::on_expression(const std::shared_ptr<InputPredicate>& e)
{
	THROW_MSG(CannotCompileException, "Cannot compile InputPredicate");
}

void
LlvmCompiler::on_expression(const std::shared_ptr<GetScanPos>& e)
{
	THROW_MSG(CannotCompileException, "Cannot compile GetScanPos");
}

void
LlvmCompiler::on_expression(const std::shared_ptr<Scan>& e)
{
	THROW_MSG(CannotCompileException, "Cannot compile Scan");
}

void
LlvmCompiler::on_expression(const std::shared_ptr<ReadVariable>& e)
{
	if (is_extended()) {
		auto var_it = variables.find(e->var.get());

		if (var_it == variables.end()) {
			e->dump();
			ASSERT(var_it != variables.end());
		}

		ASSERT(var_it->second.value.size() == 1);
		auto var_ptr = var_it->second.value[0];

		ASSERT(var_ptr);

		auto val = set_noalias_scope(builder.CreateLoad(var_ptr));
		comment_msg(builder, "ReadVariable " + e->var->dbg_name, val);
		comment_msg(builder, "ReadVariable from", var_ptr);

		put_simple(e.get(), val);
	} else {
		THROW_MSG(CannotCompileException, "Cannot compile ReadVariable");
	}
}

void
LlvmCompiler::on_expression(const std::shared_ptr<Constant>& e)
{
	VoilaTreePass::on_expression(e);

	const auto type = get_type(e.get());
	llvm::Value* result = nullptr;

	if (type->is_integer()) {
		const auto bits = type->num_bits();
		ASSERT(bits > 0);

		result = builder.getIntN(bits, std::stoll(e->value));
	} else {
		ASSERT(false && "Todo: Other types");
	}

	ASSERT(result);
	put_simple(e.get(), result);
}

void
LlvmCompiler::on_expression(const std::shared_ptr<MetaExpression>& e)
{
	VoilaTreePass::on_expression(e);
}

void
LlvmCompiler::handle(const Expr& s)
{
	auto it = cache.find(s.get());
	if (it == cache.end()) {
		VoilaTreePass::handle(s);
	}
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Assign>& b)
{
	if (!is_extended()) {
		THROW_MSG(CannotCompileException, "Cannot compile Assign");
	}
	const auto& expr = b->value;
	handle(expr);

	bool use_predicate = !predication_is_just_conjunction(*b->value);

	PredicationOpts predication_opts(*request.flavor);

	predicated(
		[&] () {
			auto expr_val = get(expr);

			auto var_it = variables.find(b->var.get());
			ASSERT(var_it != variables.end());

			ASSERT(var_it->second.value.size() == 1);
			ASSERT(expr_val.value.size() == 1);
			auto var_ptr = var_it->second.value[0];
			auto value = expr_val.value[0];

			ASSERT(var_ptr && value);

			b->dump();
			if (request.contains_sink(b)) {
				// LOG_ERROR("TODO: or contains output variable");
				LOG_DEBUG("Sink, store and translate intermediate values (assign)");
				auto var_tpe = get_type(b->var.get());
				auto val_tpe = get_type(expr);

				store_in_sink(b.get(), expr);
			}
			if (request.contains_sink(b->var)) {
				// LOG_ERROR("TODO: or contains output variable");
				LOG_DEBUG("Sink, store and translate intermediate values (var)");
				auto var_tpe = get_type(b->var.get());
				auto val_tpe = get_type(expr);

				store_in_sink(b->var.get(), expr);
			}
			LOG_DEBUG("on_statement(Assign): var_ptr type: '%s', val type: '%s'",
				llvm_to_string(var_ptr->getType()).c_str(),
				llvm_to_string(value->getType()).c_str());

			comment_ptr(builder, "Assign store", var_ptr, false);
			comment_msg(builder, "Assign " + b->var->dbg_name, value);

			auto stored_value = value;
			if (value->getType()->isIntegerTy()) {
				auto t = var_ptr->getType();
				auto ptr_type = llvm::dyn_cast<llvm::PointerType>(t);
				ASSERT(ptr_type);
				ASSERT(ptr_type->getNumContainedTypes() == 1);
				auto contained_type = ptr_type->getContainedType(0);

				stored_value = builder.CreateSExtOrTrunc(value, contained_type);
			}


			set_noalias(builder.CreateStore(stored_value, var_ptr));
		},
		use_predicate ? b->predicate : nullptr,
		"assign_" + b->var->dbg_name,
		std::move(predication_opts)
	);
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Block>& b)
{
	THROW_MSG(CannotCompileException, "Cannot compile Block");
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Emit>& b)
{
	THROW_MSG(CannotCompileException, "Cannot compile Emit");
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Effect>& b)
{
	if (!is_extended()) {
		THROW_MSG(CannotCompileException, "Cannot compile Effect");
	}

	handle(b->value);
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Loop>& b)
{
	if (is_extended()) {
		auto func = llvm_function;

		const std::string prefix("vloop" + std::to_string(id_counter));
		id_counter++;

		auto bb_header = llvm::BasicBlock::Create(context, prefix + "_header", func);
		auto bb_trailer = llvm::BasicBlock::Create(context, prefix + "_trailer", func);
		auto bb_body = llvm::BasicBlock::Create(context, prefix + "_body", func);
		builder.CreateBr(bb_header);
		builder.SetInsertPoint(bb_header);

		handle(b->predicate);
		auto pred_val = get(b->predicate);

		ASSERT(pred_val.value.size() == 1);
		auto cond = pred_val.value[0];
		comment_int(builder, prefix + "_header", cond, false);

		builder.CreateCondBr(cond, bb_body, bb_trailer);

		builder.SetInsertPoint(bb_body);
		comment(builder, prefix + "_body");
		for (auto& stmt : b->statements) {
			handle(stmt);
		}

		comment(builder, prefix + "_return_header");
		builder.CreateBr(bb_header);

		builder.SetInsertPoint(bb_trailer);
		comment(builder, prefix + "_trailer");
	} else {
		THROW_MSG(CannotCompileException, "Cannot compile Loop");
	}
}

void
LlvmCompiler::on_statement(const std::shared_ptr<EndOfFlow>& b)
{
	if (!is_extended()) {
		THROW_MSG(CannotCompileException, "Cannot compile EndOfFlow");
	}
}

void
LlvmCompiler::on_statement(const std::shared_ptr<Comment>& b)
{
	if (!is_extended()) {
		THROW_MSG(CannotCompileException, "Cannot compile Comment");
	}
}

void
LlvmCompiler::on_statement(const std::shared_ptr<MetaStatement>& b)
{
	VoilaTreePass::on_statement(b);
}

template<typename S, typename T>
static void
check_physical_type(const S& ds, const T& physical, const char* dbg_path)
{
	if (UNLIKELY(!physical)) {
		LOG_ERROR("LlvmCompiler::%s: Cannot compile, because physical_table went away. ds=%p",
			dbg_path, ds.get());
		throw OutOfBudgetException("check_physical_type");
	}
}

void
LlvmCompiler::compile_struct(const voila::DataStruct& ds, size_t idx)
{
	auto physical = ds->physical_table.get();
	check_physical_type(ds, physical, "compile_struct");

	auto it = data_structs.find(physical);
	ASSERT(it == data_structs.end());

	auto layout = physical->get_layout();
	size_t num_cols = layout.get_num_columns();
	ASSERT(num_cols > 0);

	auto padding_at_end = layout.get_row_padding_at_end();

	std::vector<llvm::Type*> cols;
	cols.reserve(num_cols + padding_at_end);
	LOG_TRACE("compile_struct: Translating Struct");
	for (size_t i=0; i<num_cols; i++) {
		auto t = compile_type(builder, *layout.get_column_type(i));
		cols.emplace_back(t);
		LOG_TRACE("compile_struct: col: %s", llvm_to_string(t).c_str());
	}

	for (size_t i=0; i<padding_at_end; i++) {
		cols.emplace_back(builder.getInt8Ty());
	}

	auto row_type = llvm::StructType::create(context, std::move(cols), "TableRow");

	// make sure the data layout matches
	{
		const auto& data_layout = fragment.get_data_layout();
		auto llvm_layout = data_layout.getStructLayout(row_type);

		auto dl_width = llvm_layout->getSizeInBytes();
		ASSERT(dl_width == layout.get_row_width());

		for (size_t i=0; i<num_cols; i++) {
			auto dl_offset = llvm_layout->getElementOffset(i);
			auto exp_offset = layout.get_column_offset(i);
			ASSERT(dl_offset == exp_offset);
		}
	}

	const size_t section_idx = 2;

	llvm::Value* ptr = func_arg;
	llvm::Value* mask = nullptr;
	llvm::Value* buckets = nullptr;
	llvm::Value* structure = nullptr;

	ptr = builder.CreateGEP(
		set_noalias_scope(builder.CreateLoad(builder.CreateGEP(ptr,
			llvm::ArrayRef<llvm::Value*> {
				builder.getInt32(0), // pointer
				builder.getInt32(section_idx),
			}))),
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(0),
		});

	mask = builder.CreateGEP(ptr,
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(idx),
			builder.getInt32(0)
		});

	buckets = builder.CreateGEP(ptr,
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(idx),
			builder.getInt32(1)
		});

	structure = builder.CreateGEP(ptr,
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(idx),
			builder.getInt32(2)
		});


	mask = set_noalias_scope(builder.CreateLoad(mask));
	mask = set_noalias_scope(builder.CreateLoad(mask));

	buckets = set_noalias_scope(builder.CreateLoad(buckets));
	buckets = set_noalias_scope(builder.CreateLoad(buckets));

	structure = set_noalias_scope(builder.CreateLoad(structure));


	auto bf_mask = builder.CreateGEP(ptr,
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(idx),
			builder.getInt32(3)
		});

	auto bf_words = builder.CreateGEP(ptr,
		llvm::ArrayRef<llvm::Value*> {
			builder.getInt32(idx),
			builder.getInt32(4)
		});

	bf_mask = set_noalias_scope(builder.CreateLoad(bf_mask));
	bf_mask = set_noalias_scope(builder.CreateLoad(bf_mask));

	bf_words = set_noalias_scope(builder.CreateLoad(bf_words));
	bf_words = set_noalias_scope(builder.CreateLoad(bf_words));


	// comment_ptr(builder, "buckets", buckets, false);

	LOG_DEBUG("row_type %s, mask %s, buckets %s",
		llvm_to_string(row_type).c_str(),
		llvm_to_string(mask->getType()).c_str(),
		llvm_to_string(buckets->getType()).c_str());

	auto pbuckets = builder.CreateAlloca(buckets->getType(), nullptr);
	builder.CreateStore(buckets, pbuckets);

	auto pmask = builder.CreateAlloca(mask->getType(), nullptr);
	builder.CreateStore(mask, pmask);

	data_structs.insert({physical, CompiledStruct {
		row_type, mask, buckets, structure, pbuckets, pmask, layout.get_row_width(),
		bf_words, bf_mask
	}});

	if (llvm_compiler_check_struct) {
		builder.CreateCall(llvm_compiler_check_struct, {
			func_arg,
			builder.getInt64(idx),
			builder.CreatePointerCast(
				builder.CreateGlobalString(layout.get_signature()),
				builder.getInt8PtrTy()),
			llvm_func_name_str,
			llvm_dbg_name_str
		});
	}
}

bool
LlvmCompiler::get_compiled_struct(CompiledStruct& out, const voila::DataStruct& ds) const
{
	auto physical = ds->physical_table.get();
	check_physical_type(ds, physical, "get_compiled_struct");

	auto it = data_structs.find(physical);
	if (it != data_structs.end()) {
		out = it->second;
		return true;
	}
	return false;
}

llvm::Value*
LlvmCompiler::create_expect_mostly(llvm::Value* val, int64_t likely_value,
	double* probability)
{
	auto t = val->getType();
	ASSERT(t->isIntegerTy());
	auto likely_val = builder.getIntN(t->getIntegerBitWidth(), likely_value);

	auto expect = probability ?
		llvm::Intrinsic::getDeclaration(module,
			llvm::Intrinsic::expect_with_probability, { t,
				builder.getDoubleTy() }) :
		llvm::Intrinsic::getDeclaration(module,
			llvm::Intrinsic::expect, { t });

	if (!probability) {
		return builder.CreateCall(expect, { val, likely_val });
	}
	return builder.CreateCall(expect, { val, likely_val,
		llvm::ConstantFP::get(builder.getDoubleTy(), *probability)
	});
}
