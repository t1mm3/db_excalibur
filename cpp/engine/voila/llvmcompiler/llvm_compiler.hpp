#pragma once

#include <memory>
#include <string>

#include "engine/voila/voila.hpp"
#include "engine/voila/code_cache.hpp"
#include "engine/types.hpp"
#include "engine/string_heap.hpp"

#include "system/system.hpp"

#include "llvm_fragment.hpp"

#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Metadata.h"

namespace engine::table {
struct ITable;
}

namespace engine::voila {
struct CompileRequest;
}

namespace engine::voila::llvmcompiler {

struct CompiledValue {
	std::vector<llvm::Value*> value;
};

} /* engine::voila::llvmcompiler */

namespace engine {
namespace voila {
namespace llvmcompiler {
struct PredicationOpts {
	PredicationOpts(FlavorSpec& spec) {

	}

	// Flavor-related stuff:

	bool allow_full_eval = true;


	// Code-related stuff:

	bool can_be_fully_evaluated = false;
	size_t simple = 0;
};

struct LlvmCompiler : voila::VoilaTreePass {
private:
	const std::string func_name;
	const std::string dbg_name;

	LlvmFragment& fragment;
	llvm::LLVMContext& context;
	llvm::Module* module;
	llvm::IRBuilder<> builder;
	CompileRequest& request;

	struct IOInfo {
		llvm::Value* ptr;
		llvm::Value* num;
		llvm::Value* offset;

		llvm::Type* type;

		bool input;
		bool output;
		bool predicate;

		std::string dbg_name;
	};

	std::unordered_map<void*, IOInfo> io_info;
	std::unordered_map<void*, CompiledValue> cache;

	std::unordered_map<void*, CompiledValue> variables;

	llvm::MDNode* meta_noalias_dom = nullptr;
	llvm::MDNode* meta_noalias_scope = nullptr;
	llvm::MDNode* meta_noalias_set = nullptr;

	std::vector<voila::Expression*> in_predicates;

	size_t id_counter = 0;

	llvm::BasicBlock* bb_entry = nullptr;

	void _loop(llvm::Value* var, llvm::Value* num,
		int64_t step, int64_t unroll, int64_t simdization,
		const std::string& name, llvm::Function* func,
		const std::function<void(llvm::Value*, int64_t)> f);

	void loop(llvm::Value* var, llvm::Value* num,
		int64_t unroll, int64_t simdization,
		const std::string& name, llvm::Function* func,
		const std::function<void(llvm::Value*, int64_t)> f);

	void stmt(const voila::Stmt& e);

	// generates vectorized loop
	void generate(llvm::Value* k_ptr, llvm::Value* num, llvm::Function* func,
		const std::string& twine);

	bool is_cached(const voila::Expr& ptr) const {
		return cache.find(ptr.get()) != cache.end();
	}

	CompiledValue get(const voila::Expr& ptr);

	void put(voila::Expression* ptr, const CompiledValue& val);

	void put_simple(voila::Expression* ptr, llvm::Value* val)
	{
		put(ptr, CompiledValue { {val} } );
	}

	engine::Type* get_type(voila::Node* ptr) const;
	engine::Type* get_type(const voila::Expr& ptr) const {
		return get_type(ptr.get());
	}

	void handle(const Expr& s) final;
	void handle(const Stmt& s) final {
		VoilaTreePass::handle(s);
	}

	void on_expression(const std::shared_ptr<Function>& e) final;
	void on_expression(const std::shared_ptr<Input>& e) final;
	void on_expression(const std::shared_ptr<InputPredicate>& e) final;
	void on_expression(const std::shared_ptr<GetScanPos>& e) final;
	void on_expression(const std::shared_ptr<Scan>& e) final;

	void on_expression(const std::shared_ptr<ReadVariable>& e) final;
	void on_expression(const std::shared_ptr<Constant>& e) final;
	void on_expression(const std::shared_ptr<MetaExpression>& b) final;

	void on_statement(const std::shared_ptr<Assign>& b) final;
	void on_statement(const std::shared_ptr<Block>& b) final;
	void on_statement(const std::shared_ptr<Emit>& b) final;
	void on_statement(const std::shared_ptr<Effect>& b) final;
	void on_statement(const std::shared_ptr<Loop>& b) final;
	void on_statement(const std::shared_ptr<EndOfFlow>& b) final;
	void on_statement(const std::shared_ptr<Comment>& b) final;
	void on_statement(const std::shared_ptr<MetaStatement>& b) final;
	
	void install_functions();

	llvm::Function* llvm_compiler_begin_prim = nullptr;
	llvm::Function* llvm_compiler_check_prim = nullptr;
	llvm::Function* llvm_compiler_check_xvalue = nullptr;
	llvm::Function* llvm_compiler_check_struct = nullptr;
	llvm::Function* llvm_compiler_end_prim = nullptr;
	llvm::Function* llvm_compiler_comment = nullptr;
	llvm::Function* llvm_compiler_comment_ptr = nullptr;
	llvm::Function* llvm_compiler_comment_ptr_nonull = nullptr;
	llvm::Function* llvm_compiler_comment_int = nullptr;
	llvm::Function* llvm_compiler_print_hash_table = nullptr;
	llvm::Function* llvm_compiler_strcmp = nullptr;
	llvm::Function* llvm_compiler_streq = nullptr;
	llvm::Function* llvm_compiler_strncmp = nullptr;
	llvm::Function* llvm_compiler_strhash = nullptr;
	llvm::Function* llvm_compiler_strcontains = nullptr;
	llvm::Function* llvm_compiler_memmem = nullptr;
	llvm::Function* llvm_compiler_extract_year = nullptr;
	llvm::Function* llvm_compiler_htappend1 = nullptr;
	llvm::Function* llvm_compiler_writepos1 = nullptr;
	llvm::Function* llvm_compiler_alloc_write_pos_chunk = nullptr;
	llvm::Function* llvm_compiler_alloc_bucket_insert = nullptr;
	llvm::Function* llvm_compiler_alloc_bucket_insert_dbg_update = nullptr;
	llvm::Function* llvm_compiler_can_full_eval = nullptr;

	llvm::Function* get_llvm_compiler_print_hash_table();

	llvm::Function* get_llvm_compiler_comment();
	llvm::Function* get_llvm_compiler_comment_ptr();
	llvm::Function* get_llvm_compiler_comment_ptr_nonull();
	llvm::Function* get_llvm_compiler_comment_int();

	llvm::Function* get_llvm_compiler_strcmp();
	llvm::Function* get_llvm_compiler_streq();
	llvm::Function* get_llvm_compiler_strncmp();
	llvm::Function* get_llvm_compiler_strhash();
	llvm::Function* get_llvm_compiler_strcontains();
	llvm::Function* get_llvm_compiler_memmem();
	llvm::Function* get_llvm_compiler_extract_year();
	llvm::Function* get_llvm_compiler_htappend1();
	llvm::Function* get_llvm_compiler_writepos1();
	llvm::Function* get_llvm_compiler_alloc_write_pos_chunk();
	llvm::Function* get_llvm_compiler_alloc_bucket_insert();
	llvm::Function* get_llvm_compiler_alloc_bucket_insert_dbg_update();
	llvm::Function* get_llvm_compiler_can_full_eval();

	void _fun_add_properties(llvm::Function& f, bool readonly = true) const;
	llvm::Function* _fun_props(llvm::Function* f, bool readonly = true) const {
		_fun_add_properties(*f, readonly);
		return f;
	}

	llvm::Type* prim_arg_io_type = nullptr;
	llvm::Type* prim_arg_io_ptr_type = nullptr;
	llvm::Type* prim_arg_struct_type = nullptr;
	llvm::Type* prim_arg_struct_ptr_type = nullptr;
	llvm::Type* prim_arg_type = nullptr;
	llvm::Type* prim_arg_ptr_type = nullptr;
	llvm::Type* str_type = nullptr;
	llvm::FunctionType* prim_func_type = nullptr;

	llvm::Value* loop_index = nullptr;

	llvm::Value* func_arg = nullptr;

	llvm::Value* stream_access_safe_space = nullptr;
	llvm::Value* stream_access_safe_space_zero = nullptr;

	llvm::BasicBlock* bb_epilogue = nullptr;

	bool can_full_eval = false;

	llvm::Function* llvm_function = nullptr;

	// kWrite needs extra offsets that we need to increment
	std::unordered_map<void*, llvm::Value*> vars_to_increment_post_loop_body;

	bool enable_comment = false;
	const bool enable_noalias = true;

	template<typename T> T*
	set_align(T* v, size_t align)
	{
		auto node = llvm::MDNode::get(context,
			llvm::ConstantAsMetadata::get(
				llvm::ConstantInt::get(context,
					llvm::APInt(64, align, true))));

		v->setMetadata("align", node);
		return v;
	}


	llvm::Instruction*
	CreateAlignedLoad(llvm::Value* value, size_t align,
		const std::string& twine = "")
	{
		return builder.CreateAlignedLoad(value, align, twine);
	}

	llvm::Instruction*
	CreateAlignedStore(llvm::Value* value, llvm::Value* ptr, size_t align,
		const std::string& twine = "")
	{
		return builder.CreateAlignedStore(value, ptr, align, false);
	}

	llvm::Instruction*
	set_deref(llvm::Instruction* v, size_t bytes = 64)
	{
		auto deref_bytes = llvm::MDNode::get(context,
			llvm::ConstantAsMetadata::get(
				llvm::ConstantInt::get(context,
					llvm::APInt(64, bytes, true))));

		v->setMetadata("dereferenceable", deref_bytes);
		return v;
	}

	llvm::Instruction*
	set_noalias_scope(llvm::Instruction* v)
	{
		if (v && enable_noalias) {
			v->setMetadata("alias.scope", meta_noalias_set);
			v->setMetadata("noalias", meta_noalias_set);
			// alias.scope
		}
		return v;
	}

	llvm::Instruction*
	set_noalias(llvm::Instruction* v)
	{
		if (v && enable_noalias) {
			v->setMetadata("alias.scope", meta_noalias_set);
			v->setMetadata("noalias", meta_noalias_set);
		}
		return v; // set_noalias_scope(v);
	}

	template<typename T>
	void comment(T& builder, const std::string& m, bool force = false)
	{
		if (!enable_comment && !force) {
			return;
		}
		builder.CreateCall(get_llvm_compiler_comment(), {
			func_arg,
			builder.CreatePointerCast(
				builder.CreateGlobalString(m),
				builder.getInt8PtrTy())
		});
	}

	template<typename T>
	void comment_msg(T& builder, const std::string& m, llvm::Value* val, bool force = false)
	{
#if 1
		if (!enable_comment && !force) {
			return;
		}
#endif
		if (!val) {
			builder.CreateCall(get_llvm_compiler_comment(), {
				func_arg,
				builder.CreatePointerCast(
					builder.CreateGlobalString(m),
					builder.getInt8PtrTy())
			});
			return;
		}

		auto t = val->getType();
		if (auto int_type = llvm::dyn_cast<llvm::IntegerType>(t)) {
			comment_int(builder, m, val, true);
			return;
		}
		if (auto ptr_type = llvm::dyn_cast<llvm::PointerType>(t)) {
			comment_ptr(builder, m, val, true);
			return;
		}
		ASSERT(false);
	}

	template<typename T>
	void comment_ptr(T& builder, const std::string& m, llvm::Value* p, bool can_be_null = true,
		bool force = false)
	{
		if (!enable_comment && !force) {
			return;
		}
		builder.CreateCall(
			can_be_null ? get_llvm_compiler_comment_ptr() : get_llvm_compiler_comment_ptr_nonull(), {
			func_arg,
			builder.CreatePointerCast(builder.CreateGlobalString(m),
				builder.getInt8PtrTy()),
			builder.CreatePointerCast(p, builder.getInt8PtrTy())
		});
	}

	template<typename T>
	void comment_int(T& builder, const std::string& m, llvm::Value* p,
		bool force = false)
	{
		if (!enable_comment && !force) {
			return;
		}
		builder.CreateCall(get_llvm_compiler_comment_int(), {
			func_arg,
			builder.CreatePointerCast(builder.CreateGlobalString(m),
				builder.getInt8PtrTy()),
			builder.CreateZExtOrTrunc(p, builder.getInt64Ty())
		});
	}

	void if_then_else(llvm::Value* val, const std::string& dbg_name,
		const std::function<void(bool branch)>& fun, bool has_else = true);

	template<typename B, typename T>
	void aggregate(B& builder, const Function& fun, llvm::Value* col_ptr,
		const T& value)
	{
		llvm::Value* old_val = set_noalias_scope(builder.CreateLoad(col_ptr));
		auto type = old_val->getType();

		llvm::Value* new_val = nullptr;
		llvm::Value* tmp_val = nullptr;

		switch (fun.tag) {
		case Function::Tag::kBucketAggrSum:
		case Function::Tag::kGlobalAggrSum:
			tmp_val = builder.CreateSExtOrTrunc(value(), type);
			new_val = builder.CreateAdd(old_val, tmp_val, "", false, true);
			break;
		case Function::Tag::kBucketAggrCount:
		case Function::Tag::kGlobalAggrCount:
			tmp_val = builder.CreateSExtOrTrunc(builder.getInt8(1), type);
			new_val = builder.CreateAdd(old_val, tmp_val, "", false, true);
			break;
		case Function::Tag::kBucketAggrMin:
		case Function::Tag::kGlobalAggrMin:
			tmp_val = builder.CreateSExtOrTrunc(value(), type);
			new_val = builder.CreateMinimum(old_val, tmp_val);
			break;
		case Function::Tag::kBucketAggrMax:
		case Function::Tag::kGlobalAggrMax:
			tmp_val = builder.CreateSExtOrTrunc(value(), type);
			new_val = builder.CreateMaximum(old_val, tmp_val);
			break;
		default:
			ASSERT(false);
			break;
		}
		set_noalias(builder.CreateStore(new_val, col_ptr));
	}

	template<typename T>
	static llvm::Type*
	get_heap_length_t(T& builder)
	{
		static_assert(sizeof(uint32_t) == sizeof(StringHeap::length_t), "Expect int32");
		return builder.getInt32Ty();
	}

	template<typename T>
	llvm::Value*
	heap_strlen(T& builder, llvm::Value* str)
	{
		auto result = builder.CreatePointerCast(str,
			llvm::PointerType::getUnqual(get_heap_length_t(builder)));

		result = builder.CreateGEP(result, builder.getInt64(-3));
		return set_noalias_scope(builder.CreateLoad(result));
	}

	template<typename T>
	llvm::Value*
	heap_strhash(T& builder, llvm::Value* str)
	{
		auto result = builder.CreatePointerCast(str,
			llvm::PointerType::getUnqual(builder.getInt64Ty()));

		result = builder.CreateGEP(result, builder.getInt64(-1));
		return set_noalias_scope(builder.CreateLoad(result));
	}

	template<typename T>
	llvm::Value* generate_strcmp(T& builder, llvm::Value* a, llvm::Value* b,
		llvm::Value* n, bool equality_only = false)
	{
		auto result_type = builder.getInt32Ty();
		auto create = [&] (auto a, auto b, auto n) {
			if (n) {
				return builder.CreateCall(get_llvm_compiler_strncmp(), {a, b, n});
			}
			return builder.CreateCall(get_llvm_compiler_strcmp(), {a, b});
		};

		// return create(a,b,n); // TODO: removeme

		if (equality_only) {
#if 1
			/* Strings are prefixed with 64-bit hash and aligned to 32-bit
			   We check the first 32-bit of the string and the last 32-bit of hash
			   togther as one 64-bit integer. */
			auto ptr8_t = llvm::PointerType::getUnqual(builder.getInt8Ty());
			auto ptr32_t = llvm::PointerType::getUnqual(builder.getInt32Ty());
			auto ptr64_t = llvm::PointerType::getUnqual(builder.getInt64Ty());

			auto org_a = a;
			auto org_b = b;

			auto ca = builder.CreatePointerCast(a, ptr32_t);
			auto cb = builder.CreatePointerCast(b, ptr32_t);

			auto va = builder.CreateGEP(ca, builder.getInt64(-1));
			auto vb = builder.CreateGEP(cb, builder.getInt64(-1));

			va = builder.CreatePointerCast(a, ptr64_t);
			vb = builder.CreatePointerCast(b, ptr64_t);

			auto neq = builder.CreateICmpNE(builder.CreateLoad(va), builder.CreateLoad(vb));

			auto bb_previous = builder.GetInsertBlock();
			auto bb_precall = llvm::BasicBlock::Create(context, "strcmp_precall",
				llvm_function);
			auto bb_call = llvm::BasicBlock::Create(context, "strcmp_call",
				llvm_function);
			auto bb_post = llvm::BasicBlock::Create(context, "strcmp_post",
				llvm_function);

			builder.CreateCondBr(neq, bb_post, bb_precall);



			builder.SetInsertPoint(bb_precall);
			auto a_len = heap_strlen(builder, org_a);
			auto b_len = heap_strlen(builder, org_b);
			builder.CreateCondBr(builder.CreateICmpNE(a_len, b_len),
				bb_post, bb_call);




			builder.SetInsertPoint(bb_call);
			a = builder.CreateGEP(ca, builder.getInt64(1));
			b = builder.CreateGEP(cb, builder.getInt64(1));

			a = builder.CreatePointerCast(a, ptr8_t);
			b = builder.CreatePointerCast(b, ptr8_t);

#if 1
			auto call_val = builder.CreateCall(get_llvm_compiler_streq(), {
				a, b, builder.CreateSub(a_len, builder.getInt32(4))
			});
#else
			auto call_val = create(a, b, n);
#endif
			builder.CreateBr(bb_post);

			builder.SetInsertPoint(bb_post);

			auto phi = builder.CreatePHI(result_type, 3, "strcmp");
			phi->addIncoming(builder.getInt32(1), bb_previous);
			phi->addIncoming(builder.getInt32(1), bb_precall);
			phi->addIncoming(call_val, bb_call);

			return phi;
#else
			a = builder.CreateGEP(a, builder.getInt64(-4));
			b = builder.CreateGEP(b, builder.getInt64(-4));
			return builder.CreateCall(get_llvm_compiler_streq(), {
				a, builder.CreateAdd(heap_strlen(builder, a), builder.getInt32(4)),
				b, builder.CreateAdd(heap_strlen(builder, b), builder.getInt32(4))
			});
#endif
		}

		auto va = builder.CreateLoad(a);
		auto vb = builder.CreateLoad(b);

		/*
		#define STRCMP(a, b)  (*(a) != *(b) ? \
			  (int) ((unsigned char) *(a) - \
			         (unsigned char) *(b)) : \
			  strcmp((a), (b)))
		*/


		auto neq = builder.CreateICmpNE(va, vb);

		auto bb_call = llvm::BasicBlock::Create(context, "strcmp_call",
			llvm_function);
		auto bb_quick_diff = llvm::BasicBlock::Create(context, "strcmp_quick_diff",
			llvm_function);
		auto bb_post = llvm::BasicBlock::Create(context, "strcmp_post",
			llvm_function);

		builder.CreateCondBr(neq, bb_quick_diff, bb_call);


		builder.SetInsertPoint(bb_call);
		auto call_val = create(a, b, n);
		builder.CreateBr(bb_post);


		builder.SetInsertPoint(bb_quick_diff);
		auto quick_diff_val = builder.CreateZExtOrBitCast(
			builder.CreateSub(va, vb),
			builder.getInt32Ty());
		builder.CreateBr(bb_post);


		builder.SetInsertPoint(bb_post);
		auto phi = builder.CreatePHI(result_type, 2, "strcmp");
		phi->addIncoming(quick_diff_val, bb_quick_diff);
		phi->addIncoming(call_val, bb_call);
		return phi;
	}

	llvm::Value* CreateComparison(voila::Function::Tag tag, llvm::Value* a, llvm::Value* b,
	Type* a_type, Type* b_type);

	llvm::Value* llvm_func_name_str = nullptr;
	llvm::Value* llvm_dbg_name_str = nullptr;


	struct CompiledStruct {
		llvm::Type* row_type = nullptr;

		llvm::Value* mask = nullptr;
		llvm::Value* buckets = nullptr;

		llvm::Value* structure = nullptr;

		llvm::Value* pbuckets = nullptr;
		llvm::Value* pmask = nullptr;

		size_t row_width = 0;

		llvm::Value* bf_words = nullptr;
		llvm::Value* bf_mask = nullptr;
	};

	std::unordered_map<engine::table::ITable*, CompiledStruct> data_structs;

	void compile_struct(const voila::DataStruct& ds, size_t idx);
	bool get_compiled_struct(CompiledStruct& out, const voila::DataStruct& ds) const;


	bool is_extended() const { return true; }

	void store_in_sink(voila::Node* p, const voila::Expr& expr, bool allow_predicate = true);

	bool is_in_predicate(voila::Expression* predicate) const {
		for (auto& p : in_predicates) {
			if (predicate == p) {
				return true;
			}
		}
		return false;
	}

	llvm::Value* translate_predicate(const voila::Expr& predicate);

	struct PredGenArgs {
		llvm::Value* compiled_predicate = nullptr;
		enum Predication {
			kNone,
			kSelect,
			kMultiply
		};

		Predication predicated = Predication::kNone;
	};

	void predicated(const std::function<void()>& f,
		const voila::Expr& predicate,
		const std::string& dbg_name,
		const PredicationOpts& opts);
	llvm::Value* predicated_expr(
		const std::function<llvm::Value*(llvm::Type*& result_type, const PredGenArgs& args)>& generator,
		const voila::Expr& predicate,
		const std::string& dbg_name,
		const PredicationOpts& opts);

	llvm::Value* create_predicated(llvm::Value* compiled_predicate,
		const std::function<llvm::Value*(llvm::Type*& result_type, const PredGenArgs& args)>& generate,
		const voila::Expr& voila_predicate,
		const std::string& prefix,
		const PredicationOpts& opts);

	llvm::Value* get_undef(llvm::Type* t);

	llvm::Value* create_expect_mostly(llvm::Value* val, int64_t likely_value,
		double* probability = nullptr);




	struct AllocaVarKey {
		void* ptr;
		int name;

		bool operator==(const AllocaVarKey& o) const {
			return ptr == o.ptr && name == o.name;
		}

		size_t hash() const {
			using std::size_t;

			return ((size_t)ptr) ^ std::hash<int>()(name);
		}
	};

	struct AllocaVarKeyHasher {
		size_t operator()(const AllocaVarKey& a) const { return a.hash(); }
	};


	std::unordered_map<AllocaVarKey, llvm::Value*,
		AllocaVarKeyHasher> alloca_variables;


	template<typename T>
	llvm::Value* create_alloca_var(llvm::Type* type, const std::string& dbg_name,
		const T& ctor, const AllocaVarKey* unique_key = nullptr)
	{
		if (unique_key) {
			auto it = alloca_variables.find(*unique_key);
			if (it != alloca_variables.end()) {
				return it->second;
			}
		}

		// add new local variable to 'entry' but before terminator
		auto old_block = builder.GetInsertBlock();
		ASSERT(bb_entry && !bb_entry->empty());
		builder.SetInsertPoint(&bb_entry->back());

		auto var = builder.CreateAlloca(type, nullptr, dbg_name);
		ctor(builder, var);

		builder.SetInsertPoint(old_block);

		auto result = set_noalias_scope(var);
		if (unique_key) {
			alloca_variables[*unique_key] = result;
		}
		return result;
	}

	template<typename T>
	llvm::Value* create_alloca_var(llvm::Type* type, const std::string& dbg_name,
		const T& ctor, const AllocaVarKey& unique_key)
	{
		return create_alloca_var<T>(type, dbg_name, ctor, &unique_key);
	}

	llvm::Value* create_alloca_var(llvm::Type* type, const std::string& dbg_name,
		const AllocaVarKey* unique_key = nullptr)
	{
		return create_alloca_var(type, dbg_name, [] (auto& _1, auto& _2) {},
			nullptr);
	}

	llvm::Value* create_alloca_var(llvm::Type* type, const std::string& dbg_name,
		const AllocaVarKey& unique_key)
	{
		return create_alloca_var(type, dbg_name, &unique_key);
	}

public:
	LlvmCompiler(const std::string& func_name,
		LlvmFragment& fragment, CompileRequest& req,
		const std::string& dbg_name);

	virtual void operator()();

	virtual ~LlvmCompiler() = default;
};

} /* llvmcompiler */
} /* voila */
} /* engine */


