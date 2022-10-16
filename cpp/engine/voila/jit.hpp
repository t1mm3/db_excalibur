#pragma once

#include <vector>
#include <stdint.h>
#include <cstring>

#include "voila.hpp"

#include "engine/types.hpp"
#include "engine/xvalue.hpp"

#include "system/system.hpp"
#include "system/memory.hpp"
#include "system/profiling.hpp"

struct IBudgetManager;
struct BudgetUser;

namespace engine {
struct XValue;
struct Stream;
struct PrimitiveProfile;

namespace adaptive {
struct Actions;
}

namespace lolepop {
struct Stride;
struct Lolepop;
struct NextContext;
}

namespace table {
struct ITable;
struct IHashTable;
struct LogicalMasterTable;
}

namespace voila {

struct CodeCache;
struct CodeBlockFuture;
struct CodeBlock;
struct CompileRequest;


struct CPrimArg {
	// read from LLVM (llvm_compiler.cpp)
	struct IO {
		void** base; //!< Pointer to base pointer of vector
		int64_t* num; //!< Pointer to length of vector, in case of Predicate
		int64_t* offset; //!< Pointer to offset, in case of Position
		XValue* xvalue;
	};

	struct Struct {
		uint64_t* mask;
		void** buckets;
		void* structure;

		uint64_t* bf_mask;
		uint64_t** bf_words;
	};

	IO* sources;
	IO* sinks;
	Struct* structs;
	int64_t num_sources;
	int64_t num_sinks;
	int64_t num_structs;

	// #tuples in
	// (profiling info written by JIT primitive)
	int64_t num_in_tuples;

	// #tuples out
	// (profiling info written by JIT primitive)
	int64_t num_out_tuples;

	void** pointer_to_invalid_access_safe_space;
	void** pointer_to_invalid_access_zero_safe_space;

	PrimitiveProfile* profile;
	CodeBlock* code_block;

	bool fail;

	void reset() {
		sources = nullptr;
		sinks = nullptr;

		num_sources = -1;
		num_sinks = -1;

		profile = nullptr;
		code_block = nullptr;

		fail = false;
		pointer_to_invalid_access_safe_space = nullptr;
		pointer_to_invalid_access_zero_safe_space = nullptr;

		num_in_tuples = 0;
		num_out_tuples = 0;
	}
};

struct JitPrimArgExtra {
	virtual ~JitPrimArgExtra() = default;
};

// Wrapper around cPrimArg, su we can use C++/non-POD features
struct JitPrimArg {
	// read from LLVM (llvm_compiler.cpp)
	CPrimArg c_arg;

	int64_t min_full_evaluation_vectorsize = -1;
	bool has_full_evaluation;
	double full_evaluation_bit_score_divisor = -1.0;
	std::shared_ptr<PrimitiveProfile> profile;


	std::unique_ptr<CodeBlockFuture> block_future;

	void* allocated_data;
	size_t allocated_size;
	memory::Context* mem_context;

	std::unique_ptr<JitPrimArgExtra> extra;

	char fake_space[std::max(sizeof(uint64_t), sizeof(uint64_t*))];


	std::vector<std::string> cpp_struct_signatures;
	const std::string dbg_name;

	static JitPrimArg* make(memory::Context* mem, CodeBlock* code_block,
		size_t sources, size_t sinks, size_t structs, const std::string signature,
		const std::string dbg_name, Stream& stream, int64_t voila_func_tag);
	static void free(JitPrimArg* arg);

	void reset();

	JitPrimArg(const std::string& dbg_name);
	~JitPrimArg();
};


static_assert(std::is_pod<CPrimArg>(), "Must be POD");
static_assert(std::is_pod<CPrimArg::IO>(), "Must be POD");

struct CodeBlock {
	struct BlockItem;
	struct Context {
		BlockItem* item;
		int64_t num_codes;
		CodeBlock* block;
		CodeBlock::BlockItem* codes_arr;
	};
	typedef int64_t (*call_t)(CPrimArg* arg, Context* ctx);

	struct BlockExtra {
		std::vector<XValue*> args;


		virtual ~BlockExtra() = default;
	};


	static constexpr int kInterpretUninitialized = 0;
	static constexpr int kInterpretYield = 1;
	static constexpr int kInterpretDone = 2;
	static constexpr int kInterpretEnd = 3;


	// TODO(tim): Investigate variable length codes
	struct BlockItem {
		call_t call = nullptr;
		XValue* result = nullptr;
		union {
			struct {
				XValue* predicate;
				size_t offset;
				bool value;
			} go;

			struct {
				void* message;
				int log_level;
				XValue* predicate;
			} comment;

			struct {
				XValue* predicate;
				XValue* source;
			} copy;

			struct {
				size_t index;
			} get;

			struct {
				XValue* read_pos;
				size_t col_index;
				XValue* predicate;
			} scan_col;

			struct {
				XValue* read_pos;
				BlockExtra* state;
				XValue* predicate;
			} scan_multiple_cols;

			struct {
				size_t vector_size;
				voila::ReadPosition* read_pos;
			} scan_pos;

			
			struct {
				XValue* predicate;
				BlockExtra* args;
			} tuple_emit;

			struct {
				int64_t* source_predicate_num;
				call_t call;
				CPrimArg *prim_arg;
			} compiling_fragment;

			struct {
				XValue* pos;
			} selnum;

			struct {
				XValue* predicate;
				XValue* other;
				XValue* tmp;
			} selunion;

			struct {
				XValue* predicate;
				XValue* values;
				CPrimArg* prof_arg;
			} selcond;

			struct {
				engine::table::ITable* table;
				XValue* predicate;
			} write_pos;

			struct {
				engine::table::IHashTable* table;
				XValue* predicate;
				XValue* indices;
			} bucket_insert;

			struct {
				XValue* predicate;
				size_t vector_size;
				BlockExtra* extra;
			} strequal;

		} data;

		BlockItem() {
			memset(&data, 0, sizeof(data));
		}
	};

	int64_t ip = 0;

	Context context_struct;

	void prepare_interpretation() {
		context_struct.block = this;
		context_struct.num_codes = b.size();
		context_struct.codes_arr = &b[0];
	}


	std::vector<BlockItem> b;

	lolepop::Stride* op_input = nullptr;
	std::unique_ptr<lolepop::Stride> op_output;
	std::unordered_set<BlockExtra*> running_futures;

	lolepop::NextContext* next_context = nullptr;

	size_t vector_size;
	size_t morsel_size;

	int interpret();


	/* polls if compilation done
	 *
	 * @param get If true, also delivers exceptions
	 */
	bool is_compilation_done(bool get);

	void print_byte_code(std::ostream& out);

	// track allocations
	std::vector<BlockExtra*> allocated_codeblocks;
	std::vector<JitPrimArg*> allocated_primargs;
	std::vector<XValue*> allocated_xvalues;

	Stream& stream;
	memory::Context& mem_context;
	adaptive::Actions* adaptive_actions;

	CodeBlock(adaptive::Actions* actions, lolepop::Lolepop& op);
	~CodeBlock();

	std::vector<std::shared_ptr<PrimitiveProfile>> get_prof_data() const;
	void propagate_internal_profiling() const;

	template<typename... Args>
	XValue* newXValue(Args&&... args)
	{
		auto r =  mem_context.newObj<XValue>(mem_context, std::forward<Args>(args)...);
		allocated_xvalues.push_back(r);
		return r;
	}
};

struct GlobalCodeContext {
	typedef XValue* visited_val_t;

	std::unordered_map<void*, visited_val_t> globals;
	memory::Context& mem_context;

	GlobalCodeContext(memory::Context& mem_context);
	~GlobalCodeContext();

	template<typename... Args>
	XValue* newXValue(Args&&... args)
	{
		return mem_context.newObj<XValue>(mem_context, std::forward<Args>(args)...);
	}
};

struct CodeBlockBuilderProfData {
	uint64_t t_prologue = 0;

	uint64_t t_copy_elision = 0;
	uint64_t t_create_xvalue = 0;
	uint64_t t_byte_code = 0;

	uint64_t t_total = 0;
	uint64_t calls = 0;

	void to_string(std::ostream& s) const;
};

struct CodeBlockBuilder {
	CodeBlock& block;
	CodeCache& code_cache;
	Context& voila_context;
	GlobalCodeContext& voila_global_context;
	const std::string dbg_name;
	std::shared_ptr<IBudgetManager> budget_manager;
	size_t operator_id;
	CodeBlockBuilderProfData* prof;
	BudgetUser* budget_user;

public:
	CodeBlockBuilder(CodeCache& code_cache, CodeBlock& block, Context& voila_context,
		GlobalCodeContext& voila_global_context, const std::string& dbg_name,
		const std::shared_ptr<IBudgetManager>& budget_manager, size_t operator_id,
		CodeBlockBuilderProfData* prof, BudgetUser* budget_user);

	void operator()(const std::vector<Stmt>& stmts);

private:
	bool run;
};

} /* voila */
} /* engine */