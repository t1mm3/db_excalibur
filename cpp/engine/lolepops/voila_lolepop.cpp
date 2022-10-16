#include "voila_lolepop.hpp"

#include "system/system.hpp"

#include "engine/stream.hpp"
#include "engine/profile.hpp"

#include "engine/voila/voila.hpp"
#include "engine/voila/jit.hpp"

#include <sstream>

using namespace engine;
using namespace lolepop;
using CodeBlock = voila::CodeBlock;

VoilaLolepop::VoilaLolepop(const std::string& name,
	const std::shared_ptr<RelOp>& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context,
	const std::shared_ptr<Lolepop>& child, uint64_t flags)
 : Lolepop(name, rel_op, stream, mem_context, child, flags),
	voila_context(*stream.execution_context.voila_context)
{
	voila_block = std::make_unique<voila::Block>(nullptr);

	ASSERT(stream.execution_context.voila_context);
	restart();
}

VoilaLolepop::~VoilaLolepop()
{

}

void
VoilaLolepop::restart()
{
	interpret_result = CodeBlock::kInterpretUninitialized;
	next_result = NextResult::kUndefined;
	fetch_next = true;
	fetch_exhausted = false;
}

void
VoilaLolepop::reconnect_ios()
{
	if (is_dummy_op) {
		return;
	}
	code_block->op_input = child ? child->output : nullptr;
	output = code_block->op_output.get();
}

NextResult
VoilaLolepop::next(NextContext& context)
{
	if (UNLIKELY(next_result == NextResult::kEndOfStream)) {
		return next_result;
	}

	if (UNLIKELY(!code_block)) {
		prepare();
	}

	while (1) {
		LOG_TRACE("%s: old_next_result %d, fetch_next %d, fetch_exhausted %d, interpret_result %d",
			name.c_str(),
			next_result,
			fetch_next,
			fetch_exhausted,
			interpret_result);

		if (fetch_next) {
			fetch_exhausted = !fetch_from_child(context);
		}

		if (interpret_result == CodeBlock::kInterpretEnd && fetch_exhausted) {
			LOG_DEBUG("%s: END_EOS", name.c_str());
			next_result = NextResult::kEndOfStream;
			break;
		}

		LOG_DEBUG("Interpret operator '%s'", name.c_str());
		code_block->next_context = &context;
		interpret_result = code_block->interpret();
		if (LIKELY(interpret_result == CodeBlock::kInterpretYield)) {
			next_result = NextResult::kYield;
			LOG_TRACE("%s: YIELD", name.c_str());
			fetch_next = false;
			break;
		} else if (interpret_result == CodeBlock::kInterpretEnd) {
			LOG_TRACE("%s: END", name.c_str());
			// end of bytecode reached, get more and re-try
			fetch_next = true;
		} else if (interpret_result == CodeBlock::kInterpretDone) {
			next_result = NextResult::kEndOfStream;
			LOG_TRACE("%s: EOS", name.c_str());
			break;
		} else {
			ASSERT(false);
		}
	}

	ASSERT(next_result != NextResult::kUndefined);
	return next_result;
}

void
VoilaLolepop::check_relop_assert(void* ptr) {
	ASSERT(ptr && "Unexpected rel_op");	
}

void
VoilaLolepop::update_profiling()
{
	Lolepop::update_profiling();

	if (profile && code_block) {
		profile->primitives = code_block->get_prof_data();

#if 0
		std::ostringstream ss;

		code_block->print_byte_code(ss);
		profile->byte_code = profiling::JsonUtils::escape(ss.str());
#endif
	}
}

void
VoilaLolepop::propagate_internal_profiling()
{
	if (!code_block) {
		return;
	}

	code_block->propagate_internal_profiling();
}

VoilaCodegenContext
VoilaLolepop::do_codegen_produce(VoilaCodegenContext&& ctx)
{
	VoilaCodegenContext result(codegen_produce(std::move(ctx)));
	output_column_names.clear();

	const size_t flow_size = result.flow_pos.size();
	output_column_names.resize(flow_size);

	for (auto col_kv : result.columns) {
		const auto& col_name = col_kv.first;
		const auto& index = col_kv.second.index;

		ASSERT(index < flow_size);

		output_column_names[index] = col_name;
	}
	return result;
}





std::shared_ptr<voila::Expression>
VoilaExprBuilder::resolve_column(const std::string& name)
{
	auto cached = resolved_columns.find(name);
	if (cached != resolved_columns.end()) {
		return cached->second;
	}

	auto it = context.columns.find(name);
	if (it == context.columns.end()) {
		LOG_ERROR("Cannot find column %s in %s",
			name.c_str(), dbg_name.c_str());
		ASSERT(it != context.columns.end());
		return nullptr;
	}

	auto resolved = builder.get(input_pred, input_data, it->second.index);
	resolved_columns.insert({name, resolved});
	return resolved;
}

struct FunctionResolver {
	typedef voila::Expr (*call_t)(voila::Builder&, const voila::Expr&,
		const std::vector<voila::Expr>&&, VoilaExprBuilder::FunctionContext* ctx);

	FunctionResolver() {
#define F(NAME, TAG) map.insert({ std::string(NAME), \
		[] (voila::Builder& b, const voila::Expr& pred, \
				const std::vector<voila::Expr>&& args, \
				VoilaExprBuilder::FunctionContext* ctx) { \
			return b.func(voila::Function::Tag::k##TAG, \
				pred, std::move(args)); \
		}});

		F("=", CmpEq)
		F("<", CmpLt)
		F(">", CmpGt)
		F("!=", CmpNe)
		F("<=", CmpLe)
		F(">=", CmpGe)
		F("+", AritAdd)
		F("-", AritSub)
		F("*", AritMul)
		F("extract_year", ExtractYear)
		F("&&", LogAnd)
		F("||", LogOr)
		F("&", LogAnd)
		F("|", LogOr)
		F("!", LogNot)
		F("contains", Contains)
		F("ifthenelse", IfThenElse)
		F("between", BetweenBothIncl)
		F("between<", BetweenUpperExcl)
		F("between>", BetweenLowerExcl)
		F("between<>", BetweenBothExcl)

#undef F

#define AGG(NAME, HASH_TAG, GLOBAL_TAG, IS_COUNT) \
		map.insert({ std::string(NAME), \
		[] (voila::Builder& b, const voila::Expr& pred, \
				const std::vector<voila::Expr>&& args, \
				VoilaExprBuilder::FunctionContext* ctx) -> voila::Expr { \
			ASSERT(ctx && "Must have context to lookup bucket, data_structure etc."); \
			const bool global_aggr = ctx->is_global_aggr; \
			const voila::Function::Tag tag = global_aggr ? \
					voila::Function::Tag::k##GLOBAL_TAG : \
					voila::Function::Tag::k##HASH_TAG; \
			ASSERT(ctx->data_structure && ctx->bucket && ctx->column); \
			auto val = IS_COUNT ? nullptr : args[0]; \
			b.bucket_aggr(tag, pred, ctx->data_structure, ctx->bucket, \
				ctx->column, val, ctx->is_reaggregation); \
			return nullptr; \
		}});

		AGG("sum", BucketAggrSum, GlobalAggrSum, false)
		AGG("min", BucketAggrMin, GlobalAggrMin, false)
		AGG("max", BucketAggrMax, GlobalAggrMax, false)
		AGG("count", BucketAggrCount, GlobalAggrCount, true)
#undef AGG

	}

	call_t get(const std::string& name) const {
		auto it = map.find(name);
		if (it == map.end()) {
			return nullptr;
		}
		return it->second;
	}

private:
	std::unordered_map<std::string, call_t> map;
};

static FunctionResolver g_resolver;

std::shared_ptr<voila::Expression>
VoilaExprBuilder::resolve_expr(const Expr& expr_sp, FunctionContext* ctx)
{
	auto expr = expr_sp.get();
	if (auto column_ref = dynamic_cast<ColumnRef*>(expr)) {
		return resolve_column(column_ref->column_name);
	} else if (auto function = dynamic_cast<Function*>(expr)) {
		std::vector<voila::Expr> args;
		args.reserve(function->args.size());

		for (auto& arg : function->args) {
			args.emplace_back(resolve_expr(arg));
		}

		auto call = g_resolver.get(function->name);
		if (!call) {
			LOG_ERROR("VoilaExprBuilder::resolve_expr: Cannot find function '%s'",
				function->name.c_str());
		}
		ASSERT(call && "Must exist");

		return call(builder, input_pred, std::move(args), ctx);
	} else if (auto constant = dynamic_cast<Constant*>(expr)) {
		return builder.constant(constant->value);
	} else {
		ASSERT(false && "Invalid expression");
		return nullptr;
	}
}

std::string
VoilaExprBuilder::output_col_name_ptr(Expression* expr)
{
	if (auto column_ref = dynamic_cast<ColumnRef*>(expr)) {
		return column_ref->column_name;
	} else if (auto assign = dynamic_cast<Assign*>(expr)) {
		auto ref = dynamic_cast<ColumnRef*>(assign->ref.get());
		ASSERT(ref && "Assign ColumnRef is not a ColumnRef");
		return ref->column_name;
	} else {
		ASSERT(false && "No ColumnRef or Assign");
		return nullptr;
	}
}

std::shared_ptr<voila::Expression>
VoilaExprBuilder::output_col_expr(const std::shared_ptr<Expression>& e)
{
	auto expr = e.get();
	if (auto column_ref = dynamic_cast<ColumnRef*>(expr)) {
		auto it = context.columns.find(column_ref->column_name);
		ASSERT(it != context.columns.end());

		auto& expr = it->second.expr;
		ASSERT(expr.get());

		return expr;
	} else {
		ASSERT(false && "No ColumnRef");
		return nullptr;
	}
}

template<typename T>
static Type*
get_type(T& context, voila::Expression* e)
{
	auto it = context.infos.find(e);
	ASSERT(it != context.infos.end());

	const auto& infos = it->second;
	ASSERT(infos.size() == 1);

	auto type = infos[0].type;
	ASSERT(type);
	return type;
}

Type*
VoilaExprBuilder::output_col_type(const std::shared_ptr<Expression>& e)
{
	auto expr = output_col_expr(e);
	ASSERT(expr);

	return get_type(voila_context, expr.get());
}