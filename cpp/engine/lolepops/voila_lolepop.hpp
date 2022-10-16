#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include "lolepop.hpp"
#include "engine/voila/voila.hpp"

namespace engine {

namespace lolepop {
struct Expression;
}

namespace voila {
struct CodeBlock;
} /* voila */

} /* engine */

namespace engine {

struct VoilaCodegenContext {
	// std::unordered_map<std::string, std::shared_ptr<voila::Variable>> global_vars;

	std::vector<int> flow_pos;

	struct Col {
		size_t index;
		voila::Expr expr;
	};
	std::unordered_map<std::string, Col> columns;
};

namespace lolepop {

struct VoilaLolepop : Lolepop {
	// Pointer to JIT code block
	std::unique_ptr<voila::CodeBlock> code_block;

	// Pointer to VOILA code
	std::unique_ptr<voila::Block> voila_block;

	voila::Context& voila_context;


	virtual VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) = 0;
	VoilaCodegenContext do_codegen_produce(VoilaCodegenContext&& ctx);

	NextResult next(NextContext& context) final;
	void restart() final;
	void reconnect_ios() final;

	void update_profiling() override;
	void propagate_internal_profiling() override;

protected:
	VoilaLolepop(const std::string& name, const std::shared_ptr<RelOp>& rel_op,
		Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child, uint64_t flags = 0);
	~VoilaLolepop();

	template<typename T> static T*
	check_relop(const std::shared_ptr<RelOp>& rel_op)
	{
		auto op = dynamic_cast<T*>(rel_op.get());
		

		return op;
	}

	static void check_relop_assert(void* ptr);

private:
	NextResult next_result;
	bool fetch_next;
	bool fetch_exhausted;
	int interpret_result;
};

struct VoilaExprBuilder {
private:
	std::shared_ptr<voila::Expression> input_pred;
	std::shared_ptr<voila::Expression> input_data;
	const VoilaCodegenContext& context;
	voila::Builder& builder;
	voila::Context& voila_context;
	const std::string dbg_name;

	std::unordered_map<std::string, std::shared_ptr<voila::Expression>> resolved_columns;

public:
	struct FunctionContext {
		voila::Expr bucket;
		voila::DataStruct data_structure;
		voila::Col column;
		bool is_reaggregation = false;
		bool is_global_aggr = false;
	};

	VoilaExprBuilder(
		const std::string dbg_name,
		const VoilaCodegenContext& context,
		std::shared_ptr<voila::Expression>& input_pred,
		std::shared_ptr<voila::Expression>& input_data,
		voila::Builder& builder,
		voila::Context& voila_context)
	 : input_pred(input_pred), input_data(input_data), context(context),
	 	builder(builder), voila_context(voila_context), dbg_name(dbg_name) {

	}

	std::shared_ptr<voila::Expression> resolve_column(const std::string& name);

	std::shared_ptr<voila::Expression> resolve_expr(const lolepop::Expr& expr,
		FunctionContext* ctx = nullptr);

	std::shared_ptr<voila::Expression> resolve_expr_as(const std::string& name,
		const lolepop::Expr& expr, FunctionContext* ctx = nullptr)
	{
		auto r = resolve_expr(expr, ctx);

		resolved_columns.insert({name, r});
		return r;
	}

	auto operator()(const lolepop::Expr& expr,
		FunctionContext* ctx = nullptr)
	{
		return resolve_expr(expr, ctx);
	}

	static std::string output_col_name(const lolepop::Expr& e) {
		return output_col_name_ptr(e.get());
	}

	static std::string output_col_name_ptr(lolepop::Expression* e);

	std::shared_ptr<voila::Expression> output_col_expr(const lolepop::Expr& e);
	Type* output_col_type(const lolepop::Expr& e);
};

} /* lolepop */
} /* engine */
