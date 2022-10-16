#include "scan.hpp"
#include "engine/engine.hpp"
#include "engine/voila/voila.hpp"
#include "engine/catalog.hpp"
#include "engine/stream.hpp"
#include "engine/voila/voila.hpp"
#include "system/system.hpp"
#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/catalog.hpp"
#include "engine/storage/scan_interface.hpp"
#include "engine/plan.hpp"

using namespace engine;
using namespace lolepop;

using engine::voila::Builder;

Filter::Filter(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child, const Expr& condition)
 : VoilaLolepop("Filter", rel_op, stream, mem_context, child),
	condition(condition)
{

}

VoilaCodegenContext
Filter::codegen_produce(VoilaCodegenContext&& ctx)
{
	Builder b(*voila_block);

	std::vector<voila::Expr> tuple;

	const auto num_cols = ctx.columns.size();
	tuple.reserve(num_cols);

	auto input_data = b.input();
	auto input_pred = b.input_pred();

	VoilaExprBuilder expr_builder(name, ctx, input_pred, input_data, b, voila_context);

	// compute condition
	auto cond = expr_builder(condition);
	auto output_pred = b.seltrue(input_pred, cond);

	b.emit(output_pred, input_data);

	return ctx;
}

Project::Project(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child, const std::vector<Expr>& projs)
 : VoilaLolepop("Project", rel_op, stream, mem_context, child),
	exprs(projs)
{

}

VoilaCodegenContext
Project::codegen_produce(VoilaCodegenContext&& ctx)
{
	Builder b(*voila_block);

	auto input_data = b.input();
	auto input_pred = b.input_pred();

	std::vector<voila::Expr> tuple;
	tuple.reserve(exprs.size());

	VoilaExprBuilder expr_builder(name, ctx, input_pred, input_data, b, voila_context);

	VoilaCodegenContext new_ctx;

	new_ctx.flow_pos.reserve(exprs.size());
	new_ctx.columns.reserve(exprs.size());

	for (size_t i=0; i<exprs.size(); i++) {
		auto& expr = exprs[i];

		std::shared_ptr<Expression> build_expr;
		std::shared_ptr<voila::Expression> col_expr;

		if (auto assign = dynamic_cast<Assign*>(expr.get())) {
			build_expr = assign->expr;

			col_expr = expr_builder.resolve_expr_as(
				expr_builder.output_col_name_ptr(assign),
				build_expr);
		} else {
			ASSERT(dynamic_cast<ColumnRef*>(expr.get()) && "Must either be ColumnId or Assign");
			build_expr = expr;
			col_expr = expr_builder(build_expr);
		}

		tuple.push_back(col_expr);

		// create flow
		new_ctx.flow_pos.push_back(i);
		new_ctx.columns[VoilaExprBuilder::output_col_name(expr)]
			= VoilaCodegenContext::Col {i, std::move(col_expr)};
	}


	b.emit(input_pred,
		b.tuple(input_pred, tuple));

	return new_ctx;
}



namespace engine {
namespace storage {
struct ScanInterface;
}

namespace lolepop {

struct ScanPosition : voila::ReadPosition, engine::storage::ScanThread {
	std::vector<char*> col_data;

	void set_columnar_data(char** data, size_t num_cols) final {
		ASSERT(num_cols == num_columns);
		for (size_t i=0; i<num_cols; i++) {
			col_data[i] = data[i];
		}
	}

	void next(size_t num) final {
		size_t offset;
		size_t n = engine::storage::ScanThread::next(offset, this, num);
		done = !n;
		num_tuples = n;
	}

	double get_progress() final {
		return engine::storage::ScanThread::get_progress();
	}

	ScanPosition(size_t num_cols,
		const std::shared_ptr<engine::storage::ScanInterface>& scan)
	 : engine::storage::ScanThread(scan)
	{
		col_data.resize(num_cols);
		column_data = &col_data[0];
		num_columns = num_cols;
	}
};	

} /* lolepop */
} /* engine */

Scan::Scan(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context)
 : VoilaLolepop("Scan", rel_op, stream, mem_context, nullptr),
	stream(stream)
{
	check_relop<relop::Scan>(rel_op);
}

Scan::~Scan()
{
}

VoilaCodegenContext
Scan::codegen_produce(VoilaCodegenContext&& ctx)
{
	ASSERT(rel_op);
	auto scan_relop = dynamic_cast<relop::Scan*>(rel_op.get());
	ASSERT(scan_relop && "Must be associated with a Scan");

	const auto& table = scan_relop->table;
	const auto& columns = scan_relop->columns;

	if (!state) {
		ASSERT(stream.execution_context.voila_context);
		auto cat_table = stream.query.catalog.get_table(table);
		ASSERT(cat_table && "Table does not exist");

		LOG_TRACE("Scan %lld columns and %lld rows",
			columns.size(), cat_table->get_num_rows());

		using ScanInterface = engine::storage::ScanInterface;
		{
			std::unique_lock lock(scan_relop->mutex);

			if (!scan_relop->scan_interface) {
				scan_relop->scan_interface = ScanInterface::get(
						ScanInterface::Args {cat_table, columns});
			}
		}

		state = std::make_unique<ScanPosition>(columns.size(),
			scan_relop->scan_interface);

		stream.execution_context.source_read_positions.insert(state.get());
	}

	Builder outer_builder(*voila_block);

	// outer_builder.comment("scan: init");

	auto pos_var = outer_builder.new_var(outer_builder.scan_pos(*state), "pos_var");

	VoilaCodegenContext new_ctx;

	size_t round = 0;

	outer_builder.while_loop([&] (auto& b) {
		// generate predicate
		return b.selnum(b.ref(pos_var, nullptr), b.ref(pos_var, nullptr));
	}, [&] (auto& b, auto& pred) {
		// generate body
		std::vector<voila::Expr> tuple;
		tuple.reserve(columns.size());

		ASSERT(!round && "Should only be run once");
		round++;

		auto pos = b.ref(pos_var, nullptr);
#if 0
		auto output_pred = b.selnum(pred, pos);
#else
		auto output_pred = pred;
#endif
		new_ctx.flow_pos.reserve(columns.size());
		new_ctx.columns.reserve(columns.size());

		for (size_t i=0; i<columns.size(); i++) {
			auto col_expr = b.scan_columnar(output_pred, pos,
					state->get_catalog_column(i), i);
			tuple.push_back(col_expr);

			new_ctx.columns[columns[i]]
				= VoilaCodegenContext::Col {i, std::move(col_expr)};
			new_ctx.flow_pos.push_back(i);
		}

		b.emit(output_pred,
			b.tuple(output_pred, std::move(tuple)));

		// get new pos for next iteration
		b.assign(pred, pos_var, outer_builder.scan_pos(*state));
	}, "scan_loop");

	outer_builder.end_of_flow();

	return new_ctx;
}


double
engine::relop::Scan::get_max_tuples() const
{
	double multiplier = query.config ?
		query.config->table_cardinality_multiplier() : 1.0;

	return scan_interface->get_max_tuples() * multiplier;
}