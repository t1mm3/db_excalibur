#pragma once

#include "voila_lolepop.hpp"

namespace engine::storage {
struct ScanInterface;
} /* engine::storage */


namespace engine::lolepop {

struct ScanPosition;

struct Scan : VoilaLolepop {
	std::unique_ptr<ScanPosition> state;
	Stream& stream;

	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;

	Scan(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context);
	~Scan();
};

struct Filter : VoilaLolepop {
	Expr condition;
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;

	Filter(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child,
		const Expr& condition);
};

struct Project : VoilaLolepop {
	std::vector<Expr> exprs;
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;

	Project(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child,
		const std::vector<Expr>& exprs);
};

} /* engine::lolepop */

namespace engine::relop {

struct Scan : RelOp {
	std::shared_ptr<engine::storage::ScanInterface> scan_interface;

	Scan(Query& q, const std::string& table, const std::vector<std::string>& columns)
	 : RelOp(q, nullptr), table(table), columns(columns) {

	}

	std::string table;
	std::vector<std::string> columns;

	double get_max_tuples() const override;
};

struct Filter : RelOp {
	Filter(Query& q, const RelOpPtr& child) : RelOp(q, child) {}

	double get_max_tuples() const override {
		return child->get_max_tuples();
	}
};

struct Project : RelOp {
	Project(Query& q, const RelOpPtr& child) : RelOp(q, child) {}

	double get_max_tuples() const override {
		return child->get_max_tuples();
	}
};

} /* engine::relop */
