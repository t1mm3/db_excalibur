#include "protoplan.hpp"
#include "stream.hpp"
#include "profile.hpp"

#include "engine/query.hpp"
#include "engine/lolepops/lolepop.hpp"
#include "engine/lolepops/scan.hpp"
#include "engine/lolepops/hash_join.hpp"
#include "engine/lolepops/hash_group_by.hpp"
#include "engine/lolepops/output.hpp"

using namespace engine::protoplan;


PlanOp::PlanOp(const std::string& name,
	const plan::RelOp* plan_op,
	const engine::RelOpPtr& rel_op,
	std::shared_ptr<PlanOp>&& child)
 : plan_op(plan_op), rel_op(rel_op),
	child(std::move(child)), name(name)
{

}




ProtoplanTranslator::ProtoplanTranslator(Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context)
 : stream(stream), mem_context(mem_context)
{

}

void
ProtoplanTranslator::operator()(const PlanOpPtr& op)
{
	ASSERT(op);

	if (op->child) {
		(*this)(op->child);
	}
	op->visit(*this, op);
}

void
ProtoplanTranslator::visit(engine::protoplan::Scan& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::Scan>(op.rel_op,
			stream, mem_context);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::Select& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		ExprTranslator translate;

		auto select_op = dynamic_cast<const plan::Select*>(op.plan_op);
		ASSERT(select_op);
		return std::make_shared<engine::lolepop::Filter>(op.rel_op,
			stream, mem_context, stream.execution_context.root,
			translate(select_op->cond));
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::Output& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::Output>(stream,
			mem_context, stream.execution_context.root);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::Project& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		std::vector<engine::lolepop::Expr> projs;
		auto project_op = dynamic_cast<const plan::Project*>(op.plan_op);
		ASSERT(project_op);

		projs.reserve(project_op->projs.size());

		for (auto& expr : project_op->projs) {
			ExprTranslator translate;
			projs.emplace_back(translate(expr));
		}

		return std::make_shared<engine::lolepop::Project>(op.rel_op,
			stream, mem_context, stream.execution_context.root, std::move(projs));
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::JoinWrite& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashJoinWrite>(op.rel_op,
			stream, mem_context, stream.execution_context.root);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::JoinBuild& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashJoinBuildTable>(
			op.rel_op, stream, mem_context);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::JoinProbeDriver& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashJoinProbe>(
			op.bloom_filter_bits, op.rel_op, stream, mem_context,
			stream.execution_context.root);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::JoinProbeCheck& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashJoinCheck>(
			op.rel_op, stream, mem_context, stream.execution_context.root);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::JoinProbeGather& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashJoinGather>(
			op.rel_op, stream, mem_context, stream.execution_context.root);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::GroupByBuild& op, const PlanOpPtr& ptr)
{
	if (!op.is_global) {
		add_stream(ptr, [&] () {
			return std::make_shared<engine::lolepop::HashGroupByFindPos>(
				op.rel_op, stream, mem_context, stream.execution_context.root);
		});
		add_stream(ptr, [&] () {
			return std::make_shared<engine::lolepop::HashGroupByCheck>(
				op.rel_op, stream, mem_context, stream.execution_context.root);
		});
	}
	if (op.has_aggregates) {
		add_stream(ptr, [&] () {
			return std::make_shared<engine::lolepop::HashGroupByAggregate>(
				op.rel_op, stream, mem_context, stream.execution_context.root);
		});
	}
}

void
ProtoplanTranslator::visit(engine::protoplan::GroupByPartition& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashGroupByPartition>(
			op.rel_op, stream, mem_context);
	});
}

void
ProtoplanTranslator::visit(engine::protoplan::GroupByScan& op, const PlanOpPtr& ptr)
{
	add_stream(ptr, [&] () {
		return std::make_shared<engine::lolepop::HashGroupByScan>(
			op.rel_op, stream, mem_context);
	});
}

void
ProtoplanTranslator::update_profiling(const std::shared_ptr<engine::lolepop::Lolepop>& p)
{
	if (!stream.profile) {
		return;
	}
	stream.profile->root_lolepop = p->profile;
}