#pragma once

#include "plan.hpp"
#include "stream.hpp"
#include "engine/lolepops/lolepop.hpp"

namespace engine::protoplan {
struct PlanOp;
struct PlanOpVisitor;

struct Scan;
struct Select;
struct Output;
struct Project;
struct JoinWrite;
struct JoinBuild;
struct JoinProbeDriver;
struct JoinProbeCheck;
struct JoinProbeGather;
struct GroupByBuild;
struct GroupByPartition;
struct GroupByScan;
}



namespace engine::protoplan {
typedef std::shared_ptr<PlanOp> PlanOpPtr;

struct PlanOp {
	const plan::RelOp* plan_op;
	engine::RelOpPtr rel_op;
	std::shared_ptr<PlanOp> child;
	const std::string name;

	PlanOp(const std::string& name,
		const plan::RelOp* plan_op,
		const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child);

	virtual void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) = 0;

	virtual std::shared_ptr<PlanOp> clone_internal() = 0;

	std::shared_ptr<PlanOp> clone() {
		std::shared_ptr<PlanOp> result(clone_internal());
		result->stable_op_id = stable_op_id;
		return result;
	}

	virtual ~PlanOp() = default;

	size_t stable_op_id = 0;

	size_t get_stable_op_id() const {
		return stable_op_id;
	}
};


struct PlanOpVisitor {
	virtual void visit(Scan&, const PlanOpPtr&) = 0;
	virtual void visit(Select&, const PlanOpPtr&) = 0;
	virtual void visit(Output&, const PlanOpPtr&) = 0;
	virtual void visit(Project&, const PlanOpPtr&) = 0;
	virtual void visit(JoinWrite&, const PlanOpPtr&) = 0;
	virtual void visit(JoinBuild&, const PlanOpPtr&) = 0;
	virtual void visit(JoinProbeDriver&, const PlanOpPtr&) = 0;
	virtual void visit(JoinProbeCheck&, const PlanOpPtr&) = 0;
	virtual void visit(JoinProbeGather&, const PlanOpPtr&) = 0;
	virtual void visit(GroupByBuild&, const PlanOpPtr&) = 0;
	virtual void visit(GroupByPartition&, const PlanOpPtr&) = 0;
	virtual void visit(GroupByScan&, const PlanOpPtr&) = 0;

	virtual void operator()(const PlanOpPtr& ptr) {
		ptr->visit(*this, ptr);
	}
};


struct Scan : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	Scan(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op)
	: PlanOp("Scan", plan_op, rel_op, nullptr)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<Scan>(plan_op, rel_op);
	}
};

struct Select : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	Select(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child)
	: PlanOp("Select", plan_op, rel_op, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<Select>(plan_op, rel_op, child->clone());
	}
};

struct Output : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	Output(std::shared_ptr<PlanOp>&& child)
	: PlanOp("Output", nullptr, nullptr, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<Output>(child->clone());
	}
};

struct Project : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	Project(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child)
	: PlanOp("Project", plan_op, rel_op, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<Project>(plan_op, rel_op, child->clone());
	}
};


struct JoinWrite : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	JoinWrite(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child)
	: PlanOp("JoinWrite", plan_op, rel_op, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<JoinWrite>(plan_op, rel_op, child->clone());
	}
};

struct JoinBuild : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	JoinBuild(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op)
	 : PlanOp("JoinBuild", plan_op, rel_op, nullptr)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<JoinBuild>(plan_op, rel_op);
	}
};

struct JoinProbeDriver : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	JoinProbeDriver(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child, size_t bloom_filter_bits = false)
	 : PlanOp("JoinProbeDriver", plan_op, rel_op, std::move(child)),
		bloom_filter_bits(bloom_filter_bits)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<JoinProbeDriver>(plan_op, rel_op, child->clone(),
			bloom_filter_bits);
	}

	size_t bloom_filter_bits = 0;
};

struct JoinProbeCheck : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	JoinProbeCheck(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child)
	 : PlanOp("JoinProbeCheck", plan_op, rel_op, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<JoinProbeCheck>(plan_op, rel_op, child->clone());
	}
};

struct JoinProbeGather : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	JoinProbeGather(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child)
	 : PlanOp("JoinProbeGather", plan_op, rel_op, std::move(child))
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<JoinProbeGather>(plan_op, rel_op, child->clone());
	}
};

struct GroupByBuild : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	GroupByBuild(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op,
		std::shared_ptr<PlanOp>&& child, bool is_global, bool has_aggregates)
	 : PlanOp("GroupByBuild", plan_op, rel_op, std::move(child)), is_global(is_global),
		has_aggregates(has_aggregates)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<GroupByBuild>(plan_op, rel_op, child->clone(),
			is_global, has_aggregates);
	}

	bool is_global = false;
	bool has_aggregates = true;
};

struct GroupByPartition : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	GroupByPartition(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op)
	 : PlanOp("GroupByPartition", plan_op, rel_op, nullptr)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<GroupByPartition>(plan_op, rel_op);
	}
};

struct GroupByScan : PlanOp {
	void visit(PlanOpVisitor& visitor, const PlanOpPtr& ptr) final {
		visitor.visit(*this, ptr);
	}

	GroupByScan(const plan::RelOp* plan_op, const engine::RelOpPtr& rel_op)
	 : PlanOp("GroupByScan", plan_op, rel_op, nullptr)
	{
	}

	PlanOpPtr clone_internal() final {
		return std::make_shared<GroupByScan>(plan_op, rel_op);
	}
};


} /* engine::protoplan */

struct ProtoplanTranslator : engine::protoplan::PlanOpVisitor {
	using Stream = engine::Stream;
	using PlanOpPtr = engine::protoplan::PlanOpPtr;

	ProtoplanTranslator(Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context);
	void operator()(const PlanOpPtr& op);

	void visit(engine::protoplan::Scan& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::Select& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::Output& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::Project& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::JoinWrite& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::JoinBuild& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::JoinProbeDriver& op,
	 const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::JoinProbeCheck& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::JoinProbeGather& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::GroupByBuild& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::GroupByPartition& op,
		const PlanOpPtr& ptr) final;
	void visit(engine::protoplan::GroupByScan& op,
		const PlanOpPtr& ptr) final;
private:
	Stream& stream;
	std::shared_ptr<memory::Context> mem_context;

	template<typename T>
	void add_stream(const PlanOpPtr op, const T& fun) {
		std::shared_ptr<engine::lolepop::Lolepop> p(fun());
		stream.execution_context.root = p;

		p->plan_op = op;
		ASSERT(op->stable_op_id > 0); // = id_counter++;

		if (stream.profile) {
			update_profiling(p);
		}
	}

	size_t id_counter = 0;

	void update_profiling(const std::shared_ptr<engine::lolepop::Lolepop>& p);
};


struct ExprTranslator : plan::NodeVisitor {
	engine::lolepop::Expr result;

	void visit(const plan::ColumnId& n) override {
		result = create_colref(n.name);
	}
	void visit(const plan::Constant& n) override {
		result = std::make_shared<engine::lolepop::Constant>(n.name);
	}
	void visit(const plan::Assign& n) override {
		ASSERT(n.children.size() == 1);

		result = create_assign(n.name, (*this)(n.children[0]));
	}
	void visit(const plan::Function& n) override {
		result = create_function(n.name, n);
	}

	engine::lolepop::Expr operator()(const plan::ExprPtr& p) {
		result.reset();
		p->visit(*this);
		return std::move(result);
	}

protected:
	engine::lolepop::Expr create_function(const std::string name, const plan::Function& n) {
		// translate arguments
		std::vector<engine::lolepop::Expr> args;
		for (auto& arg : n.children) {
			args.emplace_back((*this)(arg));
		}

		return std::make_shared<engine::lolepop::Function>(name, std::move(args));
	}

	engine::lolepop::Expr create_colref(const std::string name) {
		return std::make_shared<engine::lolepop::ColumnRef>(name);
	}

	engine::lolepop::Expr create_assign(const std::string name,
			const engine::lolepop::Expr& val) {
		return std::make_shared<engine::lolepop::Assign>(
			std::make_shared<engine::lolepop::ColumnRef>(name), val);
	}
};

struct HashAggrRewriteExprTranslator : ExprTranslator {
	void visit(const plan::ColumnId& n) override {
		result = create_colref(get_name(n.name, map_references));
	}
	void visit(const plan::Assign& n) override {
		ASSERT(n.children.size() == 1);

		result = create_assign(get_name(n.name, map_assign),
			(*this)(n.children[0]));
	}
	void visit(const plan::Function& n) override {
		result = create_function(get_name(n.name, map_function), n);
	}

	engine::lolepop::Expr operator()(const plan::ExprPtr& p) {
		result.reset();
		p->visit(*this);
		return std::move(result);
	}

	std::unordered_map<std::string, std::string> map_function;
	std::unordered_map<std::string, std::string> map_references;
	std::unordered_map<std::string, std::string> map_assign;

private:
	static std::string get_name(const std::string& old_name,
			const std::unordered_map<std::string, std::string>& map) {
		auto it = map.find(old_name);
		LOG_DEBUG("FIND: %s", old_name.c_str());
		if (it != map.end()) {
			LOG_WARN("Rewrite %s -> %s", old_name.c_str(), it->second.c_str());
			return it->second;
		}
		return old_name;
	}
};
