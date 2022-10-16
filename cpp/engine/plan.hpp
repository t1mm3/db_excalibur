#pragma once

#include <vector>
#include <memory>
#include <string>

#include "system/system.hpp"

namespace plan {

struct Node;
struct NodeVisitor;

typedef std::shared_ptr<Node> NodePtr;

struct Node {
	virtual ~Node() {
	}

	virtual void visit(NodeVisitor& visitor) = 0;
};


struct Expr;
struct Lolepop;
struct RelOp;
struct NodeVisitor;

typedef std::shared_ptr<Expr> ExprPtr;
typedef std::shared_ptr<RelOp> RelOpPtr;
typedef std::shared_ptr<RelOp> LolepopPtr;

struct Project;
struct Select;
struct ExchangeHashSplit;
struct HashJoin;
struct HashGroupBy;
struct Scan;

struct ColumnId;
struct Constant;
struct Assign;
struct Function;

struct NodeVisitor {
	virtual void visit(const Project&) { fail("Project"); }
	virtual void visit(const Select&) { fail("Select"); }
	virtual void visit(const Scan&) { fail("Scan"); }
	virtual void visit(const ExchangeHashSplit&) { fail("ExchangeHashSplit"); }
	virtual void visit(const HashJoin&) { fail("HashJoin"); }
	virtual void visit(const HashGroupBy&) { fail("HashGroupBy"); }

	virtual void visit(const ColumnId&) { fail("ColumnId"); }
	virtual void visit(const Constant&) { fail("Constant"); }
	virtual void visit(const Assign&) { fail("Assign"); }
	virtual void visit(const Function&) { fail("Function"); }

	virtual void operator()(const NodePtr& n) {
		n->visit(*this);
	} 

protected:
	void fail(const std::string& s) {
		LOG_FATAL("Visiting '%s' is not handled", s.c_str());
	}
};

struct RelOp : Node {

};

struct RelPlan {
	RelOpPtr root;

	RelPlan(const RelOpPtr& root) : root(root) {}
};

struct HashJoin : RelOp {
	RelOpPtr build_op;
	std::vector<ExprPtr> build_keys;
	std::vector<ExprPtr> build_payloads;

	RelOpPtr probe_op;
	std::vector<ExprPtr> probe_keys;
	std::vector<ExprPtr> probe_payloads;

	bool fk1;

	HashJoin(const RelOpPtr& build_rel, const std::vector<ExprPtr>& build_keys,
		const std::vector<ExprPtr>& build_payl,
		const RelOpPtr& probe_rel, const std::vector<ExprPtr>& probe_keys,
		const std::vector<ExprPtr>& probe_payl, bool fk1)
	 : build_op(build_rel), build_keys(build_keys), build_payloads(build_payl),
	 probe_op(probe_rel), probe_keys(probe_keys), probe_payloads(probe_payl), fk1(fk1)
	{
	}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct HashGroupBy : RelOp {
	RelOpPtr child;

	std::vector<ExprPtr> keys;
	std::vector<ExprPtr> payloads;
	std::vector<ExprPtr> aggregates;

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}

	HashGroupBy(const RelOpPtr& child, const std::vector<ExprPtr>& keys,
		const std::vector<ExprPtr>& payloads, const std::vector<ExprPtr>& aggregates)
	 : child(child), keys(keys), payloads(payloads), aggregates(aggregates)
	{

	}
};

struct Select : RelOp {
	RelOpPtr child;
	ExprPtr cond;

	Select(const RelOpPtr& child, const ExprPtr& cond) 
	 : child(child), cond(cond)
	{
	}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Scan : RelOp {
	const std::string table;
	const std::vector<std::string> cols;

	Scan(const std::string& table,
		const std::vector<std::string>& cols)
	 : table(table), cols(cols) {

	}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Project : RelOp {
	RelOpPtr child;
	std::vector<ExprPtr> projs;

	Project(const RelOpPtr& child, const std::vector<ExprPtr>& projs) 
	 : child(child), projs(projs)
	{
	}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Expr : Node {
	const std::string name;
	std::vector<ExprPtr> children; 

	Expr(const std::string& name, const std::vector<ExprPtr>& children) : name(name), children(children) {
	}
};

struct ColumnId : Expr {
	ColumnId(const std::string& name) : Expr(name, {}) {}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Constant : Expr {
	Constant(const std::string& name) : Expr(name, {}) {}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Assign : Expr {
	Assign(const std::string& name, const ExprPtr& val) : Expr(name, { val }) {}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

struct Function : Expr {
	Function(const std::string& name, const std::vector<ExprPtr>& children) : Expr(name, children) {}

	virtual void visit(NodeVisitor& visitor) final {
		visitor.visit(*this);
	}
};

// std::shared_ptr<plan::Node> parse_plan(const std::string& text);

struct Builder {
	auto column(const std::string& col) {
		return std::make_shared<ColumnId>(col);
	}

	auto constant(const std::string& val) {
		return std::make_shared<Constant>(val);
	}

	auto assign(const std::string& col, const ExprPtr& cond) {
		return std::make_shared<Assign>(col, cond);
	}


	auto function(const std::string& name, const std::vector<ExprPtr>& children) {
		return std::make_shared<Function>(name,
			children);
	}

	auto function(const std::string& name, const ExprPtr& child1) {
		return std::make_shared<Function>(name,
			std::vector<ExprPtr> { child1 });
	}

	auto function(const std::string& name, const ExprPtr& child1, const ExprPtr& child2) {
		return std::make_shared<Function>(name,
			std::vector<ExprPtr> { child1, child2 });
	}

	auto function(const std::string& name, const ExprPtr& child1, const ExprPtr& child2,
			const ExprPtr& child3) {
		return std::make_shared<Function>(name,
			std::vector<ExprPtr> { child1, child2, child3 });
	}


	auto scan(const std::string& table, const std::vector<std::string>& columns) {
		return std::make_shared<Scan>(table, columns);
	}

	auto select(const RelOpPtr& child, const ExprPtr& cond) {
		return std::make_shared<Select>(child, cond);
	}

	auto project(const RelOpPtr& child, const std::vector<ExprPtr>& projs) {
		return std::make_shared<Project>(child, projs);
	}

	auto hash_join(const RelOpPtr& build_rel, const std::vector<ExprPtr>& build_keys,
			const std::vector<ExprPtr>& build_payl,
			const RelOpPtr& probe_rel, const std::vector<ExprPtr>& probe_keys,
			const std::vector<ExprPtr>& probe_payl, bool fk1 = false) {
		return std::make_shared<HashJoin>(build_rel, build_keys, build_payl,
			probe_rel, probe_keys, probe_payl, fk1);
	}

	auto hash_join(const RelOpPtr& build_rel, const ExprPtr& build_key,
			const RelOpPtr& probe_rel, const ExprPtr& probe_keys, bool fk1 = false) {
		return hash_join(build_rel,
			std::vector<ExprPtr> { build_key },
			std::vector<ExprPtr> {  },
			probe_rel,
			std::vector<ExprPtr> { probe_keys },
			std::vector<ExprPtr> {  },
			fk1);
	}

	auto hash_group_by(const RelOpPtr& child, const std::vector<ExprPtr>& keys,
			const std::vector<ExprPtr>& payloads, const std::vector<ExprPtr>& aggrs) {
		return std::make_shared<HashGroupBy>(child, keys, payloads, aggrs);
	}

	auto hash_group_by(const RelOpPtr& child, const std::vector<ExprPtr>& keys,
			const std::vector<ExprPtr>& aggrs) {
		return hash_group_by(child, keys, {}, aggrs);
	}

	auto hash_group_by(const RelOpPtr& child, const ExprPtr& key,
			const ExprPtr& aggr) {
		return hash_group_by(child, std::vector<ExprPtr> { key },
			{},
			std::vector<ExprPtr> { aggr });
	}

};

} /* plan */
