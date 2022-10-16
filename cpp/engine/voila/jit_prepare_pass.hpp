#pragma once

#include "voila.hpp"

#include <unordered_set>
#include <unordered_map>
#include <random>
#include <deque>

namespace excalibur {
struct Context;
}

struct IBudgetManager;
struct QueryConfig;

namespace engine::voila {
struct CompileRequest;
struct StatementRange;
struct FlavorSpec;
}

namespace engine::voila {

struct JitPrepareExpressionGraphPass : VoilaGraphTreePass {
	struct Edge {
		Node* source;
		Node* dest;

		Edge(Node* from, Node* to) : source(from), dest(to) {
		}

		size_t hash() const {
			return (size_t)(source) ^ (size_t)(dest);
		}

		bool operator==(const Edge& a) const {
			return a.source == source && a.dest == dest;
		}
	};

	struct HashEdge {
		size_t operator()(const Edge& e) const {
			return e.hash();
		}
	};

	std::unordered_set<Edge, HashEdge> all_input_edges;

private:
	void add_expr_expr_edge(bool predicate_edge, const Expr& from,
			const Expr& to) final {
		all_input_edges.insert(Edge(from.get(), to.get()));
	}

	void add_stmt_expr_edge(bool predicate_edge, const Stmt& from,
			const Expr& to) final {
		all_input_edges.insert(Edge(from.get(), to.get()));
	}
};

// Defines a JIT-able fragment which fuses > 1 operations
struct FuseGroup {
	std::shared_ptr<CompileRequest> request;
	std::unordered_set<Node*> nodes;
	const std::shared_ptr<StatementRange> statement_range;
	const std::string dbg_name;

	FuseGroup(std::shared_ptr<CompileRequest>&& request,
		std::unordered_set<Node*>&& nodes,
		const std::shared_ptr<StatementRange>& statement_range,
		const std::string& dbg_name = "");
	~FuseGroup();

	bool contains(Node* p) const { return nodes.find(p) != nodes.end(); }
};

// Set of multiple FuseGroups
struct FuseGroups {
	void add(std::unique_ptr<FuseGroup>&& group);

	FuseGroup* get_group_by_node(Node* p) const {
		auto it = node2group.find(p);
		if (it != node2group.end()) {
			return it->second;
		}

		return nullptr;
	}
#if 0
	FuseGroup* get_group_by_sink(Node* p) const {
		auto it = sinks2group.find(p);
		if (it != sinks2group.end()) {
			return it->second;
		}

		return nullptr;
	}
#endif
	bool contains_node(Node* p) const {
		return node2group.find(p) != node2group.end();
	}

	size_t size() const { return groups.size(); }
	bool empty() const { return !size(); }

	std::unordered_map<Node*, FuseGroup*> sinks2group;

	~FuseGroups();
private:
	std::vector<std::unique_ptr<FuseGroup>> groups;

	std::unordered_map<Node*, FuseGroup*> node2group;

	void add_nodes_to_map(const std::unique_ptr<FuseGroup>& group);
	bool group_overlaps(const std::unique_ptr<FuseGroup>& group, bool assert) const;
};

struct IJitPass {
	FuseGroups* fuse_groups;
	QueryConfig* config;
	voila::Context& voila_context;
	excalibur::Context& sys_context;
	std::shared_ptr<IBudgetManager> budget_manager;

	IJitPass(excalibur::Context& sys_context, QueryConfig* config,
		voila::Context& voila_context,
		const std::shared_ptr<IBudgetManager>& budget_manager,
		FuseGroups* fuse_groups);

	struct IsJittableOpts {
		bool variable = false;
		bool can_jit_complex = true;
		bool excluded_already_jitted = true;
	};

	static bool
	is_jittable_func(const Function& f, IsJittableOpts* opts = nullptr)
	{
		using Tag = Function::Tag;

		switch (f.tag) {
		case Tag::kTGet:
		case Tag::kTuple:
		case Tag::kSelNum:
			return false;

		case Tag::kWritePos:
			return true;

		case Tag::kBucketInsert:
		case Tag::kSelUnion:
			return opts && opts->can_jit_complex;


		default:
			return true;
		}
	}

	static bool
	is_jittable_func_expr(const Function& f)
	{
		using Tag = Function::Tag;

		switch (f.tag) {
		case Tag::kWritePos:
		case Tag::kBucketInsert:
		case Tag::kSelUnion:
		case Tag::kSelTrue:
		case Tag::kSelFalse:
			return false;

		default:
			return is_jittable_func(f);
		}
	}

	void is_jittable(const Expr& expr, bool& jit_node, IsJittableOpts* opts = nullptr) const
	{
		jit_node = false;

		if (auto function = std::dynamic_pointer_cast<Function>(expr)) {
			jit_node = is_jittable_func(*function, opts);
			return;
		}

		if (auto constant = std::dynamic_pointer_cast<Constant>(expr)) {
			jit_node = can_inline_constant(constant);
			return;
		}

		if (opts && opts->variable) {
			if (auto read_var = std::dynamic_pointer_cast<ReadVariable>(expr)) {
				jit_node = true;
				return;
			}
		}
	}

	bool is_jittable_node(const Expr& expr, IsJittableOpts* opts = nullptr) const
	{
		bool jit_node = false;
		is_jittable(expr, jit_node, opts);
		return jit_node;
	}

	bool can_inline_constant(const std::shared_ptr<Constant>& e) const;
};

struct JitPreparePass : IJitPass, VoilaGraphTreePass {
	JitPreparePass(JitPrepareExpressionGraphPass& expr_graph,
		excalibur::Context& sys_context, QueryConfig* config,
		voila::Context& voila_context,
		const std::shared_ptr<IBudgetManager>& budget_manager,
		FuseGroups* fuse_groups);

	std::shared_ptr<FlavorSpec> flavor_spec;

private:
	struct Edge {
		Expr source;
		Expr dest;

		Edge(const Expr& from, const Expr& to) : source(from), dest(to) {
		}

		size_t hash() const {
			return (size_t)(source.get()) ^ (size_t)(dest.get());
		}

		bool operator==(const Edge& a) const {
			return a.source.get() == source.get() &&
				a.dest.get() == dest.get();
		}
	};

	struct HashEdge {
		size_t operator()(const Edge& e) const {
			return e.hash();
		}
	};
	std::unordered_set<Edge, HashEdge> input_edges;
	std::unordered_set<Edge, HashEdge> predicate_edges;
	std::unordered_set<Expr> nodes;
	std::unordered_set<Expr> predicates;
	JitPrepareExpressionGraphPass& expr_graph;

	struct Answer {
		std::unordered_set<Expr> picked_nodes;
		std::unordered_set<Expression*> picked_node_ptrs;

		void clear() {
			picked_nodes.clear();
			picked_node_ptrs.clear();
		}

		void add(const Expr& e) {
			picked_nodes.insert(e);
			picked_node_ptrs.insert(e.get());
		}

		bool empty() const { return picked_nodes.empty(); }
	};

	void _flush_groups_create_request(CompileRequest& request, const Expr& pe,
			const Answer& answer);

	void flush_groups_create_request(CompileRequest& request, const Answer& answer);

	void flush_groups_for_predicate(Answer& answer,
			std::unordered_set<Expression*>& discovered,
			std::unordered_set<Expression*>& temporary_marked,
			const Expr& start_node, size_t depth);

	void flush_groups();

	void add_input_edge(const Expr& parent, const Expr& child) {
		nodes.insert(child);
		input_edges.insert(Edge(parent, child));
		input_edges.insert(Edge(child, parent));
	}

	void add_predicate_edge(const Expr& parent, const Expr& predicate) {
		predicates.insert(predicate);
		predicate_edges.insert(Edge(parent, predicate));
	}

	void on_expression(const std::shared_ptr<Function>& e) final;

	void handle(const Stmt& s) final;
	void finalize() final {
		flush_groups();
	}

	void clear() {
		input_edges.clear();
		predicate_edges.clear();
		nodes.clear();
		predicates.clear();
	}

	// Random JIT generation
	std::unique_ptr<std::mt19937> random_engine;
	std::uniform_int_distribution<char> random_bool;

	bool get_rand_bool(bool default_val) {
		if (!random_engine) {
			return default_val;
		}

		return random_bool(*random_engine) == 1;
	}
};

} /* engine::voila */
