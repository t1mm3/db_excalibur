#include "statement_fragment_selection_pass.hpp"

#include "compile_request.hpp"
#include "system/system.hpp"
#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/voila/statement_identifier.hpp"
#include "engine/adaptive/decisions.hpp"
#include "engine/adaptive/plan_profile.hpp"
#include "engine/adaptive/rule_triggers.hpp"

using namespace engine;
using namespace voila;

#if 0
#define TRACE_DEBUG(...) LOG_DEBUG(__VA_ARGS__)
#define TRACE_WARN(...) LOG_WARN(__VA_ARGS__)
#define TRACE_ERROR(...) LOG_ERROR(__VA_ARGS__)
#define DUMP_VOILA(x) x->dump()
#else
#define TRACE_DEBUG(...)
#define TRACE_WARN(...)
#define TRACE_ERROR(...)
#define DUMP_VOILA(x)
#endif

#define REMOVE_DEAD_VARS_FROM_SINK

struct OnStmtExprPass : VoilaTreePass {
	OnStmtExprPass(const std::function<bool(const Stmt& e)>& on_stmt,
		const std::function<bool(const Expr& e)>& on_expr)
	 : on_stmt(on_stmt), on_expr(on_expr)
	{
	}

	void handle(const Stmt& s) final {
		if (!s || !on_stmt(s)) {
			return;
		}
		VoilaTreePass::handle(s);
	}

	void operator()(const Stmt& s) {
		handle(s);
	}

	void handle(const Expr& s) final {
		if (!s || !on_expr(s)) {
			return;
		}
		VoilaTreePass::handle(s);
	}

private:
	std::function<bool(const Stmt& e)> on_stmt;
	std::function<bool(const Expr& e)> on_expr;
};


struct OnExprPass : VoilaTreePass {
	OnExprPass(const std::function<bool(const Expr& e)>& on_expr)
	 : on_expr(on_expr)
	{
	}

	void handle(const Stmt& s) final {
		VoilaTreePass::handle(s);
	}

	void operator()(const Stmt& s) {
		handle(s);
	}

	void handle(const Expr& s) final {
		if (!s || !on_expr(s)) {
			return;
		}
		VoilaTreePass::handle(s);
	}

private:
	std::function<bool(const Expr& e)> on_expr;
};

struct OnStmtPass : VoilaTreePass {
	OnStmtPass(const std::function<bool(const Stmt& e)>& on_stmt)
	 : on_stmt(on_stmt)
	{
	}

	void handle(const Stmt& s) final {
		if (on_stmt(s)) {
			VoilaTreePass::handle(s);
		}
	}

private:
	std::function<bool(const Stmt& e)> on_stmt;
};


template<typename T>
static Type*
get_type1(const T& voila_context, Node* ptr)
{
	auto it_info = voila_context.infos.find(ptr);
	ASSERT(it_info != voila_context.infos.end());
	ASSERT(it_info->second.size() == 1);

	return it_info->second[0].type;
}

struct ShortVarInfoPass : VoilaTreePass {
	struct StmtInfo {
		std::unordered_set<Expression*> read_expr_set;
		std::vector<Expr> read_expr_vec;

		std::unordered_set<Variable*> write_set;
		std::vector<Var> write_vec;

		void clear() {
			read_expr_set.clear();
			read_expr_vec.clear();

			write_set.clear();
			write_vec.clear();
		}
	};

	ShortVarInfoPass(CompileRequest& request)
	 : request(request) {}

	StmtInfo operator()(const Stmt& s) {
		info.clear();
		handle(s);
		return info;
	}
private:
	CompileRequest& request;
	StmtInfo info;

	void on_statement(const std::shared_ptr<Assign>& b) final {
		ASSERT(b->var.get());
		if (info.write_set.find(b->var.get()) == info.write_set.end()) {
			info.write_set.insert(b->var.get());
			info.write_vec.push_back(b->var);
		}

		VoilaTreePass::on_statement(b);
	}

	void on_expression(const std::shared_ptr<ReadVariable>& e) final {
		if (info.read_expr_set.find(e.get()) == info.read_expr_set.end()) {
			info.read_expr_set.insert(e.get());
			info.read_expr_vec.push_back(e);
		}

		VoilaTreePass::on_expression(e);
	}

	void handle(const Stmt& s) final {
		VoilaTreePass::handle(s);
	}

	void handle(const Expr& s) final {
		if (!s || request.contains_source(s)) {
			return;
		}

		VoilaTreePass::handle(s);
	}
};



struct StatementDiscoverInternalNodes : VoilaTreePass {
	void operator()(const Stmt& stmt) { handle(stmt); }

	std::unordered_set<Node*> nodes_set;
	std::vector<std::shared_ptr<Node>> nodes_vec;

	StatementDiscoverInternalNodes(const IJitPass& jit_pass, CompileRequest& request)
	 : jit_pass(jit_pass), request(request) {

	}

private:
	void handle(const Stmt& s) override {
		if (!s) {
			return;
		}
		node(s);
		VoilaTreePass::handle(s);
	}

	void handle(const Expr& s) override {
		if (!s) {
			return;
		}

		// with adaptive, we might JIT some nodes twice (diff statements, but same internal nodes)
		if (jit_pass.fuse_groups->contains_node(s.get())) {
			return;
		}

		node(s);

		// dont go across bad nodes
		if (!jit_pass.is_jittable_node(s) || request.contains_source(s)) {
			return;
		}

		VoilaTreePass::handle(s);
	}

	template<typename T>
	void node(const T& s)
	{
		if (nodes_set.find(s.get()) != nodes_set.end()) {
			return;
		}

		nodes_set.insert(s.get());
		nodes_vec.push_back(s);
	}

	const IJitPass& jit_pass;
	CompileRequest& request;
};

struct HasUnjitableExpression : VoilaTreePass {
	HasUnjitableExpression(const IJitPass& jit_pass, CompileRequest* request,
		const StatementFragmentSelection::ConstructCandidateOpts& opts)
	 : opts(opts), jit_pass(jit_pass), request(request)
	{
	}

	size_t get_count() const { return count; }
	size_t num_traversed_nodes() const { return visited.size(); }
	size_t num_traversed_comments() const { return visited_comments.size(); }

	IJitPass::IsJittableOpts* jit_opts = nullptr;

	const StatementFragmentSelection::ConstructCandidateOpts& opts;

private:
	virtual void on_unjittable(const Expr& e) {}

	bool excluded_already_jitted() const {
		return !jit_opts || jit_opts->excluded_already_jitted;
	}

	void handle(const Expr& s) final {
		if (!s) {
			return;
		}

		auto ptr = s.get();
		if (visited.find(ptr) != visited.end()) {
			return;
		}

		visited.insert(ptr);

		bool has_unjitable = !jit_pass.is_jittable_node(s, jit_opts);
		if (has_unjitable) {
			// DUMP_VOILA(s);
			// LOG_DEBUG("StatementFragmentSelectionPass: HasUnjitableExpression: found unjitable");
			on_unjittable(s);
		} else if (excluded_already_jitted() && jit_pass.fuse_groups->contains_node(ptr)) {
			on_unjittable(s);
		} else if (request && request->contains_source(s)) {
			on_unjittable(s);
		} else {
			count++;
			VoilaTreePass::handle(s);
		}
	}

	void handle(const Stmt& s) final {
		if (!s) {
			return;
		}

		auto ptr = s.get();
		if (visited.find(ptr) != visited.end()) {
			return;
		}

		visited.insert(ptr);
		if (s->flags & voila::Node::kStmtComment) {
			visited_comments.insert(ptr);
		} else {
			count++;
		}

		VoilaTreePass::handle(s);
	}

	const IJitPass& jit_pass;
	std::unordered_set<Node*> visited;
	size_t count = 0;
	std::unordered_set<Node*> visited_comments;

	CompileRequest* request = nullptr;
};


struct CollectUnjitableExpression : HasUnjitableExpression {
	CollectUnjitableExpression(const IJitPass& jit_pass, CompileRequest* request,
		const StatementFragmentSelection::ConstructCandidateOpts& opts)
	 : HasUnjitableExpression(jit_pass, request, opts)
	{
	}

	std::vector<Expr> unjittable_expr_vec;

private:
	std::unordered_set<Expression*> unjittable_expr_set;

	virtual void on_unjittable(const Expr& e) final {
		auto ptr = e.get();
		if (unjittable_expr_set.find(ptr) != unjittable_expr_set.end()) {
			return;
		}
		unjittable_expr_set.insert(ptr);
		unjittable_expr_vec.push_back(e);
	}
};

struct DiscoverJitBreakers : HasUnjitableExpression {
	DiscoverJitBreakers(const IJitPass& jit_pass,
		const StatementFragmentSelection::ConstructCandidateOpts& opts)
	 : HasUnjitableExpression(jit_pass, nullptr, opts)
	{
	}

	bool has_jit_breaker = false;

private:

	virtual void on_unjittable(const Expr& e) final {
		if (has_jit_breaker) {
			return;
		}

		if (auto func = std::dynamic_pointer_cast<voila::Function>(e)) {
			if (func->tag == Function::Tag::kWritePos) {
				LOG_DEBUG("DiscoverJitBreakers: Found JIT breaker (writepos)");
				DUMP_VOILA(e);
				has_jit_breaker = true;
				return;
			}
		}

		auto mask = voila::Node::kExprGetScanPos; //  | voila::Node::kExprScan;

		if (e->flags & mask) {
			LOG_DEBUG("DiscoverJitBreakers: Found JIT breaker (scan, getpos)");
			DUMP_VOILA(e);
			has_jit_breaker = true;
			return;
		}

		VoilaTreePass::handle(e);
	}
};

StatementFragmentSelection::FragmentResult
StatementFragmentSelection::try_update_best(Candidate& best,
	Candidate&& curr, const StatementFragmentSelection::ConstructCandidateOpts& opts) const
{
	bool is_better;

	{
		CollectUnjitableExpression has_unjitable_expr(jit_prepare_pass, nullptr,
			opts);
		has_unjitable_expr(curr.stmts);

		ASSERT(has_unjitable_expr.get_count());
		curr.num_nodes = has_unjitable_expr.get_count();
		if (curr.num_nodes <= 2) {
			LOG_DEBUG("StatementFragmentSelectionPass: Not enough nodes jitted %llu",
				curr.num_nodes);
			return kCannotCompile;
		}

		ASSERT(comparator);
		is_better = comparator->is_better_than_b(curr, best);
		if (!is_better) {
			LOG_DEBUG("StatementFragmentSelectionPass: Best num %llu > curr num %llu",
				best.num_nodes, curr.num_nodes);
			return kCannotCompile;
		}
	}

	{
		DiscoverJitBreakers discovered_jit_breakers(jit_prepare_pass, opts);
		discovered_jit_breakers(curr.stmts);
		if (discovered_jit_breakers.has_jit_breaker) {
			LOG_DEBUG("StatementFragmentSelectionPass: Found hard jit breaker");
			return kCannotCompile;
		}
	}

	if (!candidate_create_request(curr, true, nullptr, "Check")) {
		LOG_DEBUG("StatementFragmentSelectionPass: JIT cannot create CompileRequest");
		return kCannotCompile;
	}

	LOG_DEBUG("StatementFragmentSelectionPass: JIT Update best. num %lld, best %lld",
		curr.num_nodes, best.num_nodes);

	LOG_TRACE("TODO: This metric is slightly off, because sometimes we cannot JIT "
		"expressions that have been used before our JIT statement "
		"(and still have the same pointer)");
	ASSERT(curr.num_nodes >= best.num_nodes);
	best = std::move(curr);
	return kSuccess;
}

bool
StatementFragmentSelection::candidate_create_request(Candidate& best_candidate,
	bool just_try, const std::shared_ptr<FlavorSpec>& flavor_spec,
	const char* dbg_path) const
{
	std::unordered_set<Node*> node_set;
	std::vector<NodePtr> node_vec;
	bool bail_out = false;

	auto must_have_type = [&] (auto ptr) {
		ASSERT(get_type1(voila_context, ptr));
	};

	auto insert_node = [&] (const auto& node_ptr) {
		auto p = node_ptr.get();
		if (node_set.find(p) != node_set.end()) {
			return;
		}

		node_set.insert(p);
		node_vec.push_back(node_ptr);
	};

	std::shared_ptr<CompileRequest> request(
		std::make_shared<CompileRequest>(
			jit_prepare_pass.sys_context, jit_prepare_pass.config, voila_context));


	// sources are expressions, either unjittable ...
	CollectUnjitableExpression collect_unjit_expr(jit_prepare_pass, request.get(),
		ConstructCandidateOpts());
	IJitPass::IsJittableOpts opts = { true };
	collect_unjit_expr.jit_opts = &opts;

	collect_unjit_expr(best_candidate.stmts);
	for (auto& expr : collect_unjit_expr.unjittable_expr_vec) {
		DUMP_VOILA(expr);
		TRACE_WARN("StatementFragmentSelectionPass: %s: Add unjitaable",
			dbg_path);
		must_have_type(expr.get());
		request->add_source(expr);
	}


#ifdef REMOVE_DEAD_VARS_FROM_SINK
	std::unordered_set<Variable*> dead_variables;

	LOG_DEBUG("StatementFragmentSelectionPass: Collect dead variables");

	// propagate dead variables up, therefore we need a deep pass :(
	OnStmtPass collect_dead_vars([&] (auto& stmt) {
		stmt->dump();
		for (auto& var : stmt->var_dead_after) {
			LOG_DEBUG("StatementFragmentSelectionPass: %s: insert dead_variable %p '%s'",
				dbg_path, var.get(), var->dbg_name.c_str());
			dead_variables.insert(var.get());
		}
		return true;
	});

	collect_dead_vars(best_candidate.stmts);
#endif

	// handle overlapping expressions used before (become sources) and
	// afterwards (become sinks)
	if (!bail_out) { //  && !just_try
		auto& scope = *best_candidate.scope;
		int64_t scope_size = scope.size();

		LOG_DEBUG("StatementFragmentSelectionPass: %s: Can have overlapping exprs: "
			"start_idx=%lld end_idx=%lld, size=%lld",
			dbg_path, best_candidate.start_idx, best_candidate.end_idx, scope_size);
		std::unordered_set<Expression*> prefix_exprs_set;
		std::unordered_set<Expression*> exprs_set;
		std::unordered_set<Variable*> vars_set;

		// #error Check prefix whether we share expressions
		OnExprPass prefix_exprs([&] (const Expr& e) {
			TRACE_WARN("StatementFragmentSelectionPass: %s: Prefix expr %p",
				dbg_path, e.get());
			DUMP_VOILA(e);

			prefix_exprs_set.insert(e.get());

			return true;
		});

		for (int64_t i=0; i<best_candidate.start_idx; i++) {
#ifdef TRACE1
			LOG_DEBUG("pre %lld", i);
			scope[i]->dump();
#endif
			prefix_exprs.handle(scope[i]);
		}

		// get exprs from within fragment
		OnStmtExprPass collect_exprs(
			[&] (const Stmt& s) {
				if (auto assign = dynamic_cast<Assign*>(s.get())) {
					vars_set.insert(assign->var.get());
				}
				return true;
			},
			[&] (const Expr& e) {
				if (request->contains_source(e)) {
					return false;
				}
				if (prefix_exprs_set.find(e.get()) != prefix_exprs_set.end()) {
					// used before our JIT block, add as source
					TRACE_WARN("StatementFragmentSelectionPass: %s: Expr used before JIT fragment. expr p=%p",
						dbg_path, e.get());
					e->dump();
					must_have_type(e.get());
					request->add_source(e);
					return false; // do not recurse further
				}

				exprs_set.insert(e.get());
				TRACE_WARN("StatementFragmentSelectionPass: %s: Add expr %p",
					dbg_path, e.get());
				DUMP_VOILA(e);

				return true;
			}
		);

		for (int64_t i=best_candidate.start_idx; i<best_candidate.end_idx; i++) {
#ifdef TRACE1
			LOG_DEBUG("in %lld", i);
			scope[i]->dump();
#endif
			collect_exprs.handle(scope[i]);
		}

		// get exprs that come afterwards that intersect with exprs
		// inside fragment
		OnExprPass collect_matching_exprs([&] (const Expr& e) {
			if (bail_out) {
				return false;
			}

			bool expr_contained = exprs_set.find(e.get()) != exprs_set.end();
			LOG_DEBUG("collect_matching_expr %d", (int)expr_contained);
			e->dump();

			if (expr_contained) {
				DUMP_VOILA(e);
				if (!request->contains_source(e)) {
					LOG_DEBUG("StatementFragmentSelectionPass: %s: %p "
						"Add expr sink", dbg_path, e.get());

					if (!dynamic_cast<ReadVariable*>(e.get())) {
						if (auto constant = dynamic_cast<Constant*>(e.get())) {
							LOG_DEBUG("StatementFragmentSelectionPass: %s: Would add Constant to sink. val='%s'",
								dbg_path, constant->value.c_str());
							bail_out = true;
							return false;
						}
						request->add_sink(e);

						ASSERT(!dynamic_cast<ReadVariable*>(e.get()));
					}
				} else {
					TRACE_WARN("StatementFragmentSelectionPass: %s: %p "
						"Skip ... Add expr sink", dbg_path, e.get());
				}
				return false;
			}
#if 1
			if (auto read_var = dynamic_cast<ReadVariable*>(e.get())) {
				bool var_contained = vars_set.find(read_var->var.get()) != vars_set.end();
				bool var_dead = dead_variables.find(read_var->var.get()) != dead_variables.end();
				LOG_DEBUG("collect_matching_var %d, dead %d, #dead %d",
					(int)var_contained, (int)var_dead, (int)dead_variables.size());
				e->dump();
#if 1
				if (var_contained && !var_dead) {
					request->add_sink(read_var->var);
				} else if (var_dead) {
					LOG_DEBUG("dead_variable");
				}
#endif
			}
#endif
			return true;
		});

		for (int64_t i=best_candidate.end_idx; i<scope_size; i++) {
#ifdef TRACE1
			LOG_DEBUG("post %lld", i);
			scope[i]->dump();
#endif
			collect_matching_exprs.handle(scope[i]);
		}
	}


	for (auto& stmt : best_candidate.stmts) {
		StatementDiscoverInternalNodes discover(jit_prepare_pass, *request);
		discover(stmt);

		insert_node(stmt);
		for (auto& s : discover.nodes_vec) {
			insert_node(s);
			if (auto expr = std::dynamic_pointer_cast<Expression>(s)) {
				if (expr->data_structure) {
					request->add_struct(expr->data_structure);
				}
			}
		}
		request->add_statement(stmt);
	}

	// ... or regular input stuff
	if (!bail_out) {
		std::unordered_set<Variable*> prefix_union_write_set;

		int64_t num_bucket_insert = 0;
		int64_t num_selunion = 0;

		OnExprPass check_expr_pass([&] (auto& expr) {
			if (auto fun = dynamic_cast<Function*>(expr.get())) {
				switch (fun->tag) {
				case Function::Tag::kBucketInsert:
					num_bucket_insert++;
					break;
				case Function::Tag::kSelUnion:
					num_selunion++;
					break;
				default:
					break;
				}
			}
			return true;
		});

		ShortVarInfoPass short_var_info(*request);

		int64_t stmt_idx = 0;
		OnStmtPass magic([&] (auto& stmt) {
			if (bail_out) {
				return false;
			}
			if (stmt->flags & (Node::kStmtLoop | Node::kStmtBlock)) {
				return true;
			}

			// maintain counters for checks
			check_expr_pass(stmt);

			const auto info = short_var_info(stmt);

			// add sinks, when variables are not dead
			if (auto assign = std::dynamic_pointer_cast<Assign>(stmt)) {
				must_have_type(assign.get());

				bool add = true;

#ifdef REMOVE_DEAD_VARS_FROM_SINK
				if (dead_variables.find(assign->var.get()) != dead_variables.end()) {
					add = false;
				}
#endif

				if (add) {
					request->add_sink(assign->var);
					DUMP_VOILA(assign);
					TRACE_WARN("StatementFragmentSelectionPass: %s: %p Add sink",
						dbg_path, stmt.get());
				}
			}

			// add input, if not create within (in prefix)
			TRACE_WARN("StatementFragmentSelectionPass: %s: statement %lld",
				dbg_path, stmt_idx);
			DUMP_VOILA(stmt);

			TRACE_WARN("StatementFragmentSelectionPass: %s: read_expr_vec %d entries",
				dbg_path, (int)info.read_expr_vec.size());
			for (auto& r : info.read_expr_vec) {
				auto read_var = std::dynamic_pointer_cast<ReadVariable>(r);
				ASSERT(read_var);

				const bool created_from_within_fragment =
					prefix_union_write_set.find(read_var->var.get()) != prefix_union_write_set.end();

				TRACE_WARN("StatementFragmentSelectionPass: %s: created_from_within_fragment %d",
					dbg_path, created_from_within_fragment);
				DUMP_VOILA(r);


				if (created_from_within_fragment) {
					TRACE_WARN("StatementFragmentSelectionPass: %s: Source did origin "
						"from this fragment. Skip",
						dbg_path);
					DUMP_VOILA(r);
					continue;
				} else {
					TRACE_WARN("StatementFragmentSelectionPass: %s: Outside source",
						dbg_path);
					DUMP_VOILA(r);
				}

				must_have_type(r.get());
				request->add_source(r);

				TRACE_WARN("StatementFragmentSelectionPass: %s: %p Add source %p",
					dbg_path, stmt.get(), r.get());
			}


			// add to write_set
			for (auto& w : info.write_set) {
				prefix_union_write_set.insert(w);
				TRACE_WARN("StatementFragmentSelectionPass: %s: insert prefix_union_write_set",
					dbg_path);
				DUMP_VOILA(w);
			}

			stmt_idx++;
			return true;
		});

		magic(best_candidate.stmts);


		// check counts
		if (!bail_out && num_bucket_insert != num_selunion) {
			LOG_DEBUG("StatementFragmentSelectionPass: %s: counts for BucketInsert and SelUnion do not match. "
				"insert=%lld, union=%lld",
				dbg_path, num_bucket_insert, num_selunion);
			bail_out = true;
		}
	}

	// make sure, our CompileRequest is valid
	if (!bail_out) {
		size_t num_input_preds = 0;
		for (auto& s : request->sources) {
			auto type = get_type1(voila_context, s.get());
			ASSERT(type);
			if (type->is_predicate()) {
				num_input_preds++;
			}
		}

		LOG_DEBUG("StatementFragmentSelectionPass: %s: Validate: num_input_preds %d",
			dbg_path, (int)num_input_preds);
		if (num_input_preds > 1) {
			for (auto& s : request->sources) {
				auto type = get_type1(voila_context, s.get());
				ASSERT(type);
				if (type->is_predicate()) {
					DUMP_VOILA(s);
				}
			}
			LOG_DEBUG("StatementFragmentSelectionPass: %s: Validate: too many input "
				"predicates ... abort",
				dbg_path);
			return false;
		}
		if (num_input_preds < 1) {
			LOG_DEBUG("StatementFragmentSelectionPass: %s: Validate: not enough input "
				"predicates ... abort",
				dbg_path);
			return false;
		}
	}

	std::unordered_set<Node*> node_ptrs;

	// cannot mix internal nodes and sources, remove them
	if (!bail_out) {
		for (auto& s : request->sources) {
			auto it = node_set.find(s.get());
			if (it == node_set.end()) {
				continue;
			}

			node_set.erase(it);
		}

		for (auto& s : request->sources) {
			ASSERT(node_set.find(s.get()) == node_set.end());
		}

		for (auto& s : node_vec) {
			if (fuse_groups->contains_node(s.get())) {
				// Cannot JIT this one, part is already jitted
				LOG_DEBUG("StatementFragmentSelectionPass: part (%p) is already jitted ... abort",
					s.get());
				DUMP_VOILA(s);
				return false;
			}

			// add internal expressiobs
			if (request->contains_source(s)) {
				continue;
			}

			node_ptrs.insert(s.get());
			if (auto expr = std::dynamic_pointer_cast<Expression>(s)) {
				TRACE_WARN("StatementFragmentSelectionPass: %s: side expression",
					dbg_path);
				DUMP_VOILA(expr);
				request->add_expression(std::move(expr));
			}
		}
	}

	if (bail_out) {
		LOG_DEBUG("StatementFragmentSelectionPass: %s: bailing out ... abort",
			dbg_path);
		return false;
	}

	if (just_try && !request->is_valid()) {
		LOG_DEBUG("StatementFragmentSelectionPass: %s: CompileRequest is not valid ... abort",
			dbg_path);
		// ASSERT(false && "Shouldn't happen");
		return false;
	}

	if (!just_try) {
		if (flavor_spec) {
			request->flavor = flavor_spec;
		}

		best_candidate.request = std::move(request);
		best_candidate.node_ptrs = std::move(node_ptrs);
	}

	return true;
}

static bool
is_trivial_statement(const Stmt& stmt)
{
	if (auto assign = std::dynamic_pointer_cast<Assign>(stmt)) {
		const auto& val = assign->value;
		if (auto scan = std::dynamic_pointer_cast<Scan>(val)) {
			return true;
		}
		if (auto scan_pos = std::dynamic_pointer_cast<GetScanPos>(val)) {
			return true;
		}
		if (auto function = std::dynamic_pointer_cast<Function>(val)) {
			switch (function->tag) {
			case Function::kSelNum:
				return true;
			default:
				break;
			}
		}
	}
	return false;
}

static bool
is_sel(voila::Function::Tag t)
{
	return t == voila::Function::kSelTrue || t == voila::Function::kSelFalse;
}

enum GetProfilesResult {
	kOkay = 0,
	kNoLocation,
	kNoProfile,
};


template<typename T>
static GetProfilesResult
_get_profiles_for_stmt(
	const Stmt& stmt,
	const voila::Context& voila_context,
	engine::adaptive::InputPlanProfile* input_plan_profile,
	const T& on_profile)
{
	auto loc_it = voila_context.locations.find(stmt.get());
	if (loc_it == voila_context.locations.end()) {
		return GetProfilesResult::kNoLocation;
	}

	const auto& loc = loc_it->second.location;
	ASSERT(loc);

	auto& prim_profiles = input_plan_profile->begin_prims;
	auto prim_it = prim_profiles.find(*loc);
	if (prim_it == prim_profiles.end()) {
		return GetProfilesResult::kNoProfile;
	}

	for (auto& p : prim_it->second) {
		on_profile(p);
	}

	return kOkay;
}

static GetProfilesResult
get_profiles_for_stmt(
	std::vector<engine::adaptive::PrimProf*>& profiles,
	const Stmt& stmt,
	const voila::Context& voila_context,
	engine::adaptive::InputPlanProfile* input_plan_profile)
{
	return _get_profiles_for_stmt(stmt, voila_context, input_plan_profile,
		[&] (auto p) {
			profiles.push_back(p);
		});
}

static GetProfilesResult
get_profiles_for_stmt(
	const Stmt& stmt,
	const voila::Context& voila_context,
	engine::adaptive::InputPlanProfile* input_plan_profile,
	const std::function<void(engine::adaptive::PrimProf*)>& on_profile)
{
	return _get_profiles_for_stmt(stmt, voila_context, input_plan_profile,
		on_profile);
}


struct CheckStatement {
	using ConstructCandidateOpts = StatementFragmentSelection::ConstructCandidateOpts;

	const voila::Context& voila_context;
	const FuseGroups& fuse_groups;
	const ConstructCandidateOpts& opts;
	const engine::adaptive::JitStatementFragmentOpts* jit_opts;

	CheckStatement(const voila::Context& voila_context,
		const FuseGroups& fuse_groups,
		const ConstructCandidateOpts& opts)
	 : voila_context(voila_context), fuse_groups(fuse_groups), opts(opts),
		jit_opts(opts.jit_opts)
	{
	}

	bool
	operator()(const Stmt& stmt, int64_t i, size_t level,
		const std::function<void(const std::string&, const Stmt& stmt)>& on_error)
	{
		const auto invalid_statement_mask =
			voila::Node::kStmtEmit | voila::Node::kStmtEndOfFlow;
		const auto recurse_mask =
			voila::Node::kStmtLoop | voila::Node::kStmtBlock;

	#define ERROR(x) { \
				LOG_DEBUG("CheckStatement failed '" x "'"); \
				on_error(x, stmt); \
				return false; \
			}

		bool first_level = level == 0;

		if (false /*!stmt->can_inline*/) {
			ERROR("Cannot inline statement");
		} else if (stmt->flags & invalid_statement_mask) {
			ERROR("Has unjitable statement");
		} else if (stmt->flags & recurse_mask) {
			auto block = std::dynamic_pointer_cast<Block>(stmt);
			ASSERT(block);

			bool failed = false;
			LOG_DEBUG("Recurse");
			for (int64_t k=0; k<block->statements.size(); k++) {
				failed |= !(*this)(block->statements[k], k, level+1,
					[] (const std::string& text, const Stmt& stmt) {
						DUMP_VOILA(stmt);
						LOG_DEBUG("Recursion failed with '%s'", text.c_str());
					});
				if (failed) {
					break;
				}
			}

			if (failed) {
				ERROR("Recursion failed");
			}
		} else if (fuse_groups.contains_node(stmt.get())) {
			ERROR("Already chosen");
		} else if (first_level && is_trivial_statement(stmt)) {
			ERROR("Has trivial statement");
		} else {
			size_t num_sel = 0;
			size_t num_mem = 0;
			size_t num_mem_non_coloc = 0;
			size_t num_complex = 0;
			OnExprPass on_sel([&] (auto& expr) {
				if (auto fun = dynamic_cast<Function*>(expr.get())) {
					if (is_sel(fun->tag)) {
						num_sel++;
					}

					auto flags = Function::get_flags(fun->tag);
					if (flags & (Function::kReadsFromMemory | Function::kWritesToMemory)) {
						num_mem++;
						if (!(flags & Function::kColocatableAccess)) {
							num_mem_non_coloc++;
						}
					}

					if (flags & (Function::kComplexOperation)) {
						num_complex++;
					}
				}
				return true;
			});
			on_sel.handle(stmt);

			ASSERT(num_mem >= num_mem_non_coloc);

			LOG_DEBUG("%lld: num_complex %lld, num_mem %lld, num_sel %lld",
				i, num_complex, num_mem, num_sel);
			if (num_complex && jit_opts && !jit_opts->can_pass_complex) {
				ERROR("Cannot pass complexop: Disabled");
			}

			if (num_sel) {
				if (jit_opts && !jit_opts->can_pass_sel) {
					ERROR("Cannot pass seltrue/selfalse: Disabled");
				}

				if (jit_opts && jit_opts->can_pass_sel < 0 && opts.input_plan_profile) {
					std::vector<engine::adaptive::PrimProf*> sel_profiles;

					auto res = _get_profiles_for_stmt(stmt, voila_context,
						opts.input_plan_profile,
						[&] (auto prof) {
							if (is_sel((voila::Function::Tag)prof->voila_func_tag)) {
								sel_profiles.push_back(prof);
							}
						});
					switch (res) {
					case GetProfilesResult::kNoProfile:
						ERROR("Cannot pass seltrue/selfalse: No profile");
						break;

					case GetProfilesResult::kNoLocation:
						ERROR("Cannot pass seltrue/selfalse: No location");
						break;

					case GetProfilesResult::kOkay:
						break;

					default:
						ASSERT(false);
						break;
					}

					bool can_pass = false;
					if (!sel_profiles.empty()) {
						can_pass = true;
						for (auto& prof : sel_profiles) {
							double sel = prof->prof_data.get_selectivity();
							if (std::isnan(sel)) {
								can_pass &= false;
								continue;
							}
							if (sel < 1.0 - jit_opts->selectivity_margin &&
									sel > jit_opts->selectivity_margin) {
								can_pass &= false;
							}
						}
					}

					if (!can_pass) {
						ERROR("Cannot pass seltrue/selfalse: Checked profile. Cannot pass");
					}
				}
			}

			if (num_mem) {
				if (jit_opts && !jit_opts->can_pass_mem) {
					ERROR("Cannot pass memory ops: Disabled");
				}

				if (jit_opts && jit_opts->can_pass_mem < 0 && opts.input_plan_profile) {
					std::vector<engine::adaptive::PrimProf*> sel_profiles;

					auto matching_flags = Function::kReadsFromMemory |
						Function::kWritesToMemory;

					auto res = _get_profiles_for_stmt(stmt, voila_context,
						opts.input_plan_profile,
						[&] (auto prof) {
							auto tag = (voila::Function::Tag)prof->voila_func_tag;
							auto flags = Function::get_flags(tag);
							if (!(flags & matching_flags)) {
								return;
							}
							sel_profiles.push_back(prof);
						});
					switch (res) {
					case GetProfilesResult::kNoProfile:
						ERROR("Cannot pass memory ops: No profile");
						break;

					case GetProfilesResult::kNoLocation:
						ERROR("Cannot pass memory ops: No location");
						break;

					case GetProfilesResult::kOkay:
						break;

					default:
						ASSERT(false);
						break;
					}

					bool can_pass = false;
					if (!sel_profiles.empty()) {

						can_pass = true;
						for (auto& prof : sel_profiles) {
							double cyc_tup = prof->prof_data.get_cycles_per_tuple();
							LOG_DEBUG("mem op: cyc_tup %f", cyc_tup);
							if (std::isnan(cyc_tup) || !std::isfinite(cyc_tup)) {
								can_pass &= false;
								continue;
							}

							if (cyc_tup > jit_opts->mem_max_in_cache_cyc_tup) {
								can_pass &= false;
							}
						}
					}

					if (can_pass) {
						LOG_DEBUG("Can pass: mem op");
					}

					if (!can_pass) {
						ERROR("Cannot pass memory ops: Checked profile. Cannot pass");
					}
				}
			}

		}

	#undef ERROR
		return true;
	}

};



StatementFragmentSelection::FragmentResult
StatementFragmentSelection::construct_candidate(int64_t start_idx,
	int64_t end_idx, int64_t& out_skip_first,
	Candidate& curr_candidate, Candidate& best_candidate,
	int64_t num, const std::vector<Stmt>& stmts,
	const ConstructCandidateOpts& opts)
{
	out_skip_first = 0;

	int64_t diff = end_idx-start_idx;
	ASSERT(diff >= 0);
	ASSERT(start_idx < end_idx);
	ASSERT(start_idx >= 0 && start_idx < num);
	ASSERT(end_idx >= 0 && end_idx <= num);

	LOG_DEBUG("StatementFragmentSelectionPass: Fragment [%lld, %lld)",
		start_idx, end_idx);
	if (LOG_WILL_LOG(DEBUG)) {
		for (int64_t i=start_idx; i<end_idx; i++) {
			stmts[i]->dump();
		}
	}
	// create canidate
	curr_candidate.clear();
	curr_candidate.reserve(diff);
	curr_candidate.start_idx = start_idx;
	curr_candidate.end_idx = end_idx;
	curr_candidate.scope = &stmts;

	bool run = true;
	int64_t last_index = -1;
	size_t num_error = 0;

	// check for invalid/unsupported statements
	for (int64_t i=start_idx; i<end_idx; i++) {
		auto& stmt = stmts[i];

		CheckStatement check_statement(voila_context, *fuse_groups, opts);

		bool succ = check_statement(stmt, i, 0,
			[&] (const std::string& text, const Stmt& stmt) {
				DUMP_VOILA(stmt);
				LOG_DEBUG("StatementFragmentSelectionPass: %s (run %d, i %d)",
					text.c_str(), run, i);

				if (run) last_index = i;
				num_error++;
			});
		if (succ) {
			run = false;
		}

		DUMP_VOILA(stmt);
		curr_candidate.stmts.push_back(stmt);
	}

	if (last_index >= 0) {
		out_skip_first = last_index+1;
	}

	if (num_error) {
		LOG_DEBUG("StatementFragmentSelectionPass: Had error, cannot compile");
		return kCannotCompile;
	}

	return try_update_best(best_candidate, std::move(curr_candidate), opts);
}

Type*
StatementFragmentSelection::get_var_type(Variable* var,
	const voila::Context& voila_context)
{
	auto it = voila_context.infos.find(var);
	ASSERT(it != voila_context.infos.end());

	ASSERT(it->second.size() == 1);

	auto type = it->second[0].type;
	ASSERT(type);

	return type;
}

StatementFragmentSelection::StatementFragmentSelection(IJitPass& jit_prepare_pass,
	StatementFragmentSelectionComparator* given_comparator)
 : jit_prepare_pass(jit_prepare_pass),
	voila_context(jit_prepare_pass.voila_context)
{
	fuse_groups = jit_prepare_pass.fuse_groups;

	comparator = given_comparator;
	if (!comparator) {
		default_comparator = std::make_unique<StatementFragmentSelectionComparator>();
		comparator = default_comparator.get();
		ASSERT(comparator);
	}
}

StatementFragmentSelection::~StatementFragmentSelection()
{

}




void
StatementFragmentSelectionPass::max_fragment(const std::vector<Stmt>& stmts)
{
	const int64_t num = stmts.size();
	ASSERT(num);

	StatementFragmentSelection::ConstructCandidateOpts opts;

	LOG_DEBUG("StatementFragmentSelectionPass::max_fragment");

	// fixed point iteration.
	// try to get biggest fragments until, we cannot find such anymore.
	size_t iteration = 0;
	while (1) {
		Candidate curr_candidate;
		Candidate best_candidate;

		// TODO: maybe rethink and try to have some merge-style algorithm
		int64_t start_idx = 0;
		while (start_idx < num) {
			int64_t next_start_idx = start_idx+1;

			for (int64_t end_idx=num; end_idx>start_idx; end_idx--) {
				int64_t out_skip_first = 0;
				selection.construct_candidate(
					start_idx, end_idx, out_skip_first,
					curr_candidate, best_candidate, num,
					stmts, opts);
				if (out_skip_first) {
					// skip first n statements, if we cannot compile them anyway
					ASSERT(out_skip_first > 0);
					next_start_idx = std::max(next_start_idx, start_idx + out_skip_first);
					LOG_DEBUG("StatementFragmentSelectionPass: Skipping %lld statements "
						"next index %lld",
						out_skip_first, next_start_idx);
					break;
				}
			}

			ASSERT(next_start_idx > start_idx);
			start_idx = next_start_idx;
		}

		// construct FuseGroup from candidate
		if (!best_candidate.empty()) {
			commit_candidate(std::move(best_candidate));
		} else {
			LOG_DEBUG("StatementFragmentSelectionPass: cannot construct new FuseGroup "
				"... Abort (iteration %llu)",
				iteration);
			break;
		}

		iteration++;
	}
}

void
StatementFragmentSelectionPass::commit_candidate(Candidate&& best_candidate)
{
	LOG_DEBUG("StatementFragmentSelectionPass: Construct FuseGroup [%lld, %lld)",
		best_candidate.start_idx, best_candidate.end_idx);
	for (auto& stmt : best_candidate.stmts) {
		stmt->dump();
	}
	LOG_DEBUG("StatementFragmentSelectionPass: End Dump");

	if (best_candidate.stmts.empty()) {
		LOG_ERROR("StatementFragmentSelectionPass: No statements");
		return;
	}

	ASSERT(!best_candidate.empty());

	bool created_request = selection.candidate_create_request(
		best_candidate, false, flavor_spec, "Construct");
	ASSERT(created_request);

	best_candidate.request->debug_validate();
	best_candidate.request->dump();

	fuse_groups->add(std::make_unique<FuseGroup>(
		std::move(best_candidate.request),
		std::move(best_candidate.node_ptrs),
		nullptr,
		std::to_string(best_candidate.start_idx) +
			std::string(",") + std::to_string(best_candidate.end_idx)));

	LOG_DEBUG("StatementFragmentSelectionPass: new group [%lld, %lld) ",
		best_candidate.start_idx, best_candidate.end_idx);
}

void
StatementFragmentSelectionPass::handle(const std::vector<Stmt>& stmts)
{
	const int64_t num = stmts.size();
	ASSERT(num);


	if (given_fragment) {
		StatementFragmentSelection::ConstructCandidateOpts opts;
		// opts.jit_opts = &given_fragment->jit_opts;

		statement_tracker->enter();

		ASSERT(given_fragment->range);
		const auto& range = *given_fragment->range;
		LOG_DEBUG("StatementFragmentSelectionPass::given_fragment: range=%s",
			range.to_string().c_str());
		size_t line = 0;

		for (auto& stmt : stmts) {
			statement_tracker->update(line);

			auto loc = statement_tracker->get_identifier();

			if (loc == range.begin) {
				LOG_DEBUG("StatementFragmentSelectionPass::found start");
				num_given_found++;

				Candidate curr_candidate;
				Candidate best_candidate;

				ASSERT(line == range.begin.get_last_line());

				int64_t start_idx = range.begin.get_last_line();
				int64_t end_idx = range.end.get_last_line();

				ASSERT(end_idx == start_idx + range.num_statements());

				ASSERT(end_idx <= num);

				int64_t out_skip_first = 0;


				auto r = selection.construct_candidate(
					start_idx, end_idx, out_skip_first,
					curr_candidate, best_candidate, num, stmts,
					opts);

				ASSERT(r != StatementFragmentSelection::FragmentResult::kCannotCompile);
				ASSERT(!out_skip_first && "Given fragment should be valid and work");

				if (LOG_WILL_LOG(DEBUG)) {
					LOG_DEBUG("StatementFragmentSelectionPass: Found JitFragment");
					for (auto& stmt : best_candidate.stmts) {
						stmt->dump();
					}
				}
				ASSERT(!best_candidate.empty());
				commit_candidate(std::move(best_candidate));
			}

			if (loc == range.end) {
				ASSERT(num_given_found > 0);
			}

			handle(stmt);
			line++;
		}

		statement_tracker->leave();
	} else {
		max_fragment(stmts);
		LOG_DEBUG("StatementFragmentSelectionPass: handle: JIT continue");
		VoilaTreePass::handle(stmts);
	}
}

void
StatementFragmentSelectionPass::operator()(const std::vector<Stmt>& stmts)
{
	LOG_DEBUG("StatementFragmentSelectionPass:()");

	VoilaTreePass::operator()(stmts);
}

StatementFragmentSelectionPass::StatementFragmentSelectionPass(
	excalibur::Context& sys_context,
	QueryConfig* config,
	voila::Context& voila_context,
	const std::shared_ptr<IBudgetManager>& budget_manager,
	FuseGroups* fuse_groups,
	const adaptive::JitStatementFragmentId* given_fragment,
	size_t operator_id)
 : IJitPass(sys_context, config, voila_context, budget_manager, fuse_groups),
	selection(*this), given_fragment(given_fragment)
{
	statement_tracker = std::make_unique<VoilaStatementTracker>(operator_id);
}

StatementFragmentSelectionPass::~StatementFragmentSelectionPass()
{
}