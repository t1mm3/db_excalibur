#include "jit_prepare_pass.hpp"

#include "compile_request.hpp"
#include "system/system.hpp"
#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/voila/statement_identifier.hpp"

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

template<typename T>
static Type*
get_type1(const T& voila_context, Node* ptr)
{
	auto it_info = voila_context.infos.find(ptr);
	ASSERT(it_info != voila_context.infos.end());
	ASSERT(it_info->second.size() == 1);

	return it_info->second[0].type;
}

FuseGroup::FuseGroup(std::shared_ptr<CompileRequest>&& request,
	std::unordered_set<Node*>&& nodes,
	const std::shared_ptr<StatementRange>& statement_range,
	const std::string& dbg_name)
 : request(std::move(request)), nodes(std::move(nodes)),
	statement_range(statement_range), dbg_name(dbg_name)
{
}

FuseGroup::~FuseGroup()
{

}



void
FuseGroups::add_nodes_to_map(const std::unique_ptr<FuseGroup>& group)
{
	LOG_DEBUG("FuseGroups: add_nodes_to_map(group=%p '%s')",
		group.get(), group->dbg_name.c_str());
	auto ptr = group.get();
	ASSERT(ptr);

	for (auto& n : group->nodes) {
		node2group[n] = ptr;
	}

	for (auto& sink : group->request->sinks) {
		ASSERT(sink->flags & (voila::Node::kVariable | voila::Node::kAllExprs |
			voila::Node::kStmtAssign) );
		sinks2group[sink.get()] = ptr;
	}
}

bool
FuseGroups::group_overlaps(const std::unique_ptr<FuseGroup>& group, bool assert) const
{
	for (auto& n : group->nodes) {
		bool contained = contains_node(n);
		if (assert && !contained) {
			DUMP_VOILA(n);
			ASSERT(!contained);
		}

		if (contained) {
			return true;
		}
	}

	return false;
}

void
FuseGroups::add(std::unique_ptr<FuseGroup>&& group)
{
	ASSERT(group && group->request && group->nodes.size() > 0);

	// make sure new group does not overlap with existing ones
	bool overlaps = group_overlaps(group, true);
	ASSERT(!overlaps);

	size_t new_size = node2group.size() + group->nodes.size();

	node2group.reserve(new_size);

	add_nodes_to_map(group);

	group->request->set_analysis_from_fuse_group(*group);

	// own the object
	groups.emplace_back(std::move(group));
}

FuseGroups::~FuseGroups()
{

}



IJitPass::IJitPass(excalibur::Context& sys_context, QueryConfig* config,
	voila::Context& voila_context,
	const std::shared_ptr<IBudgetManager>& budget_manager,
	FuseGroups* fuse_groups)
 : fuse_groups(fuse_groups), config(config), voila_context(voila_context),
	sys_context(sys_context), budget_manager(budget_manager)
{
	ASSERT(fuse_groups);
}

bool
IJitPass::can_inline_constant(const std::shared_ptr<Constant>& e) const
{
	auto type = get_type1(voila_context, e.get());
	return type->is_integer();
}


JitPreparePass::JitPreparePass(JitPrepareExpressionGraphPass& expr_graph,
	excalibur::Context& sys_context, QueryConfig* config,
	voila::Context& voila_context,
	const std::shared_ptr<IBudgetManager>& budget_manager,
	FuseGroups* fuse_groups)
 : IJitPass(sys_context, config, voila_context, budget_manager, fuse_groups),
	expr_graph(expr_graph)
{
	if (config->compilation_flavor() == QueryConfig::CompilationFlavor::kRandom) {
		std::seed_seq seed { config->compilation_strategy_seed() };
		random_engine = std::make_unique<std::mt19937>(seed);
	}
}

void
JitPreparePass::_flush_groups_create_request(CompileRequest& request, const Expr& pe,
	const Answer& answer)
{
	if (!pe) {
		return;
	}

	// a non-picked node
	if (answer.picked_nodes.find(pe) == answer.picked_nodes.end()) {
		request.add_source(pe);
		// abort, because we left the picked graph
		return;
	}

	// is used by non-picked node -> sink
	auto function = std::dynamic_pointer_cast<Function>(pe);
	if (function && is_jittable_func_expr(*function)) {
		for (auto& edge : expr_graph.all_input_edges) {
			auto& src = edge.source;
			auto& dst = edge.dest;

			if (dst != pe.get()) {
				// wrong edge, skip
				continue;
			}

			if (answer.picked_node_ptrs.find((Expression*)src) != answer.picked_node_ptrs.end()) {
				// internally used, skip
				continue;
			}

			// we are an output/sink
			ASSERT(!fuse_groups->contains_node(pe.get()));
			request.add_sink(pe);
			break;
		}
	}

	// recurse on
	if (function) {
		if (function->data_structure) {
			request.add_struct(pe->data_structure);
		}

		if (function->predicate) {
			_flush_groups_create_request(request, function->predicate, answer);
		}

		for (auto& arg : function->args) {
			_flush_groups_create_request(request, arg, answer);
		}
	}
}

void
JitPreparePass::flush_groups_create_request(CompileRequest& request,
	const Answer& answer)
{
	// find sources, sinks and data structures
	for (auto& some_node : answer.picked_nodes) {
		DBG_ASSERT(!fuse_groups->contains_node(some_node.get()));
		_flush_groups_create_request(request, some_node, answer);
	}

}

void
JitPreparePass::flush_groups_for_predicate(Answer& answer,
	std::unordered_set<Expression*>& discovered,
	std::unordered_set<Expression*>& temporary_marked,
	const Expr& start_node, size_t depth)
{
	LOG_TRACE("flush_groups_for_predicate: %p", start_node.get());

	const auto& start_ptr = start_node.get();
	ASSERT(!fuse_groups->contains_node(start_ptr));

	ASSERT(temporary_marked.find(start_ptr) == temporary_marked.end() &&
		"Graph has cycle");
	temporary_marked.insert(start_ptr);

	for (auto& edge : input_edges) {
		ASSERT(edge.source.get() != edge.dest.get() &&
			"No self edges");

		// find all edges with our start_node
		if (edge.source != start_node) {
			continue;
		}

		auto& other_node = edge.dest;
		const auto& other_ptr = other_node.get(); 

		if (!is_jittable_node(other_node)) {
			continue;
		}

		// skip already discovered nodes
		if (discovered.find(other_ptr) != discovered.end()) {
			continue;
		}

		if (fuse_groups && fuse_groups->contains_node(other_node.get())) {
			continue;
		}

		discovered.insert(other_ptr);

		DBG_ASSERT(!fuse_groups->contains_node(other_node.get()));
		answer.add(other_node);
		DUMP_VOILA(other_node);

		flush_groups_for_predicate(answer, discovered, temporary_marked,
			other_node, depth+1);
	}

	temporary_marked.erase(start_node.get());
}

void
JitPreparePass::flush_groups()
{
	// skip empty
	if (nodes.empty()) {
		LOG_DEBUG("flush_groups: empty, skip");
		clear();
		return;
	}

	LOG_TRACE("flush_groups");

	std::unordered_map<Expr, size_t> in_degrees;
	for (auto& edge : input_edges) {
		in_degrees[edge.dest]++;
	}

	std::vector<Answer> answers;
	std::unordered_set<Expression*> discovered;
	std::unordered_set<Expression*> temporary_marked;

	for (auto& predicate : predicates) {
		LOG_DEBUG("flush_groups: predicate = %p", predicate.get());
		ASSERT(predicate);
		DUMP_VOILA(predicate);


		// find all nodes connected to this predicate
		std::unordered_set<Expr> node_set;
		std::vector<std::pair<Expr, size_t>> node_vec;
		for (auto& edge : predicate_edges) {
			if (edge.dest.get() == predicate.get()) {
				if (node_set.find(edge.source) != node_set.end()) {
					continue;
				}

				auto in_degree_iter = in_degrees.find(edge.source);
				size_t in_degree = 0;
				if (in_degree_iter != in_degrees.end()) {
					in_degree = in_degree_iter->second;
				}

				node_set.insert(edge.source);
				node_vec.push_back({ edge.source, in_degree });
			}
		}
#if 0
		// try to order node_vec ascending by least in-degree
		// to get top-most expressions first
		std::sort(node_vec.begin(), node_vec.end(),
			[] (auto& a, auto& b) { return a.second > b.second; });

		ASSERT(node_vec.size() == node_set.size());
#endif
		// find components using DFS on input_edges
		Answer answer;
		for (auto& start_node : node_set) {
			if (discovered.find(start_node.get()) != discovered.end()) {
				// skip already seen nodes
				continue;
			}
			if (!is_jittable_node(start_node)) {
				continue;
			}
			if (fuse_groups && fuse_groups->contains_node(start_node.get())) {
				continue;
			}

			discovered.insert(start_node.get());

			LOG_DEBUG("new component");
			DBG_ASSERT(!fuse_groups->contains_node(start_node.get()));
			answer.add(start_node);
			DUMP_VOILA(start_node);

			flush_groups_for_predicate(answer, discovered,
				temporary_marked, start_node, 0);
		}

		LOG_DEBUG("JitPreparePass: picked_nodes: ");
		for (auto& n : answer.picked_nodes) {
			DUMP_VOILA(n);
		}

		if (!answer.empty()) {
			answers.emplace_back(std::move(answer));
		}
	}

	// answers contains the components of the graph
	// create CompileRequests
	for (auto& answer : answers) {
		if (answer.picked_nodes.size() <= 1) {
			// skip too small sets, they are handled as residuals later
			LOG_DEBUG("JitPreparePass: Skip <= 1");
			continue;
		}
#if 0
		for (auto& ptr : answer.picked_node_ptrs) {
			DBG_ASSERT(!fuse_groups->contains_node(ptr));
		}
#endif
		TRACE_WARN("JitPreparePass: Create FuseGroup");

		auto request = std::make_shared<CompileRequest>(sys_context,
			config, voila_context);

		request->budget_manager = budget_manager;
		request->flavor = flavor_spec;

		flush_groups_create_request(*request, answer);

		std::unordered_set<Node*> jit_nodes;
		jit_nodes.reserve(answer.picked_node_ptrs.size());
		for (auto& ptr : answer.picked_node_ptrs) {
			DBG_ASSERT(!fuse_groups->contains_node(ptr));
			jit_nodes.insert(ptr);
		}

		request->dump();
		request->debug_validate();


		fuse_groups->add(std::make_unique<FuseGroup>(std::move(request),
			std::move(jit_nodes), nullptr));
#if 0
		LOG_WARN("Todo: skipping other FuseGroups. Remove this");
		return;
#endif
	}

	clear();
}

void
JitPreparePass::on_expression(const std::shared_ptr<Function>& e)
{
	if (fuse_groups->contains_node(e.get())) {
		return;
	}

	if (get_rand_bool(false)) {
		return;
	}

	if (is_jittable_func_expr(*e)) {
		nodes.insert(e);

		if (e->predicate) {
			add_predicate_edge(e, e->predicate);
		}

		for (auto& arg : e->args) {
			add_input_edge(e, arg);
		}
	}

	VoilaTreePass::on_expression(e);
}

void
JitPreparePass::handle(const Stmt& s)
{
	LOG_DEBUG("JitPreparePass: handle(stmt)");
	DUMP_VOILA(s);

	if (!std::dynamic_pointer_cast<Effect>(s) || get_rand_bool(false)) {
		// s->dump();
		flush_groups();
	} else {
		LOG_DEBUG("JitPreparePass: add another Effect");
		DUMP_VOILA(s);
	}

	VoilaTreePass::handle(s);
}
