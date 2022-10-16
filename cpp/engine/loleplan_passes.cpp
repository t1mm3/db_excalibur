#include "loleplan_passes.hpp"
#include "engine/voila/voila.hpp"
#include "engine/voila/statement_identifier.hpp"
#include "engine/lolepops/voila_lolepop.hpp"

using namespace engine;
using Lolepop = engine::lolepop::Lolepop;
using VoilaLolepop = lolepop::VoilaLolepop;
using Expr = engine::voila::Expr;
using Stmt = voila::Stmt;

bool
LoleplanPass::is_non_voila_lolepop(Lolepop& e)
{
	return !dynamic_cast<lolepop::VoilaLolepop*>(&e);
}
	
void
LoleplanPass::operator()(const std::shared_ptr<Lolepop>& first_op)
{
}



void
PrinterPass::operator()(const std::shared_ptr<Lolepop>& first_op)
{
	if (LOG_WILL_LOG(DEBUG)) {
		apply_source_to_sink(first_op, [&] (auto& op) {
			auto op_name = op->name.c_str();
			auto voila = std::dynamic_pointer_cast<VoilaLolepop>(op);
			ASSERT(voila.get() && "Todo: Support mixes of VOILA and non-VOILA ops")
			if (!voila->is_dummy_op) {
				LOG_DEBUG("Printer: Operator '%s'", op_name);
				voila->voila_block->dump();
			}
		});
	}
}


namespace engine::voila {

struct LocationVoilaPass : VoilaTreePass {
	LocationVoilaPass(voila::Context& context, size_t operator_id)
	 : context(context), tracker(operator_id) {

	}

	void handle(const std::vector<Stmt>& stmts) final {
		tracker.enter();
		location_stack.push_back(nullptr);
		for (size_t i=0; i<stmts.size(); i++) {
			tracker.update(i);

			location_stack[location_stack.size()-1] =
				std::make_shared<StatementIdentifier>(tracker.get_identifier());
			handle(stmts[i]);
		}
		location_stack.pop_back();
		tracker.leave();
	}

	void handle(const Stmt& s) final {
		tag_node(s.get());
		VoilaTreePass::handle(s);
	}

	void handle(const Expr& s) final {
		tag_node(s.get());
		VoilaTreePass::handle(s);
	}

	void tag_node(Node* n) {
		auto& info = context.locations[n];

		ASSERT(!location_stack.empty());
		info.location = location_stack[location_stack.size()-1];
		info.range = tracker.get_curr_range();
	}

private:
	voila::Context& context;
	VoilaStatementTracker tracker;

	std::vector<std::shared_ptr<StatementIdentifier>> location_stack;
};

} /* engine::voila */

void
LocationTrackingPass::operator()(const std::shared_ptr<Lolepop>& first_op)
{
	size_t operator_id = 0;
	apply_source_to_sink(first_op, [&] (auto& op) {
		auto voila = std::dynamic_pointer_cast<VoilaLolepop>(op);
		if (voila && !voila->is_dummy_op) {
			engine::voila::LocationVoilaPass loc_pass(context, operator_id);

			loc_pass(voila->voila_block->statements);
		}

		operator_id++;
	});
}



bool
InlinePass::Context::try_replace_input(Expr& expr)
{
	if (expr->flags & voila::Node::kExprFunc) {
		auto fun = std::dynamic_pointer_cast<voila::Function>(expr);
		ASSERT(fun);

		if (fun->tag == voila::Function::kTGet) {
			ASSERT(fun->args.size() == 2);
			if (fun->args[0]->flags & voila::Node::kExprInput) {
				ASSERT(fun->args[1]->flags & voila::Node::kExprConst);
				auto idx_arg = std::dynamic_pointer_cast<voila::Constant>(fun->args[1]);
				ASSERT(idx_arg);
				int64_t idx = std::stoll(idx_arg->value);
				ASSERT(idx >= 0 && idx < (int64_t)input_datas.size());
				expr = input_datas[idx];
				return true;
			} else {
				ASSERT(false);
			}
		}
		return false;
	}
	if (expr->flags & (voila::Node::kExprInput | voila::Node::kExprInputPred)) {
		if (expr->flags & voila::Node::kExprInput) {
			engine::voila::ExprBuilder b;
			expr = b.tuple(input_pred, input_datas);
		} else if (expr->flags & voila::Node::kExprInputPred) {
			expr = input_pred;
		} else {
			ASSERT(false);
		}
		return true;
	}

	return false;
}



void
InlinePass::operator()(const std::shared_ptr<Lolepop>& first_op)
{
	build_chain(first_op);

	auto voila = std::dynamic_pointer_cast<VoilaLolepop>(first_op);
	ASSERT(voila.get() && "Todo: Support mixes of VOILA and non-VOILA ops");
	inline_op(0, nullptr);

	if (first_voila_op) {
		// last operator inlined into first one
		ASSERT(last_voila_op);

		LOG_DEBUG("InlinePass: first_voila_op = %s, flags %llu",
			first_voila_op->name.c_str(), first_voila_op->runtime_flags);
		LOG_DEBUG("InlinePass: last_voila_op = %s, flags %llu",
			last_voila_op->name.c_str(), last_voila_op->runtime_flags);

		first_voila_op->runtime_flags = last_voila_op->flags;
	}
}

std::vector<Stmt>&
InlinePass::inline_op(size_t op_idx, Context* context)
{
	auto& op = chain[op_idx];

	LOG_DEBUG("InlinePass: inline_op %p, %s", op, op->name.c_str());
	auto inline_count = op_inline_count[op]++;
	ASSERT(!inline_count && "Can only be inlined once");

	ASSERT(op->voila_block);

	auto& block = *op->voila_block;
	ASSERT(!block.statements.empty());

	if (context) {
		// rewrite input_data and input_predicate with context
		ReplacePass replace(*context);
		replace(block.statements);
	}

	inline_op_block(op_idx, context, block);

	return block.statements;
}

void
InlinePass::build_chain(const std::shared_ptr<Lolepop>& first_op)
{
	VoilaLolepop* prev = nullptr;
	apply_source_to_sink(first_op, [&] (auto& op) {
		auto voila = std::dynamic_pointer_cast<VoilaLolepop>(op);
		auto voila_ptr = voila.get();
		ASSERT(voila_ptr && "Todo: Support mixes of VOILA and non-VOILA ops");

		next_operator.insert({prev, voila_ptr});
		chain.push_back(voila_ptr);

		last_voila_op = voila_ptr;
		if (!first_voila_op) {
			first_voila_op = voila_ptr;
		}
		prev = voila_ptr;
	});

	next_operator.insert({prev, nullptr});
}

voila::Expr
InlinePass::copy_expr(voila::BaseBuilder& builder, const voila::Expr& expr,
	const voila::Expr& pred, bool input_predicate, const std::string& dbg_name)
{
	if (auto read_var = dynamic_cast<voila::ReadVariable*>(expr.get())) {
		if (kComment) {
			builder.comment("'" + dbg_name + "' already variable");
		}
		auto copy = builder.ref(read_var->var, pred);
		copy_infos(copy.get(), expr.get());
		return copy;
	}
	auto var_ipred = builder.new_var(dbg_name);

	voila::Stmt assign;
	if (input_predicate) {
		assign = builder.new_assign(pred, var_ipred, expr, true, true);
	} else {
		// Create copy and change predicate
		auto clone = expr->clone();
		ASSERT(input_predicate || pred);
		clone->predicate = pred;

		// type clone
		copy_infos(clone.get(), expr.get());

		assign = builder.new_assign(pred, var_ipred, clone, true, true);
	}

	auto copy = builder.ref(var_ipred, pred);

	// type statement
	copy_infos(assign.get(), expr.get());
	// type expression
	copy_infos(copy.get(), expr.get());
	// type variable
	copy_infos(var_ipred.get(), expr.get());
	return copy;
}

void
InlinePass::inline_op_block(size_t op_idx, Context* context, voila::Block& block)
{
	auto& stmts = block.statements;

	// traverse block and replace EMIT with following operator code
	for (size_t i=0; i<block.statements.size(); i++) {
		auto& b = stmts[i];

		if (auto emit = std::dynamic_pointer_cast<voila::Emit>(b)) {
			auto next_idx = op_idx+1;
			if (next_idx < chain.size() &&
					emit_output.find(emit.get()) == emit_output.end()) {
				auto next_op = chain[next_idx];
				auto this_op = chain[op_idx];
				std::vector<voila::Expr> emit_datas;

				std::vector<Stmt> new_stmts;
				voila::DirectStatementBuilder b(new_stmts);

				if (emit->value->flags & voila::Node::kExprFunc) {
					auto fun = std::dynamic_pointer_cast<voila::Function>(emit->value);
					ASSERT(fun);

					if (fun->tag == voila::Function::kTuple) {
						emit_datas = fun->args;
					} else {
						ASSERT(false);
					}
				} else {
					ASSERT(!(emit->value->flags & voila::Node::kExprInput));

					ASSERT(context);
					emit_datas = context->input_datas;
				}

				ASSERT(emit->predicate && emit->value);
				ASSERT(!(emit->predicate->flags & voila::Node::kExprInputPred));
				ASSERT(!(emit->value->flags & voila::Node::kExprInput));

				std::vector<std::vector<engine::voila::Context::NodeInfo>> node_infos;
				node_infos.reserve(emit_datas.size());

				for (auto& e : emit_datas) {
					const auto node_info_it = voila_context.infos.find(e.get());
					ASSERT(node_info_it != voila_context.infos.end());
					node_infos.push_back(node_info_it->second);
				}

				// Handle EMIT
				voila::Expr ipred;
				{
					voila::DirectStatementBuilder b(new_stmts);

					if (kReassignOnEmit) {
						ipred = copy_expr(b, emit->predicate, nullptr, true,
							this_op->name + "_ipred");
					} else {
						ipred = emit->predicate;
					}

					if (kReassignOnEmit) {
						std::vector<Expr> new_emit_datas;
						new_emit_datas.reserve(emit_datas.size());
						size_t idx = 0;
						for (auto& expr : emit_datas) {
							new_emit_datas.emplace_back(copy_expr(b, expr, ipred, false,
								this_op->name + "_value" + std::to_string(idx)));
							idx++;
						}

						emit_datas = std::move(new_emit_datas);
					}


					if (kComment) {
						b.comment("<" + next_op->name + ">");
					}
				}

				ASSERT(ipred);


				Context subcontext {
					ipred ? ipred : emit->predicate,
					std::move(emit_datas),
					node_infos };

				// Replace Input and InputPredicate
				for (auto&& s : inline_op(next_idx, &subcontext)) {
					new_stmts.emplace_back(s);
				}

				if (kComment) {
					voila::DirectStatementBuilder b(new_stmts);
						b.comment("</" + next_op->name + ">");
				}

				// replace EMIT with new statements
				stmts[i] = new_stmts[0];
				for (size_t k=1; k<new_stmts.size(); k++) {
					stmts.insert(stmts.begin() + i + k, new_stmts[k]);
				}

				LOG_DEBUG("InlinePass: inlined %p %s",
					next_op, next_op->name.c_str());

				// mark as dummy op
				next_op->is_dummy_op = true;
			} else {
				emit_output.insert(emit.get());
			}
		} else if (auto subblock = std::dynamic_pointer_cast<voila::Block>(b)) {
			inline_op_block(op_idx, context, *subblock);
		}
	}
}