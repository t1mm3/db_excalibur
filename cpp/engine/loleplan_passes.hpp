#pragma once

#include "lolepops/lolepop.hpp"
#include "lolepops/voila_lolepop.hpp"
#include "engine/voila/voila.hpp"

#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace engine {

struct LoleplanPass {
	using Lolepop = engine::lolepop::Lolepop;

	virtual void operator()(const std::shared_ptr<Lolepop>& first_op);


	template<typename S, typename T>
	static void
	apply_source_to_sink(S& op, const T& fun)
	{
		if (!op) {
			return;
		}

		apply_source_to_sink<S, T>(op->child, fun);

		fun(op);
	}

	template<typename S, typename T>
	static void
	apply_source_to_sink_with_id(S& op, const T& fun)
	{
		size_t id = 0;
		apply_source_to_sink<S>(op, [&] (auto op) {
			fun(op, id);
			id++;
		});
	}


	template<typename S, typename T>
	static void
	apply_source_to_sink_pre_early_out(S& op, const T& fun)
	{
		if (!op) {
			return;
		}

		if (fun(op)) {
			apply_source_to_sink<S, T>(op->child, fun);
		}
	}

	static bool is_non_voila_lolepop(Lolepop& e);

	template<typename T>
	static T
	skip_first_non_voila_ops(const T& first_op)
	{
		if (is_non_voila_lolepop(*first_op)) {
			return first_op->child;
		}
		return first_op;
	}


};

struct PrinterPass : LoleplanPass {
	using VoilaLolepop = lolepop::VoilaLolepop;
	using Expr = engine::voila::Expr;
	using Stmt = voila::Stmt;

	void operator()(const std::shared_ptr<Lolepop>& first_op) final;
};

struct LocationTrackingPass : LoleplanPass {
	using VoilaLolepop = lolepop::VoilaLolepop;
	using Expr = engine::voila::Expr;
	using Stmt = voila::Stmt;

	LocationTrackingPass(voila::Context& context) : context(context) {}

	void operator()(const std::shared_ptr<Lolepop>& first_op) final;

private:
	voila::Context& context;
};

struct InlinePass : LoleplanPass {
	using VoilaLolepop = lolepop::VoilaLolepop;
	using Expr = engine::voila::Expr;
	using Stmt = voila::Stmt;

	static const bool kReassignOnEmit = true;
	static const bool kComment = false; 

	void operator()(const std::shared_ptr<Lolepop>& first_op) final;

	InlinePass(voila::Context& voila_context) : voila_context(voila_context) {}

private:
	voila::Context& voila_context;
	std::unordered_map<VoilaLolepop*, VoilaLolepop*> next_operator;
	VoilaLolepop* first_voila_op = nullptr;
	VoilaLolepop* last_voila_op = nullptr;

	std::vector<VoilaLolepop*> chain;

	void build_chain(const std::shared_ptr<Lolepop>& first_op);

	struct Context {
		Expr input_pred;

		std::vector<Expr> input_datas;
		std::vector<std::vector<engine::voila::Context::NodeInfo>> input_infos;

		bool try_replace_input(Expr& expr);
	};

	struct ReplacePass : voila::VoilaTreeRewritePass {
		Context& context;

		ReplacePass(Context& context) : context(context) {}

		void handle(voila::Expr& s) final {
			if (!s) {
				return;
			}

			context.try_replace_input(s);
			VoilaTreeRewritePass::handle(s);
		}
	};

	std::unordered_set<void*> emit_output;

	std::vector<Stmt>& inline_op(size_t op_idx, Context* context);

	void copy_infos(voila::Node* dest, voila::Node* src) {
		if (dest == src) {
			return;
		}
		const auto node_info_it = voila_context.infos.find(src);
		ASSERT(node_info_it != voila_context.infos.end());
		voila_context.infos[dest] = node_info_it->second;

		const auto node_loc_it = voila_context.locations.find(src);
		ASSERT(node_loc_it != voila_context.locations.end());
		voila_context.locations[dest] = node_loc_it->second;
	}

	voila::Expr copy_expr(voila::BaseBuilder& builder, const voila::Expr& expr,
			const voila::Expr& pred, bool input_predicate,
			const std::string& dbg_name);
		

	void inline_op_block(size_t op_idx, Context* context, voila::Block& block);

	std::unordered_map<VoilaLolepop*, size_t> op_inline_count;
};

} /* engine */