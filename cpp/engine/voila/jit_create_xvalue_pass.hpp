#pragma once

namespace engine::voila {

struct CreateXValuePass : VoilaTreePass {
	CreateXValuePass(CodeBlockBuilder& builder)
	 : builder(builder),
		voila_global_context(builder.voila_global_context),
		voila_context(builder.voila_context),
		block(builder.block) {

	}

	struct VisitedNodeInfo {
		bool copy;
	};
	std::unordered_map<void*, XValue*> visited;
	std::unordered_map<void*, VisitedNodeInfo> infos;


private:
	XValue** inject_result = nullptr;
	CodeBlockBuilder& builder;
	GlobalCodeContext& voila_global_context;
	Context& voila_context;
	CodeBlock& block;

	static constexpr uint32_t kHandleNodeCannotModifyInput = 1; 

	template<typename T>
	void handle_node(Node* e, XValue::Tag tag, const T& f,
			const char* dbg_name, uint32_t flags = 0)
	{
		const bool cannot_modify_input = flags & kHandleNodeCannotModifyInput;
		bool copy = false;

		// try to avoid copying values by allowing to inject the result pointer
		// directly
		XValue* r;
		if (inject_result) {
			r = *inject_result;
			r->tag = tag;

			copy = cannot_modify_input;
			inject_result = nullptr;
		} else {
			r = block.newXValue(tag);
		}

		if (r && dbg_name) {
			r->dbg_name = dbg_name;
		}

		// e.g. go to children
		f(r);

		visited[e] = r;
		infos[e] = VisitedNodeInfo { copy };
	}

	void on_statement(const std::shared_ptr<Assign>& b) final {
		auto assign = [&] (auto& map, bool global) {
			auto var_ptr = b->var.get();
			auto it = map.find(var_ptr);

			TRACE("Build Assign %s to %p '%s'",
				it == map.end() ? "initial" : "reassign",
				var_ptr, b->var->dbg_name.c_str());

			XValue* dest = nullptr;
			if (it == map.end()) {
				// first assignment, create variable
				const auto node_info_it = voila_context.infos.find(b->value.get());
				ASSERT(node_info_it != voila_context.infos.end() && "Must be typed");

				const auto& node_infos = node_info_it->second;
				ASSERT(node_infos.size() == 1);

				auto tag = XValue::Tag::kVector;

				if (node_infos[0].type->is_position()) {
					tag = XValue::Tag::kPosition;
				} else if (node_infos[0].type->is_predicate()) {
					tag = XValue::Tag::kPredicate;
				}

				if (global) {
					dest = voila_global_context.newXValue(tag);
				} else {
					dest = block.newXValue(tag);
				}


				inject_result = &dest;
				VoilaTreePass::on_statement(b);
				inject_result = nullptr;

				ASSERT(dest);
				map.insert({var_ptr, dest});
			} else {
				// re-assignment, copy into variable
				dest = it->second;
				ASSERT(dest);

				inject_result = &dest;
				VoilaTreePass::on_statement(b);
				inject_result = nullptr;
			}
		};

		if (b->var->global) {
			assign(voila_global_context.globals, true);
		} else {
			assign(visited, false);
		}
	}



	void on_expression(const std::shared_ptr<Input>& e) final {
		handle_node(e.get(), XValue::Tag::kTuple,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Input"
		);
	}

	void on_expression(const std::shared_ptr<InputPredicate>& e) final {
		handle_node(e.get(), XValue::Tag::kTuple,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"InputPredicate",
			kHandleNodeCannotModifyInput
		);
	}

	void on_expression(const std::shared_ptr<ReadVariable>& e) final {
		auto var_ptr = e->var.get();

		TRACE("Build ReadVariable %p '%s' global=%d",
			var_ptr, e->var->dbg_name.c_str(), e->var->global);

		auto read_from_map = [&] (const auto& map) {
			auto it = map.find(var_ptr);
			ASSERT(it != map.end() && "Variable must have been assigned");
			ASSERT(it->second && "Must have stored pointer");

			handle_node(e.get(), it->second->tag,
				[&] (auto& result) {
					VoilaTreePass::on_expression(e);
				},
				"ReadVariable",
				kHandleNodeCannotModifyInput
			);
		};

		if (e->var->global) {
			read_from_map(voila_global_context.globals);
		} else {
			read_from_map(visited);
		}
	}

	void on_expression(const std::shared_ptr<Function>& e) final {
		ASSERT(false && "todo: tag depends on function");

		handle_node(e.get(), XValue::Tag::kTuple,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Function"
		);
	}

	void on_expression(const std::shared_ptr<Constant>& e) final {
		handle_node(e.get(), XValue::Tag::kVector,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Constant"
		);
	}

	void on_expression(const std::shared_ptr<GetScanPos>& e) final {
		handle_node(e.get(), XValue::Tag::kPosition,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"GetScanPos"
		);
	}

	void on_expression(const std::shared_ptr<Scan>& e) final {
		handle_node(e.get(), XValue::Tag::kVector,
			[&] (auto& result) {
				VoilaTreePass::on_expression(e);
			},
			"Scan"
		);
	}
};

} /* engine::voila */