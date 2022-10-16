#include "var_life_time_pass.hpp"
#include "voila/voila.hpp"
#include "lolepops/voila_lolepop.hpp"

#include <unordered_set>
#include <memory>

using namespace engine;

struct VoilaVarUsage : voila::VoilaTreePass {
	struct Info {
		std::unordered_set<voila::Variable*> read;
		std::unordered_set<voila::Variable*> written;

		bool is_read(voila::Variable* v) const {
			return read.find(v) != read.end();
		}

		bool is_written(voila::Variable* v) const {
			return written.find(v) != written.end();
		}

		bool is_used(voila::Variable* v) const {
			return is_read(v) || is_written(v);
		}
	};
	std::unordered_map<voila::Statement*, Info> infos;

private:
	struct StmtState {
		std::unordered_set<voila::Variable*> read;
		std::unordered_set<voila::Variable*> written;

		std::unique_ptr<StmtState> prev;

		StmtState(std::unique_ptr<StmtState>&& prev) : prev(std::move(prev)) {}
	};

	std::unique_ptr<StmtState> current_state;

	void handle(const voila::Stmt& s) final {
		current_state = std::make_unique<StmtState>(std::move(current_state));
		VoilaTreePass::handle(s);

		// update public info
		{
			auto& info = infos[s.get()];
			for (auto& v : current_state->read) { info.read.insert(v); }
			for (auto& v : current_state->written) { info.written.insert(v); }
		}

		// pop
		current_state = std::move(current_state->prev);
	}

	void access(const voila::Var& var, bool read, bool write) {
		StmtState* curr = current_state.get();
		while (curr) {
			LOG_TRACE("VoilaVarUsage: access: var %p ('%s'), state %p, read %d, write %d",
				var.get(), var->dbg_name.c_str(), curr, read, write);
			if (read) {
				curr->read.insert(var.get());
			}
			if (write) {
				curr->written.insert(var.get());
			}

			curr = curr->prev.get();
		}
	}

	void on_statement(const std::shared_ptr<voila::Assign>& b) final {
		access(b->var, false, true);

		voila::VoilaTreePass::on_statement(b);
	}

	void on_expression(const std::shared_ptr<voila::ReadVariable>& e) final {
		access(e->var, true, false);

		voila::VoilaTreePass::on_expression(e);
	}

};


struct VoilaVarLifeTimePass : voila::VoilaTreePass {
private:
	VoilaVarUsage var_usage;

	struct State {
		std::unique_ptr<State> prev;
		std::unordered_set<voila::Var> created_variables;

		State(std::unique_ptr<State>&& prev)
		 : prev(std::move(prev))
		{
			created_variables.reserve(16);
		}
	};

	std::unique_ptr<State> current_state;

	void handle(const voila::Stmt& s) final { VoilaTreePass::handle(s); }
	void handle(const voila::Expr& s) final { VoilaTreePass::handle(s);	}

	void on_statement(const std::shared_ptr<voila::Assign>& b) final {
		ASSERT(current_state);

		// is used in outer scope?
		State* curr = current_state.get();
		while (curr) {
			if (curr->created_variables.find(b->var) !=
					curr->created_variables.end()) {
				break;
			}

			curr = curr->prev.get();
		}

		if (!curr) {
			current_state->created_variables.insert(b->var);
		}


		VoilaTreePass::on_statement(b);
	}

	void on_statement(const std::shared_ptr<voila::Block>& b) final {
		scope(b, b->statements);
	}
	void on_statement(const std::shared_ptr<voila::Loop>& b) final {
		scope(b, b->statements);
	}

	void scope(const voila::Stmt& s, const std::vector<voila::Stmt>& stmts) {
		if (s && s->predicate) {
			handle(s->predicate);
		}

		current_state = std::make_unique<State>(std::move(current_state));

		LOG_TRACE("VoilaVarLifeTimePass: begin scope %p", current_state.get());

		for (size_t i=0; i<stmts.size(); i++) {
			handle(stmts[i]);
		}

		ASSERT(current_state);
		LOG_TRACE("VoilaVarLifeTimePass: end scope %p", current_state.get());

		// declare all locally created variables as dead
		if (s) {
			s->dump();

			const auto& infos = var_usage.infos;
			for (auto& var : current_state->created_variables) {
				int64_t found_pos = -1;
				ASSERT(stmts.size());
				for (int64_t i=(int64_t)stmts.size()-1; i>=0; i--) {
					auto it = infos.find(stmts[i].get());
					ASSERT(it != infos.end());

					if (it->second.is_used(var.get())) {
						found_pos = i;
						stmts[i]->dump();
						LOG_TRACE("VoilaVarLifeTimePass: DEAD @ %lld: %s",
							found_pos, var->dbg_name.c_str());
						break;
					}
				}

#ifdef IS_DEBUG_BUILD
				// Should not be used afterwards
				for (int64_t i=found_pos+1; i<(int64_t)stmts.size(); i++) {
					auto it = infos.find(stmts[i].get());
					ASSERT(it != infos.end());
					ASSERT(!it->second.is_used(var.get()));
				}
#endif

				if (found_pos < 0) {
					LOG_TRACE("VoilaVarLifeTimePass: DEAD: %s",
						var->dbg_name.c_str());
					s->var_dead_after.insert(var);
				} else {
					ASSERT(found_pos < stmts.size());
					stmts[found_pos]->var_dead_after.insert(var);
				}
			}
		}

		current_state = std::move(current_state->prev);
	}

public:
	virtual void operator()(const std::vector<voila::Stmt>& stmts) {
		var_usage(stmts);

		scope(nullptr, stmts);
		ASSERT(!current_state);
		finalize();
	}
};



void
VarLifeTimePass::lolepop(std::shared_ptr<lolepop::Lolepop>& op)
{
	LOG_TRACE("VarLifeTimePass: lolepop %p ('%s')",
		op.get(), op->name.c_str());

	if (op->is_dummy_op) {
		return;
	}

	auto voila_op = std::dynamic_pointer_cast<lolepop::VoilaLolepop>(op);
	ASSERT(voila_op && voila_op->voila_block);

	VoilaVarLifeTimePass pass;
	pass(voila_op->voila_block->statements);
}