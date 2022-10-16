#pragma once

#include "jit_prepare_pass.hpp"
#include "engine/voila/statement_identifier.hpp"

#include "system/system.hpp"

namespace engine::adaptive {
struct JitStatementFragmentOpts;
struct JitStatementFragmentId;
struct InputPlanProfile;
} /* engine::adaptive */

namespace engine::voila {
struct FlavorSpec;
struct VoilaStatementTracker;
} /* engine::voila */


namespace engine::voila {

struct Candidate {
	std::vector<Stmt> stmts;
	size_t num_nodes = 0;
	std::shared_ptr<CompileRequest> request;
	std::unordered_set<Node*> node_ptrs;
	const std::vector<Stmt>* scope;

	int64_t start_idx = -1;
	int64_t end_idx = -1;

	void reserve(int64_t num) {
		stmts.reserve(num);
	}

	bool empty() const {
		return stmts.empty();
	}

	Candidate() {
		clear();
	}

	void clear() {
		scope = nullptr;
		stmts.clear();
		num_nodes = 0;
		request.reset();
		node_ptrs.clear();

		start_idx = -1;
		end_idx = -1;
	}
};

struct StatementFragmentSelectionComparator {
	virtual bool is_better_than_b(const Candidate& a, const Candidate& b) {
		return a.num_nodes > b.num_nodes;
	}

	virtual ~StatementFragmentSelectionComparator() = default;
};

struct StatementFragmentSelection {
	enum FragmentResult {
		kCannotCompile = 0,
		kSuccess = 1,
	};

	struct ConstructCandidateOpts {
		engine::adaptive::InputPlanProfile* input_plan_profile = nullptr;

		const engine::adaptive::JitStatementFragmentOpts* jit_opts = nullptr;
	};

	using ConstructCandidateOpts = StatementFragmentSelection::ConstructCandidateOpts;

	FragmentResult construct_candidate(int64_t start_idx, int64_t end_idx, int64_t& out_skip_first,
		Candidate& curr_candidate, Candidate& best_candidate,
		int64_t num, const std::vector<Stmt>& stmts,
		const ConstructCandidateOpts& opts);

	bool candidate_create_request(Candidate& best_candidate, bool just_try,
		const std::shared_ptr<FlavorSpec>& flavor_spec,
		const char* dbg_path) const;

	StatementFragmentSelection(IJitPass& jit_prepare_pass,
		StatementFragmentSelectionComparator* comparator = nullptr);
	~StatementFragmentSelection();

	static Type* get_var_type(Variable* var, const voila::Context& voila_context);

private:
	IJitPass& jit_prepare_pass;
	voila::Context& voila_context;
	FuseGroups* fuse_groups;

	StatementFragmentSelectionComparator* comparator;
	std::unique_ptr<StatementFragmentSelectionComparator> default_comparator;

	FragmentResult try_update_best(Candidate& best, Candidate&& cand,
		const ConstructCandidateOpts& opts) const;
};

struct StatementFragmentSelectionPass : IJitPass, VoilaTreePass {
	void operator()(const std::vector<Stmt>& stmts) override;

	StatementFragmentSelectionPass(excalibur::Context& sys_context,
		QueryConfig* config,
		voila::Context& voila_context,
		const std::shared_ptr<IBudgetManager>& budget_manager,
		FuseGroups* fuse_groups,
		const adaptive::JitStatementFragmentId* given_fragment,
		size_t operator_id);

	~StatementFragmentSelectionPass();

	bool found_given_fragment() const { return !!num_given_found; }

	std::shared_ptr<FlavorSpec> flavor_spec;

private:
	StatementFragmentSelection selection;
	const adaptive::JitStatementFragmentId* given_fragment;
	size_t num_given_found = 0;

	std::unique_ptr<VoilaStatementTracker> statement_tracker;

	void handle(const std::vector<Stmt>& stmts) override;
	void handle(const Stmt& s) override { VoilaTreePass::handle(s); }

	void max_fragment(const std::vector<Stmt>& stmts);

	void commit_candidate(Candidate&& best_candidate);
};

} /* engine::voila */
