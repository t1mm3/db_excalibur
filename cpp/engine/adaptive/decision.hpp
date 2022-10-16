#pragma once

#include <string>
#include <memory>
#include <vector>
#include <iostream>

#include "system/system.hpp"

struct BudgetUser;
struct QueryConfig;

namespace engine {
struct Stream;
} /* engine */

namespace engine::voila {
struct CodeBlockBuilder;
struct FuseGroups;
struct FuseGroup;
struct CompileRequest;
struct Statement;
struct FlavorSpec;
struct StatementRange;
} /* engine::voila */

namespace engine::protoplan {
struct PlanOp;
} /* engine::protoplan */

namespace engine::adaptive {
struct SetVectorKey;
}

namespace engine::adaptive {

struct DecisionContext {
	virtual ~DecisionContext() = default;
};

enum ConfigOption {
	kVectorSize,
	kCanFullEval,
	kBitScoreDivisor,
	kEnableSimdOpts,
};

struct IDecision {
	virtual void to_string(std::ostream& o) const = 0;

	virtual ~IDecision() = default;

	virtual double gowers_distance(size_t& count, const IDecision* o) const = 0;

protected:
	virtual void apply_protoplan_internal(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op) = 0;

	virtual void apply_passes_internal(
		DecisionContext* ctx,
		Stream& stream,
		BudgetUser* budget_user) = 0;

	virtual void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) = 0;

	virtual void apply_fragment_internal(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group) = 0;

	virtual bool get_config_long_internal(
		long& out,
		QueryConfig& config,
		ConfigOption key) const = 0;

	virtual bool get_config_bool_internal(
		bool& out,
		QueryConfig& config,
		ConfigOption key) const = 0;
};



struct DecisionState {
	uint64_t node_type = 0;

	std::shared_ptr<engine::voila::FlavorSpec> flavor_spec;

	int64_t destination_index = -1;
	int64_t source_index = -1;

	size_t stable_op_id = 0;

	std::shared_ptr<engine::adaptive::SetVectorKey> set_vector_key;

	std::shared_ptr<engine::voila::StatementRange> statement_range;

	static size_t gowers_distance_count();
	double gowers_distance_diff(const DecisionState& o) const;

	bool operator==(const DecisionState& o) const;
	size_t hash() const;
};



struct Decision : IDecision {
	typedef uint32_t Type;

	Decision(Type type, const std::string& dbg_name);
	virtual ~Decision();

	void apply_protoplan(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op);
	void apply_passes(
		DecisionContext* ctx,
		Stream& s,
		BudgetUser* budget_user);
	void apply_jit(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user);
	void apply_fragment(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group);
	bool get_config_long(
		long& out,
		QueryConfig& config,
		ConfigOption key) const;
	bool get_config_bool(
		bool& out,
		QueryConfig& config,
		ConfigOption key) const;

	const std::string& get_dbg_name() const { return dbg_name; }

	virtual void to_string(std::ostream& o) const override;

	Type get_type() const { return _type; }

	virtual std::unique_ptr<DecisionContext> make_decision_context();

	static size_t gowers_distance_count();
	double gowers_distance(size_t& count, const IDecision* o) const override;

	const DecisionState& get_decision_state() const {
		return _state;
	}

protected:
	virtual void apply_protoplan_internal(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op) override;

	virtual void apply_passes_internal(
		DecisionContext* ctx,
		Stream& stream,
		BudgetUser* budget_user) override;

	virtual void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) override;

	virtual void apply_fragment_internal(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group) override;

	virtual bool get_config_long_internal(
		long& out,
		QueryConfig& config,
		ConfigOption key) const override;

	virtual bool get_config_bool_internal(
		bool& out,
		QueryConfig& config,
		ConfigOption key) const override;

private:
	const Type _type;
	const std::string dbg_name;

protected:
	DecisionState _state;
};


struct Actions : IDecision {
	void apply_protoplan(
			DecisionContext* ctx,
			std::shared_ptr<engine::protoplan::PlanOp>& op) {
		return apply_protoplan_internal(ctx, op);
	}

	void apply_passes(DecisionContext* ctx,
			Stream& s,
			BudgetUser* budget_user) {
		apply_passes_internal(ctx, s, budget_user);
	}

	void apply_jit(
			DecisionContext* ctx,
			engine::voila::FuseGroups* out_fuse_groups,
			engine::voila::CodeBlockBuilder& s,
			const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
			size_t operator_id,
			BudgetUser* budget_user) {
		apply_jit_internal(ctx, out_fuse_groups, s, stmts, operator_id, budget_user);
	}

	void apply_fragment(
			DecisionContext* ctx,
			engine::voila::CompileRequest& request,
			engine::voila::FuseGroup* fuse_group) {
		apply_fragment_internal(ctx, request, fuse_group);
	}

	bool get_config_long(
			long& out,
			QueryConfig& config,
			ConfigOption key) const {
		return get_config_long_internal(out, config, key);
	}

	bool get_config_bool(
			bool& out,
			QueryConfig& config,
			ConfigOption key) const {
		return get_config_bool_internal(out, config, key);
	}

	size_t size() const {
		return 1 + (previous ? previous->size() : 0);
	}

	template<typename T>
	void for_each_bottomup(const T& f)
	{
		if (previous) {
			previous->for_each_bottomup<T>(f);
		}

		f(decision);
	}

	template<typename T>
	void for_each(const T& f)
	{
		for_each_bottomup<T>(f);
	}

	template<typename T>
	void for_each_bottomup(const T& f) const
	{
		if (previous) {
			previous->for_each_bottomup<T>(f);
		}

		f(decision);
	}

	template<typename T>
	void for_each(const T& f) const
	{
		for_each_bottomup<T>(f);
	}


	template<typename T>
	void _for_each_bottomup_with_index(const T& f, size_t& start_idx)
	{
		if (previous) {
			previous->_for_each_bottomup_with_index<T>(f, start_idx);
		}

		f(start_idx, decision);
		start_idx++;
	}

	template<typename T>
	void for_each_bottomup_with_index(const T& f)
	{
		size_t start_idx = 0;

		_for_each_bottomup_with_index<T>(f, start_idx);
	}



	size_t hash() const {
		size_t seed = 13;

		for_each_bottomup([&] (auto d) {
			seed ^= (size_t)d + 0x9e3779b9 + (seed << 6) + (seed >> 2);
		});
		return seed;
	}

	bool operator==(const Actions& o) const { return equals(o); }

	bool equals(const Actions& o) const {
#if 0
		LOG_WARN("Action '%s' == '%s'",
			to_string().c_str(), o.to_string().c_str());
		LOG_WARN("Decision %p == decision %p",
			decision, o.decision);
#endif
		if (decision != o.decision) {
			return false;
		}
#if 0
		LOG_WARN("previous %p == previous %p",
			previous, o.previous);
#endif
		if (!previous || !o.previous) {
			if (!previous && !o.previous) {
				return true;
			}
			return false;
		}
		return previous->equals(*o.previous);
	}

	Actions(Decision* d, Actions* previous = nullptr)
	 : decision(d), previous(previous) {}

	Actions() : decision(nullptr), previous(nullptr) {
	}

	void to_string(std::ostream& o) const final;
	std::string to_string() const;

	Decision* get_decision() const { return decision; }
	Actions* get_previous() const { return previous; }

	double gowers_distance(size_t& count, const IDecision* o) const override;

	double gowers_distance(std::vector<const Decision*>& tmp_actions1,
		std::vector<const Decision*>& tmp_actions2,
		size_t& count, const IDecision* o) const;

private:
	Decision* decision;
	Actions* previous;

	void apply_protoplan_internal(
		DecisionContext* ctx,
		std::shared_ptr<engine::protoplan::PlanOp>& op) final;

	void apply_passes_internal(
		DecisionContext* ctx,
		Stream& stream,
		BudgetUser* budget_user) final;

	void apply_jit_internal(
		DecisionContext* ctx,
		engine::voila::FuseGroups* out_fuse_groups,
		engine::voila::CodeBlockBuilder& s,
		const std::vector<std::shared_ptr<engine::voila::Statement>>& stmts,
		size_t operator_id,
		BudgetUser* budget_user) final;

	void apply_fragment_internal(
		DecisionContext* ctx,
		engine::voila::CompileRequest& request,
		engine::voila::FuseGroup* fuse_group) final;

	bool get_config_long_internal(
		long& out,
		QueryConfig& config,
		ConfigOption key) const final;

	bool get_config_bool_internal(
		bool& out,
		QueryConfig& config,
		ConfigOption key) const final;

	friend struct ActionsContext;
};

struct ActionsContext : DecisionContext {
	static std::shared_ptr<ActionsContext> make_from_actions(Actions*);

private:
	std::vector<std::unique_ptr<DecisionContext>> contexts;

	friend struct Actions;
};


struct DecisionConfigGetter {
	static bool get_long(long& out, Actions* decision, QueryConfig& config,
		ConfigOption key);
	static long get_long_assert(Actions* decision, QueryConfig& config,
		ConfigOption key);

	static bool get_bool(bool& out, Actions* decision, QueryConfig& config,
		ConfigOption key);
	static bool get_bool_assert(Actions* decision, QueryConfig& config,
		ConfigOption key);
};


} /* engine::adaptive */


namespace std {
	template <>
	struct hash<engine::adaptive::Actions>
	{
		std::size_t operator()(const engine::adaptive::Actions& k) const
		{
			return k.hash();
		}
	};

	template <>
	struct hash<engine::adaptive::DecisionState>
	{
		std::size_t operator()(const engine::adaptive::DecisionState& k) const
		{
			using std::size_t;

			return k.hash();
		}
	};
} /* std */