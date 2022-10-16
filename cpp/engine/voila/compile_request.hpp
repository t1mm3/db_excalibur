#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

struct IBudgetManager;

namespace excalibur {
struct Context;
}

struct QueryConfig;
struct Query;

namespace engine::voila {

struct Node;
struct Context;
struct FlavorSpec;
struct FuseGroup;

struct ComplexityMetric {
	size_t num_inputs = 0;
	size_t num_outputs = 0;
	size_t num_structs = 0;
};

struct SignatureCache;

struct CompileRequest {
	typedef std::shared_ptr<Node> NodePtr;

	// can be either regular expression or predicate
	std::vector<NodePtr> sources;
	std::vector<NodePtr> sinks;
	std::vector<NodePtr> structs;
	std::vector<NodePtr> statements;
	std::vector<NodePtr> expressions;

	enum Type { kSource, kSink, kStruct, kStatement, kExpression };

	bool add_source(const NodePtr& ptr) {
		return add_value(ptr, kSource);
	}
	bool add_sink(const NodePtr& ptr) {
		return add_value(ptr, kSink);
	}
	bool add_struct(const NodePtr& ptr) {
		return add_value(ptr, kStruct);
	}
	bool add_statement(const NodePtr& ptr) {
		return add_value(ptr, kStatement);
	}
	bool add_expression(const NodePtr& ptr) {
		return add_value(ptr, kExpression);
	}

	bool add_value(const NodePtr& ptr, Type type);
	bool contains_value(int64_t& out_index, const NodePtr& ptr, Type type) const;

	bool contains_source(const NodePtr& ptr) const {
		int64_t index;
		return contains_value(index, ptr, kSource);
	}

	bool contains_sink(const NodePtr& ptr) const {
		int64_t index;
		return contains_value(index, ptr, kSink);
	}

	int64_t get_source_index(const NodePtr& ptr) const {
		int64_t index;
		contains_value(index, ptr, kSource);
		return index;
	}

	// The JIT-ted fragment relies on deterministic ordering of
	// sources, sinks and structs.
	// This function intends to bring all 3 into determinstic order.
	void canonicalize();


	std::unordered_map<NodePtr, int64_t> source_set;
	std::unordered_map<NodePtr, int64_t> sink_set;
	std::unordered_map<NodePtr, int64_t> struct_set;
	std::unordered_map<NodePtr, int64_t> statement_set;
	std::unordered_map<NodePtr, int64_t> expression_set;

	std::unique_ptr<voila::Context> context;

	bool asynchronous_compilation = false;
	bool can_be_cached = true;

	bool enable_optimizations = true;
	bool verify = true;
	bool paranoid = false;

	std::shared_ptr<FlavorSpec> flavor;

	// #nodes jitted, Set from set_analysis_from_fuse_group()
	int64_t complexity = 1;
	// SUM(bits), set from set_analysis_from_fuse_group()
	int64_t sum_bits = 0;

	ComplexityMetric get_complexity_metric() const {
		return ComplexityMetric { source_set.size(), sink_set.size(), struct_set.size() };
	}

	bool can_evaluate_speculatively() const;

	// Computes, if not cached, full signature (get_operation_signature() + flavor specs)
	const std::string& get_full_signature(Query* query);

	// Computes, if not cached, signature of the operations (signature of VOILA ops)
	const std::string& get_operation_signature(Query* query);

	CompileRequest(excalibur::Context& sys_context, QueryConfig* config, voila::Context& voila_context);
	~CompileRequest();

	void dump() const;

	bool is_valid() const { return validate_check(false); }
	void validate() const { validate_check(true); }

	void debug_validate() const {
#ifdef IS_DEBUG_BUILD
		validate();
#endif
	}

	std::unique_ptr<SignatureCache> signature_cache;

	excalibur::Context& sys_context;

	std::shared_ptr<IBudgetManager> budget_manager;

	void set_analysis_from_fuse_group(const FuseGroup& group);

private:
	std::string _get_operation_signature();

	mutable std::string cached_full_sign;
	mutable std::string cached_op_sign;

	void update();

	bool validate_check(bool use_assert) const;
};

} /* engine::voila */