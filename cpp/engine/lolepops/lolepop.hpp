#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>

#include "system/system.hpp"
#include "system/memory.hpp"
#include "system/profiling.hpp"

struct Query;
struct BudgetUser;

namespace scheduler {
struct Task;
} /* scheduler */

namespace engine {
struct Stream;
struct Engine;
struct XValue;
struct LolepopProf;

namespace protoplan {
struct PlanOp;
}

} /* engine */

namespace engine {

struct RelOp;
typedef std::shared_ptr<RelOp> RelOpPtr;

struct RelOp {
	virtual ~RelOp() = default;

	std::string unique_prefix(const std::string& id) const;

	std::mutex mutex;

	RelOp(Query& query, const std::shared_ptr<RelOp>& child);

	virtual double get_max_tuples() const = 0;

	std::shared_ptr<RelOp> child;

	size_t get_stable_op_id() const { return m_stable_op_id; }

	virtual void dealloc_resources();

protected:
	friend struct protoplan::PlanOp;
	Query& query;

private:
	const size_t m_stable_op_id;
};

namespace lolepop {
struct Expression;
typedef std::shared_ptr<Expression> Expr;

struct Expression {
	virtual ~Expression() = default;
};

struct ColumnRef : Expression {
	const std::string column_name;

	ColumnRef(const std::string& name) : column_name(name) {}
};

struct Constant : Expression {
	const std::string value;

	Constant(const std::string& value) : value(value) {}
};

struct Function : Expression {
	std::string name;
	std::vector<Expr> args;

	Function(const std::string& name, const std::vector<Expr>& args) : name(name), args(args) {}
};

struct Assign : Expression {
	Expr ref;
	Expr expr;

	Assign(const Expr& ref, const Expr& expr);
};

struct OrderExpression : Expression {
	Expr expr;

	OrderExpression(const Expr& expr) : expr(expr) {}
};

struct Descending : OrderExpression {
	Descending(const Expr& expr) : OrderExpression(expr) {}
};

struct Ascending : OrderExpression {
	Ascending(const Expr& expr) : OrderExpression(expr) {}
};

struct Stride {
	std::vector<XValue*> col_data;
	XValue* predicate = nullptr;

	virtual ~Stride() = default;

	void get_flow_meta(size_t& num, bool& done) const;
	size_t get_num() const {
		size_t num;
		bool done;

		get_flow_meta(num, done);
		return num;
	}
};

struct Lolepop;
typedef std::shared_ptr<Lolepop> LolepopPtr;

enum NextResult {
	kUndefined = 0,
	kEndOfStream = -1,
	kYield = 1,
};

struct NextContext {
	scheduler::Task* current_task;
	int64_t thread_id;
	int64_t numa_node;
};

struct Lolepop {
	std::shared_ptr<Lolepop> child;
	Stride* output = nullptr;
	Stride own_stride;

	std::shared_ptr<LolepopProf> profile;

public:
	const std::string name;
	std::shared_ptr<RelOp> rel_op;
	std::shared_ptr<protoplan::PlanOp> plan_op;
	Stream& stream;
	bool is_prepared = false;
	std::shared_ptr<memory::Context> mem_context;

	static const uint64_t kNoResult = 1 << 1;

	const uint64_t flags;
	uint64_t runtime_flags; //! Like 'flags' but can change

	bool is_dummy_op = false;

	// Allows to run code generation in parallel before interpreting
	void do_prepare() {
		if (is_prepared) {
			return;
		}
		prepare();

		is_prepared = true;
	}

	void do_update_profiling() {
		update_profiling();

		if (child) {
			child->do_update_profiling();
		}
	}

	NextResult get_next(NextContext& context);

	virtual void restart() {}

	// Reconnect inputs and outputs
	// Used for VoilaLolepop
	virtual void reconnect_ios() {}

	virtual void post_type_pass(BudgetUser* budget_user) {
		/* do nothing */
	}

	virtual void update_profiling();

	virtual void propagate_internal_profiling() {}

	virtual ~Lolepop();

	void reset_runtime_flags() {
		runtime_flags = flags;
	}

	static void print_plan(const std::shared_ptr<Lolepop>& op,
		const std::string& prefix, size_t level = 0);

	std::vector<std::string> output_column_names;

protected:
	Lolepop(const std::string& name, const std::shared_ptr<RelOp>& rel_op,
		Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
		const std::shared_ptr<Lolepop>& child, uint64_t flags = 0);

	virtual void prepare();

	virtual NextResult next(NextContext& context) = 0;


	bool fetch_from_child(NextContext& context) {
		if (!child) {
			return false;
		}

		bool fetch_next = true;
		bool fetch_exhausted = false;

		while (fetch_next && !fetch_exhausted) {
#ifdef HAS_PROFILING
			auto prof_start = profiling::physical_rdtsc();
#endif
			NextResult c = child->get_next(context);

#ifdef HAS_PROFILING
			prof_child_fetch_sum += profiling::physical_rdtsc() - prof_start;
#endif

			if (c == NextResult::kYield) {
				// repeat until we get data
				fetch_next = child->output->get_num() <= 0;
			} else if (c == NextResult::kEndOfStream) {
				fetch_next = false;
				fetch_exhausted = true;
			} else {
				fetch_from_child_invalid_result(c);
			}
		}

		return !fetch_exhausted;
	}

	void fetch_from_child_invalid_result(NextResult r) const;

private:
#ifdef HAS_PROFILING
	uint64_t prof_child_fetch_sum = 0;
#endif
};

} /* lolepop */
} /* engine */