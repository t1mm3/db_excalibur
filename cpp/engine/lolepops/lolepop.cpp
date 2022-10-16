#include "lolepop.hpp"
#include "system/system.hpp"
#include "engine/xvalue.hpp"
#include "engine/stream.hpp"
#include "engine/query.hpp"
#include "engine/profile.hpp"
#include "voila_lolepop.hpp"

#include <sstream>

using namespace engine;
using namespace lolepop;

RelOp::RelOp(Query& query, const std::shared_ptr<RelOp>& child)
 : child(child), query(query), m_stable_op_id(++query.stable_op_id_counter)
{
}

std::string
RelOp::unique_prefix(const std::string& id) const
{
	std::ostringstream s;
	s << (size_t)(this) << "_" + id;
	return s.str();
}

void
RelOp::dealloc_resources()
{
	LOG_TRACE("RelOp::dealloc_resources() fallback");
}



Lolepop::Lolepop(const std::string& name, const std::shared_ptr<RelOp>& rel_op,
	Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
	const std::shared_ptr<Lolepop>& child, uint64_t flags)
 : child(child), name(name), rel_op(rel_op), stream(stream),
 	mem_context(mem_context),
 	flags(flags)
{
	if (stream.profile) {
		profile = std::make_shared<LolepopProf>(name,
			child ? child->profile : nullptr);
	}

	reset_runtime_flags();
}

Lolepop::~Lolepop()
{
}

void
Lolepop::prepare()
{
	if (child) {
		child->do_prepare();
	}
}

void
Lolepop::update_profiling()
{
}

NextResult
Lolepop::get_next(NextContext& context)
{
	ASSERT(is_prepared);
	LOG_TRACE("Lolepop '%s': next", name.c_str());

	if (is_dummy_op) {
		return child->get_next(context);
	}

#ifdef HAS_PROFILING
	auto prof_start = profiling::physical_rdtsc();
#endif

	NextResult result = next(context);

#ifdef HAS_PROFILING
	auto prof_time = profiling::physical_rdtsc() - prof_start;

	// Update profiling data
	size_t in_tuples = child ? child->output->get_num() : 0;

	bool no_output = runtime_flags & Lolepop::kNoResult;
	size_t out_tuples = no_output ? 0 : output->get_num();

	LOG_DEBUG("Lolepop '%s'(%p): next, no_output %d, input %llu, returned %llu",
		name.c_str(), this, no_output, in_tuples, out_tuples);

	if (profile) {
		profile->update_on_next(1, in_tuples, out_tuples, prof_time - prof_child_fetch_sum);
		prof_child_fetch_sum = 0;
	}
#endif

	return result;
}

void
Lolepop::fetch_from_child_invalid_result(NextResult r) const
{
	ASSERT(false);
}

void
Lolepop::print_plan(const std::shared_ptr<engine::lolepop::Lolepop>& op,
	const std::string& prefix, size_t level)
{
	LOG_WARN("%s [%llu]: Operator '%s' (%p) is_dummy_op %d",
		prefix.c_str(), level, op->name.c_str(), op.get(), op->is_dummy_op);

	if (auto voila_op = dynamic_cast<engine::lolepop::VoilaLolepop*>(op.get())) {
		auto& block = voila_op->voila_block;
		block->dump();
	}

	if (op->child) {
		print_plan(op->child, prefix, level+1);
	}
}



Assign::Assign(const Expr& ref, const Expr& expr)
 : ref(ref), expr(expr)
{
	auto column_ref = dynamic_cast<ColumnRef*>(ref.get());
	ASSERT(column_ref && "Must be ColumnRef");
}


void
Stride::get_flow_meta(size_t& num, bool& done) const
{
	if (predicate) {
		num = predicate->num;
		done = predicate->done;
	} else {
		ASSERT(col_data.size() > 0);
		num = col_data[0]->num;
		done = col_data[0]->done;
	}
	LOG_TRACE("get_flow_meta: %s num=%d done=%d",
		predicate ? "predicate" : "normal", num, done);
}