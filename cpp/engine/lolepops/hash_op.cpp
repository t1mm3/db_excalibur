#include "hash_op.hpp"

#include "hash_join.hpp"
#include "hash_group_by.hpp"

#include "engine/types.hpp"
#include "engine/stream.hpp"
#include "engine/table.hpp"
#include "engine/voila/voila.hpp"
#include "system/system.hpp"

using namespace engine;
using namespace lolepop;
using namespace relop;

using engine::voila::Builder;

HashOpRelOp::HashOpRelOp(Query& q, const RelOpPtr& child) : RelOp(q, child)
{

}

HashOpLolepop::HashOpLolepop(const RelOpPtr& rel_op, Stream& stream)
{
	hash_op = std::dynamic_pointer_cast<engine::relop::HashOpRelOp>(rel_op);
	ASSERT(hash_op);

	hash_group_by = std::dynamic_pointer_cast<engine::relop::HashGroupBy>(rel_op);
	hash_join = std::dynamic_pointer_cast<engine::relop::HashJoin>(rel_op);
	ASSERT(hash_group_by || hash_join);
}




HashOpCheck::HashOpCheck(const std::string& name, const RelOpPtr& rel_op,
	Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child)
 : VoilaLolepop(name, rel_op, stream, mem_context, child),
 	HashOpLolepop(rel_op, stream)
{
	check_relop<relop::HashJoin>(rel_op);
}

VoilaCodegenContext
HashOpCheck::codegen_produce(VoilaCodegenContext&& ctx)
{
	auto hash_table = get_voila_hash_table();
	ASSERT(hash_table);

	Builder b(*voila_block);

	auto input_pred = b.input_pred();
	auto input_data = b.input();

	VoilaExprBuilder expr_builder(name, ctx, input_pred, input_data, b, voila_context);

	voila::Expr check;
	voila::Expr bucket;

	// get variables from scope
	voila::Var check_var;


	{
		HashOpRelOp::StreamState* stream_state;

		if (hash_join) {
			stream_state = hash_join->get_stream_state<HashOpRelOp::StreamState>(&stream);
		} else {
			stream_state = hash_group_by->get_stream_state<HashOpRelOp::StreamState>(&stream);
		}
		ASSERT(stream_state);

		if (fk1) {
			check_var = stream_state->var_probe_check;
			ASSERT(check_var);
		}

		ASSERT(stream_state->var_probe_bucket);
		bucket = b.ref(stream_state->var_probe_bucket, input_pred);
	}

	for (size_t i=0; i<hash_op->probe_keys.size(); i++) {
		auto p = b.bucket_check(input_pred, hash_table, bucket, hash_table->keys[i],
			expr_builder(hash_op->probe_keys[i]));
		check = check ? b.l_and(input_pred, check, std::move(p)) : std::move(p);
	}

	if (fk1) {
		b.assign(input_pred, check_var, check);
	}
	b.emit(b.seltrue(input_pred, check), input_data);
	return ctx;
}

size_t
HashOpRelOp::stream_id(Stream& s)
{
	return s.get_parallel_id();
}