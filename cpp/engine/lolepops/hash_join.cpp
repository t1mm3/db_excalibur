#include "hash_join.hpp"
#include "engine/query.hpp"
#include "engine/budget.hpp"
#include "engine/engine.hpp"
#include "engine/types.hpp"
#include "engine/stream.hpp"
#include "engine/table.hpp"
#include "engine/voila/voila.hpp"
#include "system/system.hpp"

using namespace engine;
using namespace lolepop;
using namespace relop;
using engine::voila::Builder;

HashJoin::HashJoin(Query& q, const RelOpPtr& child)
 : HashOpRelOp(q, child)
{
	LOG_TRACE("HashJoin(%p): ctor", this);
}

void
HashJoin::dealloc_resources()
{
	LOG_TRACE("Manual deallocate");
	std::unique_lock lock(mutex);

	LOG_TRACE("HashJoin(%p): dtor", this);
	if (voila_hash_table) {
		voila_hash_table->deallocate();
	}
	physical_hash_table.reset();
}

HashJoin::~HashJoin()
{
	dealloc_resources();
}



HashJoinProbe::HashJoinProbe(size_t bloom_filter_bits, const RelOpPtr& rel_op,
	Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child)
 : VoilaLolepop("HashJoinProbe", rel_op, stream, mem_context, child),
 	HashOpLolepop(rel_op, stream),
 	bloom_filter_bits(bloom_filter_bits)
{
	check_relop<relop::HashJoin>(rel_op);
}

size_t
HashJoinProbe::estimate_hash_table_size(size_t& num_rows, size_t& num_buckets) const
{
	auto table = hash_join->physical_hash_table;
	ASSERT(table);
	num_rows = table->post_build_get_total_row_count();
	num_buckets = table->hash_index_capacity;
	return num_rows * table->get_row_width();
}

VoilaCodegenContext
HashJoinProbe::codegen_produce(VoilaCodegenContext&& ctx)
{
	Builder outer_builder(*voila_block);

	auto& hash_table = hash_join->voila_hash_table;

	auto input_pred = outer_builder.input_pred();
	auto input_data = outer_builder.input();

	VoilaExprBuilder expr_builder(name, ctx, input_pred, input_data, outer_builder,
		voila_context);

	voila::Expr hash;
	for (auto& key : hash_join->probe_keys) {
		auto col = expr_builder(key);
		hash = hash ? outer_builder.rehash(input_pred, hash, col)
			: outer_builder.hash(input_pred, col);
	}

	ASSERT(hash);

	auto probe_predicate = input_pred;

	if (bloom_filter_bits) {
		probe_predicate = outer_builder.seltrue(
			probe_predicate,
			outer_builder.bloomfilter_lookup(probe_predicate, hash_table, hash,
				bloom_filter_bits));
	}


	auto buckets_var = outer_builder.new_var(
		outer_builder.bucket_lookup(probe_predicate, hash_table, hash, false),
		"buckets_var", true /* global */);

	auto check_var = outer_builder.new_var(
		outer_builder.constant(0),
		"check_var", true /* global */);

	{
		auto tmp_state = hash_join->get_stream_state_or_create<HashJoin::JoinStreamState>(&stream);
		tmp_state->var_probe_check = check_var;
		tmp_state->var_probe_bucket = buckets_var;
	}

	auto valid_buckets = [&] (const auto& pred, auto& b, const auto& buckets) {
		return b.selfalse(pred, b.eq(pred, buckets, b.constant(0)));
	};

	auto cond_var = outer_builder.new_var(
		valid_buckets(probe_predicate, outer_builder,
			outer_builder.ref(buckets_var, probe_predicate)),
		"cond_var");

	outer_builder.in_loop(outer_builder.ref(cond_var, nullptr), [&] (auto b) {
		auto valid_pred = outer_builder.ref(cond_var, nullptr);

		b.emit(valid_pred, input_data);

		if (hash_join->fk1) {
			// eliminate matches
			valid_pred = b.selfalse(valid_pred, b.ref(check_var, valid_pred));
		}

		// get new pos for next iteration
		b.assign(valid_pred, buckets_var,
			outer_builder.bucket_next(valid_pred, hash_table,
				b.ref(buckets_var, valid_pred)));

		b.assign(valid_pred, cond_var,
			valid_buckets(valid_pred, b,
				b.ref(buckets_var, valid_pred)));
	});

	return ctx;
}

struct BloomFilterBuildTask : scheduler::Task {
	using IHashTable = engine::table::IHashTable;
	std::shared_ptr<IHashTable> physical_hash_table;
	std::unique_ptr<BudgetUser> budget;

	BloomFilterBuildTask(const std::shared_ptr<IHashTable>& physical_hash_table,
		BudgetUser* budget_user)
	 : physical_hash_table(physical_hash_table)
	{
		if (budget_user) {
			budget = budget_user->create_sibling();
		}
	}

	void operator()() final {
		if (budget) {
			budget->with([&] () {
				ASSERT(physical_hash_table);
				physical_hash_table->build_bloom_filter(budget.get());
			}, "BloomFilterBuildTask");
		} else {
			ASSERT(physical_hash_table);
			physical_hash_table->build_bloom_filter(budget.get());
		}
	}
};

void
HashJoinProbe::post_type_pass(BudgetUser* budget_user)
{
	ASSERT(hash_join->physical_hash_table);

	if (bloom_filter_bits) {
		auto table = hash_join->physical_hash_table;
		size_t parallelism = stream.query.config->parallelism();
		size_t num_rows = table->post_build_get_total_row_count();

		if (budget_user) {
			budget_user->ensure_sufficient(kDefaultBudget,
				"HashJoinProbe::post_type_pass(bf)");
		}

		// adjust #threads downwards
		parallelism = std::min(parallelism, num_rows / 20000);

		if (parallelism > 1) {
			std::vector<std::shared_ptr<scheduler::Task>> tasks;
			tasks.reserve(parallelism-1);

			for (size_t i=1; i<parallelism; i++) {
				tasks.emplace_back(std::make_shared<BloomFilterBuildTask>(
					table, budget_user));
			}

			g_scheduler.submit_n(tasks);
		}
		table->build_bloom_filter(budget_user);
	}
}



HashJoinCheck::HashJoinCheck(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child)
 : HashOpCheck("HashJoinCheck", rel_op, stream, mem_context, child)
{
	check_relop<relop::HashJoin>(rel_op);

	ASSERT(hash_join->build_keys.size() == hash_join->probe_keys.size());

	fk1 = hash_join->fk1;
}

std::shared_ptr<voila::HashTable>
HashJoinCheck::get_voila_hash_table()
{
	return hash_join->voila_hash_table;
}



HashJoinGather::HashJoinGather(const RelOpPtr& rel_op, Stream& stream, 
	const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child)
 : VoilaLolepop("HashJoinGather", rel_op, stream, mem_context, child),
 	HashOpLolepop(rel_op, stream)
{
	check_relop<relop::HashJoin>(rel_op);
}

struct GatherTuple {
	Expr column;
	bool is_key;
	size_t index;

	GatherTuple(const Expr& column, bool is_key, size_t index) : column(column), is_key(is_key), index(index) {}
};

VoilaCodegenContext
HashJoinGather::codegen_produce(VoilaCodegenContext&& ctx)
{
	size_t i;
	Builder b(*voila_block);

	auto input_pred = b.input_pred();
	auto input_data = b.input();

	auto& hash_table = hash_join->voila_hash_table;

	voila::Expr bucket;

	{
		auto stream_state = hash_join->get_stream_state<HashJoin::JoinStreamState>(&stream);
		
		ASSERT(stream_state->var_probe_bucket);
		bucket = b.ref(stream_state->var_probe_bucket, input_pred);
	}

	std::vector<voila::Expr> tuple;

	std::vector<GatherTuple> cols;
	cols.reserve(hash_join->build_keys.size() + hash_join->build_payloads.size());

	i=0;
	for (auto& key : hash_join->build_keys) {
		cols.emplace_back(GatherTuple { key, true, i });
		i++;
	}
	i=0;
	for (auto& val : hash_join->build_payloads) {
		cols.emplace_back(GatherTuple { val, false, i });
		i++;
	}

	tuple.reserve(cols.size() + ctx.flow_pos.size());

	size_t pos = 0;
	for (size_t i=0; i<ctx.flow_pos.size(); i++, pos++) {
		tuple.emplace_back(b.get(input_pred, input_data, ctx.flow_pos[i]));
	}

	ctx.flow_pos.reserve(ctx.flow_pos.size() + cols.size());

	i=0;
	for (auto gather_tuple : cols) {
		auto col_value = gather_tuple.is_key ?
			hash_table->keys[gather_tuple.index] :
			hash_table->values[gather_tuple.index];

		auto col_expr = b.bucket_gather(input_pred, hash_table,
			bucket, col_value);
		tuple.push_back(col_expr);

		// reconnect flow
		ctx.flow_pos.push_back(pos);
		ctx.columns[VoilaExprBuilder::output_col_name(gather_tuple.column)]
			= VoilaCodegenContext::Col { pos, std::move(col_expr)};

		pos++;
	}

	b.emit(input_pred, b.tuple(input_pred, std::move(tuple)));
	return ctx;
}



HashJoinWrite::HashJoinWrite(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child)
 : VoilaLolepop("HashJoinWrite", rel_op, stream, mem_context, child,
 		Lolepop::kNoResult),
 	HashOpLolepop(rel_op, stream)
{

}

VoilaCodegenContext
HashJoinWrite::codegen_produce(VoilaCodegenContext&& ctx)
{
	std::unique_lock lock(hash_join->mutex);

	bool update_voila_table = !hash_join->voila_hash_table;
	if (update_voila_table) {
		hash_join->voila_hash_table = std::make_shared<voila::HashTable>();
	}

	LOG_DEBUG("TODO: type_stream should type columns, do we still need types here?");
	Builder b(*voila_block);

	auto input_pred = b.input_pred();
	auto input_data = b.input();

	auto& hash_table = hash_join->voila_hash_table;

#if 1
	auto pos_var = b.new_var(b.write_pos(input_pred, hash_table), "write_pos");
	auto pos = b.ref(pos_var, input_pred);
#else
	auto pos = b.write_pos(input_pred, hash_table);
#endif

	size_t col_id = 0;

	// compute hash for keys
	VoilaExprBuilder expr_builder(name, ctx, input_pred, input_data, b, voila_context);

	auto write_col = [&] (bool is_key, auto& col, auto type) {
		std::string col_name(std::to_string(col_id));

		std::shared_ptr<voila::Column> ht_col;

		if (update_voila_table) {
			ht_col = std::make_shared<voila::Column>(col_name, type);
			hash_table->add_column(is_key, ht_col);
		} else {
			ht_col = hash_table->get_column(col_name);
		}
		b.write(input_pred, hash_table, ht_col, pos, col);

		col_id++;
	};

	voila::Expr hash;
	for (auto& key : hash_join->build_keys) {
		auto col = expr_builder(key);
		hash = hash ? b.rehash(input_pred, hash, col)
			: b.hash(input_pred, col);

		write_col(true, col, TypeSystem::new_empty());
	}

	for (auto& val : hash_join->build_payloads) {
		auto col = expr_builder(val);
		write_col(false, col, TypeSystem::new_empty());
	}

	ASSERT(hash);

	write_col(false, hash, TypeSystem::new_hash());

	return VoilaCodegenContext {};
}

void
HashJoinWrite::post_type_pass(BudgetUser* budget_user)
{
	std::unique_lock lock(hash_join->mutex);

	auto& hash_table = hash_join->voila_hash_table;
	ASSERT(hash_table);
	if (hash_join->physical_hash_table) {
		return;
	}

	// overwrite empty types with actual data types
	for (auto& key : hash_table->keys) {
		ASSERT(key);
		std::unique_lock lock_guard(key->mutex);
		ASSERT(key->node_info && key->type);
		key->type = key->node_info->type;
		ASSERT(key->type && !key->type->is_empty());
	}

	for (auto& val : hash_table->values) {
		ASSERT(val);
		std::unique_lock lock_guard(val->mutex);
		ASSERT(val->node_info && val->type);
		val->type = val->node_info->type;
		ASSERT(val->type && !val->type->is_empty());
	}

	// create PhysicalHashTable
	engine::table::TableLayout layout;
	// Needs to be synced with HashTable::get_phy_column_index()
	for (auto& key : hash_table->keys) layout.add_column(key->type);
	for (auto& val : hash_table->values) layout.add_column(val->type);

	layout.add_column(TypeSystem::new_bucket(), true); // next

	hash_join->physical_hash_table = std::make_shared<engine::table::IHashTable>(
		"hash_join", stream.query, (engine::table::LogicalMasterTable*)nullptr,
		std::move(layout), false, false, false);

	hash_table->physical_table = hash_join->physical_hash_table;
}


HashJoinBuildTable::HashJoinBuildTable(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context)
 : Lolepop("HashJoinBuildTable", rel_op, stream, mem_context,
 		nullptr, Lolepop::kNoResult),
	HashOpLolepop(rel_op, stream)
{

}

NextResult
HashJoinBuildTable::next(NextContext& context)
{
	auto& table = hash_join->physical_hash_table;
	ASSERT(table);

	LOG_TRACE("Build Table numa %lld, id %lld",
		context.numa_node, context.thread_id);

	table->build_index(true, context.numa_node);
	return NextResult::kEndOfStream;
}



double
engine::relop::HashJoin::get_max_tuples() const
{
	if (fk1) {
		return child->get_max_tuples();
	}

	ASSERT(build_relop && probe_relop);
	return std::min(query.config->max_tuples(),
		build_relop->get_max_tuples() * probe_relop->get_max_tuples());
}
