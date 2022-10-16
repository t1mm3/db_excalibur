#include "hash_group_by.hpp"

#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/stream.hpp"
#include "engine/table.hpp"
#include "engine/voila/voila.hpp"
#include "system/system.hpp"


using namespace engine;
using namespace lolepop;
using namespace relop;

using engine::voila::Builder;
typedef HashGroupBy::GroupStreamState GroupStreamState;

HashGroupBy::HashGroupBy(Query& q, const RelOpPtr& child, bool local_group_by,
	bool is_reaggregation, bool is_global_aggregation)
 : HashOpRelOp(q, child), local_group_by(local_group_by),
	is_reaggregation(is_reaggregation), is_global_aggregation(is_global_aggregation)
{
	LOG_TRACE("HashGroupBy(%p): ctor, local_group_by=%d",
		this, local_group_by);
	if (!local_group_by) {
		master_hash_table = std::make_shared<table::LogicalMasterTable>();
	}
}

void
HashGroupBy::dealloc_resources()
{
	LOG_TRACE("HashGroupBy(%p): ITable: dtor", this);

	std::unique_lock lock(mutex);

	for (auto& keyval : stream_state) {
		auto state = (GroupStreamState*)keyval.second.get();

		if (!state) {
			continue;
		}
		if (state->voila_hash_table) {
			state->voila_hash_table->deallocate();
		}
		state->physical_hash_table.reset();
	}

	master_hash_table.reset();
}


HashGroupBy::~HashGroupBy()
{
	LOG_TRACE("HashGroupBy(%p): dtor", this);
	dealloc_resources();
}



HashGroupByBuildOpHelper::HashGroupByBuildOpHelper(
	const std::shared_ptr<engine::relop::HashGroupBy>& hash_group_by,
	VoilaLolepop* op)
 : hash_group_by(hash_group_by), op(op)
{
	auto state = hash_group_by->get_stream_state_or_create<GroupStreamState>(&op->stream);
	ASSERT(state);
	state->last_build_phase_op = op;
}

bool
HashGroupByBuildOpHelper::is_last() const
{
	auto state =
		hash_group_by->get_stream_state<GroupStreamState>(&op->stream);
	ASSERT(state);

	return state->last_build_phase_op == op;
}

void
HashGroupByBuildOpHelper::post_type_pass(BudgetUser* budget_user)
{
	auto stream = &op->stream;
	if (!is_last()) {
		return;
	}

	LOG_TRACE("HashGroupByBuildOpHelper::Type HashTable");

	auto state = hash_group_by->get_stream_state<GroupStreamState>(stream);
	ASSERT(state);

	auto& hash_table = state->voila_hash_table;
	ASSERT(hash_table);

	// overwrite empty types with actual data types
	for (auto& key : hash_table->keys) {
		ASSERT(key && key->node_info && key->type);
		key->type = key->node_info->type;
		ASSERT(key->type && !key->type->is_empty());
	}

	for (auto& val : hash_table->values) {
		ASSERT(val && val->node_info && val->type);
		val->type = val->node_info->type;
		ASSERT(val->type && !val->type->is_empty());
	}

	engine::table::TableLayout layout;
	// Needs to be synced with HashTable::get_phy_column_index()
	for (auto& key : hash_table->keys) layout.add_column(key->type);
	for (auto& val : hash_table->values) {
		layout.add_column(val->type, hash_table->column_must_init(val));
	}

	if (!hash_group_by->is_global_aggregation) {
		layout.add_column(TypeSystem::new_bucket(), true); // next
	}

	// create physical hash table
	bool has_hash_index = !hash_group_by->is_global_aggregation;

	if (!state->physical_hash_table) {
		ASSERT(!hash_table->physical_table);
		state->physical_hash_table = std::make_shared<engine::table::IHashTable>(
			"hash_group_by", stream->query,
			hash_group_by->master_hash_table.get(),
			std::move(layout), true, hash_group_by->local_group_by, has_hash_index);
		LOG_TRACE("HashGroupBy: ITable: create %p",
			state->physical_hash_table.get());

		hash_table->physical_table = state->physical_hash_table;
	}

	if (hash_group_by->is_global_aggregation && !state->global_aggr_bucket_ptr) {
		ASSERT(state->global_aggr_bucket);

		// allocate row
		auto table = std::dynamic_pointer_cast<engine::table::IHashTable>(
			hash_table->physical_table);
		ASSERT(table);
		auto block = table->hash_append_prealloc(1,
			scheduler::get_current_numa_node());
		ASSERT(block && block->data);
		table->hash_append_prune(block, 1);

		// overwrite constant with row pointer
		state->global_aggr_bucket_ptr = block->data;

		static_assert(sizeof(size_t) == sizeof(char*), "Need 64-bit pointer");
		auto row_int = (size_t)state->global_aggr_bucket_ptr;
		state->global_aggr_bucket->value = std::to_string(row_int);
		// state->global_aggr_bucket->can_inline = true; // false; /* unpredictable value */
	}

}

HashGroupByBuildOpHelper::~HashGroupByBuildOpHelper()
{
}



HashGroupByFindPos::HashGroupByFindPos(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child)
 : VoilaLolepop("HashGroupByFindPos", rel_op, stream, mem_context, child),
 	HashOpLolepop(rel_op, stream)
{
	check_relop<relop::HashGroupBy>(rel_op);

	build_op_helper = std::make_unique<HashGroupByBuildOpHelper>(hash_group_by, this);

	ASSERT(!hash_group_by->is_global_aggregation);
}

VoilaCodegenContext
HashGroupByFindPos::codegen_produce(VoilaCodegenContext&& ctx)
{
	auto state = hash_group_by->get_stream_state_or_create<GroupStreamState>(&stream);
	ASSERT(state);

	auto& hash_table = state->voila_hash_table;
	if (!hash_table) {
		state->voila_hash_table = std::make_shared<voila::HashTable>();
		hash_table = state->voila_hash_table;
	}

	Builder outer_builder(*voila_block);
	auto input_data = outer_builder.input();

	auto miss_var = outer_builder.new_var(outer_builder.input_pred(), "miss_var");
	auto input_pred = outer_builder.ref(miss_var, nullptr);

	VoilaExprBuilder expr_builder(name,
		ctx, input_pred, input_data, outer_builder,
		voila_context);

	// hash keys
	voila::Expr hash;
	for (auto& key : hash_op->probe_keys) {
		auto col = expr_builder(key);
		hash = hash ? outer_builder.rehash(input_pred, hash, col)
			: outer_builder.hash(input_pred, col);
	}

	auto hash_var = outer_builder.new_var(
		hash, "hash_var", true /* global */);

	auto hit_var = outer_builder.new_var(outer_builder.input_pred(), "hit_var");

	size_t rounds = 0;
	outer_builder.in_loop(outer_builder.ref(miss_var, nullptr), [&] (auto& b) {
		ASSERT(!rounds);
		rounds++;
		auto retry_pred = b.ref(miss_var, nullptr);

		auto check_var = b.new_var(
			outer_builder.constant(0),
			"check_var", true /* global */);

		auto buckets_var = b.new_var(
			b.bucket_lookup(retry_pred, hash_table, b.ref(hash_var, retry_pred)),
			"buckets_var", true /* global */);

		state->var_probe_check = check_var;
		state->var_probe_bucket = buckets_var;

		auto empty = b.eq(retry_pred, b.ref(buckets_var, retry_pred), b.constant(0));

		b.assign(retry_pred, hit_var, b.selfalse(retry_pred, empty));
		b.assign(retry_pred, miss_var, b.seltrue(retry_pred, empty));

		// LOOP over hash chain
		b.in_loop(b.ref(hit_var, nullptr), [&] (auto& b) {
			// flow into Check, Aggregate etc.
			auto hit_pred = b.ref(hit_var, nullptr);
			b.emit(hit_pred, input_data);

			// eliminate matches
			b.assign(hit_pred, hit_var,
				b.selfalse(hit_pred, b.ref(check_var, hit_pred)));

			hit_pred = b.ref(hit_var, nullptr);

			b.assign(hit_pred, buckets_var,
				b.bucket_next(hit_pred, hash_table, b.ref(buckets_var, hit_pred)));

			auto empty = b.eq(hit_pred,
				b.ref(buckets_var, hit_pred), b.constant(0));

			auto sel_union = b.selunion(
				b.ref(miss_var, nullptr),
				b.seltrue(hit_pred, empty));

			b.assign(nullptr, miss_var, sel_union);
			b.assign(hit_pred, hit_var, b.selfalse(hit_pred, empty));
		});

		{
			// insert missing tuples
			auto miss_pred = b.ref(miss_var, nullptr);

			// store nex_pos in variable
			auto new_pos_var = b.new_var(b.bucket_insert(miss_pred, hash_table,
				b.ref(hash_var, miss_pred)), "new_pos");
			auto new_pos = b.ref(new_pos_var, nullptr);

			auto can_scatter_var = b.new_var(b.selfalse(miss_pred,
				b.eq(miss_pred, new_pos, b.constant(0))), "can_scatter");
			auto can_scatter = b.ref(can_scatter_var, nullptr);

			VoilaExprBuilder expr_builder(name, ctx, can_scatter, input_data, b,
				voila_context);

			size_t col_id = 0;
			for (auto& key : hash_op->probe_keys) {
				auto ht_col = std::make_shared<voila::Column>("k" + std::to_string(col_id),
					TypeSystem::new_empty());
				col_id++;

				b.bucket_scatter(can_scatter, hash_table, new_pos, ht_col,
					expr_builder(key));
				if (hash_table->add_column(true, std::move(ht_col))) {
					state->key_col_names.emplace_back(
						expr_builder.output_col_name(key));
				}

			}

			for (auto& payload : hash_group_by->payloads) {
				auto ht_col = std::make_shared<voila::Column>("p" + std::to_string(col_id),
					TypeSystem::new_empty());
				col_id++;

				b.bucket_scatter(can_scatter, hash_table, new_pos, ht_col,
					expr_builder(payload));
				if (hash_table->add_column(false, std::move(ht_col))) {
					state->payload_col_names.emplace_back(
						expr_builder.output_col_name(payload));
				}
			}

			auto ht_col = std::make_shared<voila::Column>("hash",
				TypeSystem::new_hash());

			b.bucket_scatter(can_scatter, hash_table, new_pos, ht_col,
				b.ref(hash_var, can_scatter));
			hash_table->add_column(false, std::move(ht_col));
		}
	});

	return ctx;
}



HashGroupByCheck::HashGroupByCheck(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context,
	const LolepopPtr& child)
 : HashOpCheck("HashGroupByCheck", rel_op, stream, mem_context, child)
{
	fk1 = true;
	ASSERT(!hash_group_by->is_global_aggregation);
}

std::shared_ptr<voila::HashTable>
HashGroupByCheck::get_voila_hash_table()
{
	auto state = hash_group_by->get_stream_state<GroupStreamState>(&stream);
	ASSERT(state);

	return state->voila_hash_table;
}



HashGroupByAggregate::HashGroupByAggregate(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child)
 : VoilaLolepop("HashGroupByAggregate", rel_op, stream, mem_context,
 	child, Lolepop::kNoResult),
 	HashOpLolepop(rel_op, stream)
{
	check_relop<relop::HashGroupBy>(rel_op);

	build_op_helper = std::make_unique<HashGroupByBuildOpHelper>(hash_group_by, this);
}

VoilaCodegenContext
HashGroupByAggregate::codegen_produce(VoilaCodegenContext&& ctx)
{
	auto state = hash_group_by->get_stream_state_or_create<GroupStreamState>(&stream);
	ASSERT(state);

	state->count_aggr_col_id = -1;

	Builder b(*voila_block);

	auto input_pred = b.input_pred();
	auto input_data = b.input();


	VoilaExprBuilder expr_builder(name,
		ctx, input_pred, input_data, b, voila_context);

	voila::Expr bucket;

	auto& hash_table = state->voila_hash_table;
	if (hash_group_by->is_global_aggregation) {
		if (!hash_table) {
			state->voila_hash_table = std::make_shared<voila::HashTable>();
			hash_table = state->voila_hash_table;
		}

		if (!state->global_aggr_bucket) {
			ASSERT(!state->global_aggr_bucket);
			state->global_aggr_bucket =
				std::dynamic_pointer_cast<voila::Constant>(b.constant(0));

			state->global_aggr_bucket->type = TypeSystem::new_bucket();
		}

		state->var_probe_bucket = b.new_var(state->global_aggr_bucket,
			"global_aggr_bucket", false,
			true /* cannot inline, A := Constant() */);
	} else {
		ASSERT(hash_table);
		ASSERT(state->var_probe_bucket);
	}
	bucket = b.ref(state->var_probe_bucket, input_pred);


	const size_t col_id = hash_group_by->probe_keys.size() +
		hash_group_by->payloads.size();
	std::shared_ptr<lolepop::Expression> some_input_val;

	for (size_t i=0; i<hash_group_by->aggregates.size(); i++) {
		// create column
		auto ht_col = std::make_shared<voila::Column>(
			"a" + std::to_string(col_id + i),
			TypeSystem::new_empty());

		auto& aggr_col = hash_group_by->aggregates[i];

		if (hash_table->add_column(false, ht_col, true)) {
			state->aggr_col_names.emplace_back(
				expr_builder.output_col_name(aggr_col));
		}


		auto assignment = std::dynamic_pointer_cast<lolepop::Assign>(aggr_col);
		ASSERT(assignment && "We don't allow un-named aggregates");

		// resolve aggregate
		auto aggr_func = std::dynamic_pointer_cast<lolepop::Function>(assignment->expr);
		ASSERT(aggr_func);

		if (!some_input_val) {
			ASSERT(aggr_func->args.size() == 1);
			some_input_val = aggr_func->args[0];
		}

		VoilaExprBuilder::FunctionContext func_ctx;
		func_ctx.bucket = bucket;
		func_ctx.data_structure = hash_table;
		func_ctx.column = std::move(ht_col);
		func_ctx.is_reaggregation = hash_group_by->is_reaggregation;
		func_ctx.is_global_aggr = hash_group_by->is_global_aggregation;

		auto aggr = expr_builder(aggr_func, &func_ctx);
		ASSERT(!aggr && "Should not have result");

		if (state->count_aggr_col_id < 0 && !aggr_func->name.compare("count")) {
			state->count_aggr_col_id = col_id+i;
			ASSERT(state->count_aggr_col_id >= 0);
		}
	}

	if (hash_group_by->is_global_aggregation && state->count_aggr_col_id < 0) {
		// add COUNT(*)
		const std::string kCountColName("$count");

		ASSERT(hash_group_by->aggregates.size() > 0 && some_input_val);
		size_t id = col_id + hash_group_by->aggregates.size();

		state->count_aggr_col_id = id;

		// create column
		auto ht_col = std::make_shared<voila::Column>(
			kCountColName, TypeSystem::new_empty());
		hash_table->add_column(false, ht_col, true);

		auto aggr_func = std::make_shared<lolepop::Function>(
			"count", std::vector<lolepop::Expr> { some_input_val });

		auto assignment = std::make_shared<lolepop::Assign>(
			std::make_shared<lolepop::ColumnRef>(kCountColName),
			aggr_func);

		// resolve aggregate
		VoilaExprBuilder::FunctionContext func_ctx;
		func_ctx.bucket = bucket;
		func_ctx.data_structure = hash_table;
		func_ctx.column = std::move(ht_col);
		func_ctx.is_reaggregation = hash_group_by->is_reaggregation;
		func_ctx.is_global_aggr = hash_group_by->is_global_aggregation;

		auto aggr = expr_builder(aggr_func, &func_ctx);
		ASSERT(!aggr && "Should not have result");
	}

	return ctx;
}



HashGroupByPartition::HashGroupByPartition(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context)
 : Lolepop("HashGroupByPartition", rel_op, stream, mem_context,
 		nullptr, Lolepop::kNoResult),
	HashOpLolepop(rel_op, stream)
{

}

NextResult
HashGroupByPartition::next(NextContext& context)
{
	auto state = hash_group_by->get_stream_state_or_create<GroupStreamState>(&stream);
	ASSERT(state);

	auto& table = state->physical_hash_table;
	ASSERT(table);

	table->flush2partitions(stream.query.config->parallelism(), context.numa_node);
	return NextResult::kEndOfStream;
}



HashGroupByScan::HashGroupByScan(const RelOpPtr& rel_op, Stream& stream,
	const std::shared_ptr<memory::Context>& mem_context)
 : VoilaLolepop("HashGroupByScan", rel_op, stream, mem_context, nullptr),
 	HashOpLolepop(rel_op, stream)
{
	check_relop<relop::HashGroupBy>(rel_op);
}

struct LoadCol {
	std::string name;
	size_t col_id;
	bool key;
};

VoilaCodegenContext
HashGroupByScan::codegen_produce(VoilaCodegenContext&& ctx)
{
	auto state = hash_group_by->get_stream_state<GroupStreamState>(&stream);
	ASSERT(state);


	auto& table = state->physical_hash_table;
	ASSERT(table);

	ASSERT(state->voila_hash_table);
	auto& voila_hash_table = state->voila_hash_table;

	if (!state->read_position) {
		state->read_position = table->makeReadPosition(stream);
	}
	stream.execution_context.source_read_positions.insert(state->read_position.get());



	Builder outer_builder(*voila_block);
	VoilaCodegenContext new_ctx;

	auto& scan_pos = *state->read_position;
	auto pos_var = outer_builder.new_var(
		outer_builder.read_pos(scan_pos, voila_hash_table),
		"pos_var");

	size_t round = 0;

	outer_builder.while_loop([&] (auto& b) {
		// generate predicate
		return b.selnum(b.ref(pos_var, nullptr), b.ref(pos_var, nullptr));
	}, [&] (auto& b, auto& pred) {
		// generate body
		std::vector<voila::Expr> tuple;

		std::vector<LoadCol> columns;
		columns.reserve(state->key_col_names.size() +
			state->payload_col_names.size() +
			state->aggr_col_names.size());
		size_t col_id = 0;
		for (auto& k : state->key_col_names) {
			columns.emplace_back(LoadCol{ k, col_id, true });

			col_id++;
		}

		col_id = 0;
		for (auto& p : state->payload_col_names) {
			columns.emplace_back(LoadCol{ p, col_id, false });

			col_id++;
		}

		// hash
		if (!hash_group_by->is_global_aggregation) {
			col_id++;
		}

		for (auto& a : state->aggr_col_names) {
			columns.emplace_back(LoadCol{ a, col_id, false });

			col_id++;
		}

		tuple.reserve(columns.size());

		ASSERT(!round && "Should only be run once");
		round++;

		auto pos = b.ref(pos_var, nullptr);
#if 0
		auto output_pred = b.selnum(pred, pos);
#else
		auto output_pred = pred;
#endif
		new_ctx.flow_pos.reserve(columns.size());
		new_ctx.columns.reserve(columns.size());

		if (state->count_aggr_col_id >= 0) {
			// has count aggregate to filter 0 values
			// required for parallelism with global aggregates 

			auto col_id = state->count_aggr_col_id;

			auto& cols = voila_hash_table->values;
			auto& struct_col = cols[col_id];
			ASSERT((size_t)col_id < cols.size() && struct_col->type->is_supported_output_type());

			output_pred = b.seltrue(output_pred,
				b.gt(output_pred,
					b.read_col(output_pred, pos, voila_hash_table, struct_col),
					b.constant(0)));
		}

		size_t i=0;
		for (auto& c : columns) {
			auto& cols = c.key ? voila_hash_table->keys : voila_hash_table->values;
			ASSERT(c.col_id < cols.size());
			auto& struct_col = cols[c.col_id];

			// b.comment("read col " + std::to_string(c.col_id) + "name " + struct_col->name);

			ASSERT(struct_col->type->is_supported_output_type());

			auto col_expr = b.read_col(output_pred, pos, voila_hash_table, struct_col);
			tuple.push_back(col_expr);

			new_ctx.columns[c.name]
				= VoilaCodegenContext::Col {i, std::move(col_expr)};
			new_ctx.flow_pos.push_back(i);

			i++;
		}

		b.emit(output_pred, b.tuple(output_pred, std::move(tuple)));

		// get new pos for next iteration
		b.assign(pred, pos_var, outer_builder.read_pos(scan_pos, voila_hash_table));
	}, "scan_loop");

	outer_builder.end_of_flow();
	return new_ctx;
}

HashGroupByScan::~HashGroupByScan()
{

}


double
engine::relop::HashGroupBy::get_max_tuples() const
{
	LOG_TRACE("TODO: Better max tuples bound for GroupBy");
	if (is_global_aggregation) {
		return 1.0;
	}
	return child->get_max_tuples();
}