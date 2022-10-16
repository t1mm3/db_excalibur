#include "table_reader.hpp"

#include "engine/table.hpp"
#include "engine/stream.hpp"

using namespace engine::table;

MasterTableReadPosition::MasterTableReadPosition(LogicalMasterTable& master_table,
	Stream& stream)
 : master_table(master_table), part_id(stream.get_parallel_id())
{

}

void
MasterTableReadPosition::next(size_t num)
{
	if (!next_partition()) {
		mark_as_done();
	}
}

double
MasterTableReadPosition::get_progress()
{
	// LOG_ERROR("Todo: MasterTableReadPosition::get_progress()");
	return 0.0;
}

bool
MasterTableReadPosition::next_partition()
{
	LOG_TRACE("MasterTableReadPosition::next_partition(%lld)", part_id);
	auto& tables = master_table.tables;
	ASSERT(tables.size() > 0);

	if (first) {
		index = 0;
		first = false;
		part_id = master_table.m_par_read_part.fetch_add(1);
	}

	while (1) {
		if (index >= tables.size()) {
			LOG_TRACE("LogicalMasterTable::get_read_morsel: part %lld out",
				part_id);
			ASSERT(index == tables.size());

			index = 0;
			part_id = master_table.m_par_read_part.fetch_add(1);
		}

		ASSERT(index < tables.size());
		auto t = tables[index];
		ASSERT(t);
		if (t->m_flush_partitions.size() <= part_id) {
			LOG_TRACE("MasterTableReadPosition::done");
			return false;
		}
		auto partition = t->m_flush_partitions[part_id].get();
		ASSERT(partition);

		Block* blk = last_buffer;
		const bool generate_range = blk && blk->num > 0;

		if (generate_range) {
			// output non-empty range
			num_tuples = blk->num;
			column_data = (char**)blk->data;

			LOG_TRACE("MasterTableReadPosition::get_read_morsel: "
				"part %lld table %lld block num %lld block cap %lld "
				"block width %lld",
				part_id, index, blk->num, blk->capacity, blk->width);
		}

		// go to next buffer
		if (blk) {
			last_buffer = blk->next;
		} else {
			blk = partition->head;
			last_buffer = blk;
		}

		if (!last_buffer) {
			index++;
			LOG_TRACE("MasterTableReadPosition::get_read_morsel: "
				"part %lld set table to %lld",
				part_id, index);
		}

		if (generate_range) {
			// ASSERT(morsel._num <= 4);
			LOG_TRACE("MasterTableReadPosition::done 2?");
			break;
		}
	};

	return true;
}


void
LocalTableReadPosition::next(size_t num)
{
	ASSERT(table.m_fully_thread_local && "todo: implement shared scan");
	ASSERT(table.m_write_partitions.size() == 1);

	LOG_TRACE("LocalTableReadPosition::next(%llu)", num);

	Block* blk = last_buffer;

	if (!blk && index) {
		mark_as_done();
		return;
	}

	if (!blk && !index) {
		blk = table.m_write_partitions[0]->head;
		index = 1;
	} else {
		blk = blk->next;
		index = 1;
	}

	LOG_TRACE("blk %p, index %llu. head %p, data %p",
		blk, index, table.m_write_partitions[0]->head,
		blk ? blk->data : nullptr);

	if (!blk) {
		mark_as_done();
		return;
	}

	last_buffer = blk;

	num_tuples = blk->num;
	column_data = (char**)blk->data;
}

double
LocalTableReadPosition::get_progress()
{
	// LOG_ERROR("Todo: LocalTableReadPosition::get_progress()");
	return 0.0;
}
