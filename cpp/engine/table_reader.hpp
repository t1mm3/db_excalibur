#pragma once

#include "engine/voila/voila.hpp"

namespace engine {
struct Stream;
}

namespace engine::table {

struct LogicalMasterTable;
struct ITable;
struct Block;

struct TableReadPosition : engine::voila::ReadPosition {
protected:
	TableReadPosition() {
		index = 0;
		last_buffer = nullptr;
		done = false;
	}

	size_t index;
	Block* last_buffer;

	void mark_as_done() {
		num_tuples = 0;
		column_data = nullptr;
		done = true;
	}
};

struct MasterTableReadPosition : TableReadPosition {
	void next(size_t num) final;
	double get_progress() final;

	MasterTableReadPosition(LogicalMasterTable& master_table, engine::Stream& stream);
private:
	bool next_partition();

	LogicalMasterTable& master_table;
	size_t part_id;
	bool first = true;
};

struct LocalTableReadPosition : TableReadPosition {
	void next(size_t num) final;
	double get_progress() final;

	LocalTableReadPosition(ITable& table) : table(table) {}

private:
	ITable& table;
};

} /* engine::table */