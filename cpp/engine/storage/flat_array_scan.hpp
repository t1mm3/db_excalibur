#pragma once

#include "scan_interface.hpp"
#include "engine/types.hpp"
#include "engine/catalog.hpp"

namespace engine {
namespace storage {

struct FlatArrayScan : ScanInterface {
	size_t next(size_t& offset, ScanThread* thread, size_t morsel) override {
		LOG_TRACE("FlatArrayScan(thread=%p, morsel=%llu)",
			thread, morsel);
		offset = progress_read_position.fetch_add(morsel);
		if (offset >= table_size) {
			// done
			return 0;
		}
		size_t num = std::min(morsel, table_size-offset);
		if (!num) {
			return 0;
		}

		// compute new local data
		const auto num_cols = col_data.size();
		std::vector<char*> data;
		data.resize(num_cols);
		for (size_t i=0; i<num_cols; i++) {
			data[i] = col_data[i]+col_width[i]*offset;
		}

		thread->set_columnar_data(&data[0], num_cols);
		return num;
	}

	std::vector<char*> col_data;

	FlatArrayScan(const ScanInterface::Args& args)
	 : ScanInterface(args)
	{
		using FlatArrayColumn = engine::catalog::FlatArrayColumn;

		auto& columns = args.cols;
		const auto num_cols = columns.size();

		col_data.resize(num_cols);
		col_width.resize(num_cols);
		col_catalog.resize(num_cols);

		for (size_t i=0; i<num_cols; i++) {
			const auto& col_name = columns[i];
			auto col_ptr = args.table->get_column(col_name);
			ASSERT(col_ptr && "Column does not exist");
			const auto& cat_col = dynamic_cast<FlatArrayColumn*>(col_ptr);
			ASSERT(cat_col && "Must be FlatArrayColumn");
			col_data[i] = cat_col->get_flat_array_data();
			ASSERT(cat_col->data_type);

			col_catalog[i] = col_ptr;
			col_width[i] = col_catalog[i]->data_type->get_width();
		}
	}
};

} /* storage */
} /* engine */
