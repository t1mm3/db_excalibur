#include "catalog.hpp"

#include "engine/types.hpp"
#include "engine/sql_types.hpp"
#include "engine/buffer.hpp"
#include "engine/string_heap.hpp"
#include "engine/utils.hpp"

#include "system/memory.hpp"
#include "system/system.hpp"
#include "storage/file_io.hpp"


#include <limits>

using namespace engine;
using namespace catalog;

static std::string
translate_column_name(const std::string& n)
{
	return StringUtils::to_lower_case(n);
}


static std::string
translate_table_name(const std::string& n)
{
	return StringUtils::to_lower_case(n);
}


Column::Column(std::unique_ptr<SqlType>&& sql_type)
 : sql_type(std::move(sql_type))
{
	ASSERT(this->sql_type);
	data_type = this->sql_type->get_physical_type();
	ASSERT(data_type);

	dmin = 0;
	dmax = 0;
	has_minmax = false;
}

void
Column::append_n(void* data, size_t num)
{
	ASSERT(false && "Not supported");
}

bool
Column::get_min_max(double& min, double& max) const
{
	min = dmin;
	max = dmax;

	if (data_type->is_integer()) {
		return true;
	}

	return has_minmax;
}



FlatArrayColumn::FlatArrayColumn(std::unique_ptr<SqlType>&& sql_type)
 : Column(std::move(sql_type))
{

}



AppendableFlatArrayColumn::AppendableFlatArrayColumn(std::unique_ptr<SqlType>&& sql_type)
 : FlatArrayColumn(std::move(sql_type))
{
	buffer = std::make_unique<Buffer>(*data_type);
}

void
AppendableFlatArrayColumn::append_n(void* src_data, size_t src_num)
{
	if (!src_num) {
		return;
	}

	buffer->append_n(src_data, src_num);

	double min, max;
	bool has = data_type->get_min_max(min, max, src_data, src_num);
	if (has) {
		if (has_minmax) {
			dmin = std::min(dmin, min);
			dmax = std::max(dmax, max);
		} else {
			dmin = min;
			dmax = max;

			has_minmax = true;
		}
	}
}

char*
AppendableFlatArrayColumn::get_flat_array_data()
{
	return buffer->get_data();
}

AppendableFlatArrayColumn::~AppendableFlatArrayColumn()
{

}



void
Table::append_n(void** col_data, size_t num_cols, size_t num_rows)
{
	ASSERT(num_cols == columns_ordered.size());

	for (size_t i=0; i<num_cols; i++) {
		const auto& col = columns_ordered[i];
		ASSERT(col);

		col->append_n(col_data[i], num_rows);
	}

	size += num_rows;
}

Table::~Table()
{

}

void
Table::add_column(std::shared_ptr<Column>&& data, const std::string& _name)
{
	std::string name(translate_column_name(_name));

	auto it = columns.find(name);
	ASSERT(it == columns.end() && "Must have a new & unique column name");

	columns[name] = data;
	column_names.push_back(name);
	columns_ordered.push_back(std::move(data));
}

Column*
Table::get_column(const std::string& _name) const
{
	std::string name(translate_column_name(_name));

	auto it = columns.find(name);
	if (it == columns.end()) {
		return nullptr;
	}

	ASSERT(it->second);
	return it->second.get();
}

Column*
Table::get_column(size_t index) const
{
	if (index >= get_num_columns()) {
		return nullptr;
	}

	ASSERT(columns_ordered[index]);
	return columns_ordered[index].get();
}

void
Catalog::add_table(const std::string& _name, std::unique_ptr<Table>&& table)
{
	std::string name(translate_table_name(_name));

	auto it = tables.find(name);

	if (it != tables.end()) {
		LOG_ERROR("Cannot insert table '%s' because it already exists",
			_name.c_str());
		ASSERT(it == tables.end() && "Must be a new table");
	}

	tables.insert({name, std::move(table)});
}

Table*
Catalog::get_table(const std::string& _name)
{
	std::string name(translate_table_name(_name));

	auto it = tables.find(name);
	if (it == tables.end()) {
		LOG_ERROR("Cannot find table '%s'", _name.c_str());
		ASSERT(it != tables.end());
	}

	return it->second.get();
}
