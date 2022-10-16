#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>

namespace memory {
struct Context;
}

namespace plan {

struct PhysicalType;
struct LogicalType;

} /* plan */


namespace engine {

struct SqlType;
struct Type;
struct StringHeap;
struct Buffer;
struct LogicalSchema;
	
namespace catalog {

struct Column {
	bool get_min_max(double& min, double& max) const;

	Type* data_type = nullptr;
	std::unique_ptr<SqlType> sql_type;

	virtual void append_n(void* data, size_t num);

	Column(std::unique_ptr<SqlType>&& sql_type);

	virtual ~Column() = default;

protected:
	double dmin;
	double dmax;
	bool has_minmax;
};

struct FlatArrayColumn : Column {
	FlatArrayColumn(std::unique_ptr<SqlType>&& sql_type);

	virtual char* get_flat_array_data() = 0;
};

struct AppendableFlatArrayColumn : FlatArrayColumn {
	AppendableFlatArrayColumn(std::unique_ptr<SqlType>&& sql_type);

	void append_n(void* data, size_t num);

	~AppendableFlatArrayColumn();
	virtual char* get_flat_array_data();

private:
	std::unique_ptr<Buffer> buffer;
};

struct Table {
	void add_column(std::shared_ptr<Column>&& data, const std::string& name);

	Column* get_column(const std::string& name) const;

	Column* get_column(size_t index) const;

	size_t get_num_columns() const {
		return columns.size();
	}

	size_t get_num_rows() const {
		return size;
	}

	std::vector<std::string> get_column_names() const {
		return column_names;
	}

	void append_n(void** col_data, size_t num_cols, size_t num_rows);

	virtual ~Table();

	size_t size;
private:
	std::unordered_map<std::string, std::shared_ptr<Column>> columns;
	std::vector<std::string> column_names;
	std::vector<std::shared_ptr<Column>> columns_ordered;

};

struct FlatArrayTable : Table {
	~FlatArrayTable() = default;
};


struct Catalog {	
	void add_table(const std::string& name, std::unique_ptr<Table>&& table);

	Table* get_table(const std::string& name);
	size_t get_num_tables() const {
		return tables.size();
	}

private:
	std::unordered_map<std::string, std::unique_ptr<Table>> tables;
};

} /* catalog */

} /* engine */