#pragma once

#include <string>
#include <vector>

#include "engine/buffer.hpp"

namespace memory {
struct Context;
}

namespace engine {
struct Type;
struct SqlType;

namespace catalog {
struct Table;
struct AppendableFlatArrayColumn;
}

}

namespace engine {
namespace storage {

struct ChunkedFileReader;
struct ChunkedFileWriter;
struct TableSlice;

using engine::catalog::AppendableFlatArrayColumn;

struct CsvLoadException {
	CsvLoadException(std::string s) {}
};

struct CsvLoader {
private:
	catalog::Table& table;
	const std::string seperator = "|";
	const size_t num_cols;
	size_t row_id;


	std::vector<std::unique_ptr<Buffer>> buffers;
	std::vector<SqlType*> sql_types;

	std::unique_ptr<memory::Context> mem_context;

public:
	CsvLoader(catalog::Table& table, size_t start_row = 0);
	~CsvLoader();
	void flush();
	void push_row(const std::string& cpp_string);

	void from_file(const std::string& file_name,
		const std::string& postfix = "",
		bool cache = true, int64_t num_lines = -1);
private:
	void push_val(size_t col_id, const char* ptr, size_t len);

	size_t buf_sz = 0;

	std::unique_ptr<ChunkedFileReader> cache_reader;
	std::unique_ptr<ChunkedFileWriter> cache_writer;
	std::unique_ptr<TableSlice> cache_slice;

	void create_cache_slice();
};

} /* storage */
} /* engine */