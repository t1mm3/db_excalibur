#include "csv_loader.hpp"

#include "engine/catalog.hpp"
#include "engine/string_heap.hpp"
#include "engine/sql_types.hpp"
#include "engine/types.hpp"

#include "system/system.hpp"
#include "system/memory.hpp"

#include "file_io.hpp"

#include <cstring>
#include <fstream>

using namespace engine;
using namespace storage;

CsvLoader::CsvLoader(catalog::Table& table, size_t start_row)
 : table(table), num_cols(table.get_num_columns())
{
	ASSERT(num_cols > 0);
	row_id = start_row;

	mem_context = std::make_unique<memory::Context>(nullptr, "csv_loader", -1);

	auto names = table.get_column_names();

	ASSERT(num_cols == names.size());
	for (auto& name : names) {
		auto col = table.get_column(name);
		ASSERT(col && col->data_type && col->sql_type);

		sql_types.push_back(col->sql_type.get());
		buffers.emplace_back(std::make_unique<Buffer>(*col->data_type));
	}
}

void
CsvLoader::push_row(const std::string& cpp_string)
{
	const char* s = &cpp_string[0];
	size_t total_len = cpp_string.size();

	size_t readpos = 0;
	size_t col_id = 0;
	const char* sep = seperator.c_str();

	while (1) {
		char* next_sep = strstr((char*)s, sep);
		size_t len = next_sep ? next_sep - s : total_len - readpos;

		push_val(col_id, s, len);
		col_id++;

		if (!next_sep || col_id >= num_cols) {
			break;
		}

		readpos += len;

		s = next_sep+seperator.size();
	}

	buf_sz++;
	if (buf_sz >= 16*1024*1024) {
		flush();
	}

	row_id++;
}

void
CsvLoader::flush()
{
	if (!buf_sz) {
		return;
	}
	size_t num = buf_sz;

	create_cache_slice();
	std::vector<uint64_t> indexes;
	std::vector<uint64_t> lengths;
				
	if (cache_writer && cache_slice) {
		cache_slice->reset();
		cache_slice->num_values = num;
		indexes.resize(num);
		lengths.resize(num);
	}


	for (size_t i=0; i<num_cols; i++) {
		auto col = table.get_column(i);
		ASSERT(col && buffers[i]);
		auto& buf = *buffers[i];

		ASSERT(buf.size() == num);

		col->append_n(buf.get_data(), num);

		if (cache_writer) {
			const auto& type = col->data_type;
			auto& chunk = cache_slice->columns[i];
			
			chunk.size = 0;

			if (type->is_var_len()) {
				char** data = (char**)buf.get_data();
				// add table
				size_t base = 0;
				for (size_t i=0; i<num; i++) {
					lengths[i] = strlen(data[i]);
					indexes[i] = base;
					base += lengths[i]+1;
				}

				chunk.write((char*)&indexes[0], num*sizeof(uint64_t));

				// add values
				for (size_t i=0; i<num; i++) {
					uint64_t length = lengths[i];

					chunk.write(data[i], length+1);
				}
			} else {
				chunk.buffer = buf.get_data();
				chunk.size = type->get_width() * num;
			}
		}

		buf.clear();
	}
	table.size += num;
	if (cache_writer && cache_slice && num > 0) {
		cache_writer->write_slice(*cache_slice);
	}

	buf_sz = 0;
}

void
CsvLoader::push_val(size_t col_id, const char* ptr, size_t len)
{
	ASSERT(col_id < num_cols);
	auto& buf = *buffers[col_id];
	auto& sql_type = *sql_types[col_id];

	StructThatFitsBiggestType tmp;

	std::string str(ptr, ptr+len);

	LOG_TRACE("CsvLoader::push_val[%llu] = %s", col_id, str.c_str());

	// translate
	if (!sql_type.parse(&tmp, str)) {
		LOG_ERROR("parsing column %llu failed", col_id);
		THROW(CsvLoadException, "Parsing failed");
		return;
	}

	// materialize
	buf.append_n(&tmp, 1);
}

void
CsvLoader::from_file(const std::string& file_name, const std::string& postfix,
	bool cache, int64_t num_lines)
{
	std::string cache_file_name(file_name + ".cache" + postfix);

	if (cache) {
		try {
			cache_reader = std::make_unique<ChunkedFileReader>(cache_file_name);
		} catch (FileIOException& e) {
			LOG_ERROR("Cannot read from cache");
			cache_reader.reset();
		}
	}

	if (cache_reader) {
		create_cache_slice();
		std::vector<char*> pointers;

		// read from cached files
		while (1) {
			cache_slice->reset();

			ASSERT(cache_slice->columns.size() > 0);
			bool succ = cache_reader->next_slice(*cache_slice);
			if (!succ) {
				break;
			}

			ASSERT(cache_slice->num_values > 0);
			const auto num = cache_slice->num_values;

			if (pointers.size() < num) {
				pointers.resize(num);
			}

			for (size_t i=0; i<num_cols; i++) {
				auto col = table.get_column(i);
				ASSERT(col);

				const auto& type = col->data_type;
				auto& chunk = cache_slice->columns[i];

				if (type->is_var_len()) {
					uint64_t post_table_offset = num*sizeof(uint64_t);
					uint64_t* table = (uint64_t*)chunk.buffer;
					char** ptrs = &pointers[0];

					for (size_t i=0; i<num; i++) {
						ptrs[i] = (char*)chunk.buffer + table[i] + post_table_offset;
					}
					col->append_n(ptrs, num);
				} else {
					col->append_n(chunk.buffer, num);
				}
			}

			table.size += num;
		}

		return;
	}

	if (cache) {
		try {
			cache_writer = std::make_unique<ChunkedFileWriter>(cache_file_name);
		} catch (FileIOException& e) {
			LOG_ERROR("Cannot create cache writer");
			cache_writer.reset();
		}

		ASSERT(!!cache_reader == !cache_writer);
	}

	std::ifstream f(file_name);

	if (!f.is_open()) {
		LOG_ERROR("CsvLoader: Cannot open file '%s'", file_name.c_str());
		ASSERT(f.is_open());
	}

	int64_t current_line = 0;
	try {
		std::string line;
		while (std::getline(f, line)) {
			if (num_lines >= 0 && current_line >= num_lines) {
				break;
			}
			push_row(line);

			current_line++;
		}

		flush();
		f.close();
	} catch (...) {
		f.close();
		RETHROW();
	}

	cache_writer.reset();
}

void
CsvLoader::create_cache_slice()
{
	if (!cache_reader && !cache_writer && !cache_slice) {
		return;
	}

	cache_slice = std::make_unique<TableSlice>();
	cache_slice->columns.resize(num_cols);
}

CsvLoader::~CsvLoader()
{
	flush();
}