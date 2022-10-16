#pragma once

#include <memory>
#include <fstream>
#include <vector>
#include <mutex>

#include "system/system.hpp"

namespace memory {
struct UntypedArray;
} /* memory */

namespace engine::storage {

struct ColumnSlice {
	char* buffer = nullptr;
	size_t size = 0;

	std::unique_ptr<memory::UntypedArray> internal_buffer;

	ColumnSlice();
	void write(char* data, size_t n);
	void ensure_fits(size_t sz);
	void clear();
};

struct TableSlice {
	size_t num_values = 0;

	std::vector<ColumnSlice> columns;

	void ensure_fits(size_t sz) {
		for (auto& col : columns) {
			col.ensure_fits(sz);
		}
	}

	void reset() {
		for (auto& col : columns) {
			col.clear();
		}	
	}
};


struct FileIOException : Exception {
	FileIOException(const std::string& msg) : Exception(msg) {

	}
};

struct ChunkedFileReader {
	ChunkedFileReader(const std::string& file_name);

	bool next_slice(TableSlice& slice);

	~ChunkedFileReader();
private:
	std::ifstream f;
	std::vector<char> buf;
};

struct ChunkedFileWriterColState;
struct ChunkedFileWriter {
	ChunkedFileWriter(const std::string& file_name);

	void write_slice(const TableSlice& slice);

	~ChunkedFileWriter();
private:
	bool parallel = true;
	std::ofstream f;
	std::vector<std::unique_ptr<ChunkedFileWriterColState>> col_states;
	std::mutex mutex;

	void prepare_column(size_t col_id, const ColumnSlice& slice, bool expensive = false);
	void write_column(size_t col_id);
};

} /* engine::storage */