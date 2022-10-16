#include "file_io.hpp"
#include <vector>
#include <zlib.h>
#include <lz4.h>
#include <lz4hc.h>

#include "system/memory.hpp"
#include "system/scheduler.hpp"

using namespace engine;
using namespace storage;

static constexpr uint64_t kCodecNone = 0;
static constexpr uint64_t kCodecLZ4 = 1;

struct RowGroupHeader {
	uint64_t leading_magic;
	uint64_t num_values;
	uint64_t num_columns;
	uint64_t trailing_magic;

	static constexpr uint64_t kLeadingMagic = 0x1342;
	static constexpr uint64_t kTrailingMagic = 0xBADC0FF33;

	RowGroupHeader() {
		reset();
	}

	void reset() {
		leading_magic = kLeadingMagic;
		trailing_magic = kTrailingMagic;
		num_values = 0;
		num_columns = 0;
	}
};

struct ColumnHeader {
	uint64_t leading_magic;
	uint64_t compr_payload_bytes;
	uint64_t uncompr_payload_bytes;
	uint64_t codec;
	uint64_t column_id;
	uint64_t trailing_magic;
	uint32_t crc32;

	static constexpr uint64_t kLeadingMagic = 0xBADF00D;
	static constexpr uint64_t kTrailingMagic = 0xDEADBEEF;

	ColumnHeader() {
		reset();
	}

	void reset() {
		leading_magic = kLeadingMagic;
		trailing_magic = kTrailingMagic;
		codec = 0;
		compr_payload_bytes = 0;
		uncompr_payload_bytes = 0;
		column_id = 0;
	}
};

ColumnSlice::ColumnSlice()
{
	internal_buffer = std::make_unique<memory::UntypedArray>(1, 0);
}

void
ColumnSlice::write(char* data, size_t n)
{
	internal_buffer->push_back_data(data, n);
	size = internal_buffer->size();
	buffer = (char*)internal_buffer->get_data();
}

void
ColumnSlice::ensure_fits(size_t sz)
{
	internal_buffer->prealloc(sz);
	buffer = (char*)internal_buffer->get_data();
}

void
ColumnSlice::clear()
{
	internal_buffer->clear();
}



ChunkedFileReader::ChunkedFileReader(const std::string& file_name)
{
	f.open(file_name.c_str(), std::ios::binary | std::ios::in);
	if (!f.is_open()) {
		LOG_ERROR("ChunkedFileReader: Cannot open file '%s'",
			file_name.c_str());
		throw FileIOException("Couldn't open file");
	}
}

static uint32_t
get_crc32(char* buffer, size_t len)
{
	uint32_t crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, (const unsigned char*)buffer, len);

	return crc;
}

bool
ChunkedFileReader::next_slice(TableSlice& slice)
{
	if (f.eof()) {
		return false;
	}

	RowGroupHeader row_hdr;
	f.read((char*)&row_hdr, sizeof(row_hdr));

	if (!f.good()) {
		if (f.eof()) {
			return false;
		}
		THROW_MSG(FileIOException, "Cannot read RowGroupHeader");
	}

	if (row_hdr.leading_magic != RowGroupHeader::kLeadingMagic ||
			row_hdr.trailing_magic != RowGroupHeader::kTrailingMagic) {
		THROW_MSG(FileIOException, "Invalid RowGroupHeader");
	}

	if (row_hdr.num_columns != slice.columns.size()) {
		LOG_DEBUG("row_hdr.num_columns %llu != slice.columns.size() %llu",
			row_hdr.num_columns, slice.columns.size());
		THROW_MSG(FileIOException, "#Given columns doesn't match RowGroupHeader");
	}

	slice.num_values = row_hdr.num_values;
	ASSERT(row_hdr.num_columns > 0);

	for (size_t i=0; i<row_hdr.num_columns; i++) {
		ColumnHeader col_hdr;

		f.read((char*)&col_hdr, sizeof(col_hdr));

		if (!f.good()) {
			THROW_MSG(FileIOException, "Cannot read ColumnHeader");
		}

		if (col_hdr.leading_magic != ColumnHeader::kLeadingMagic ||
				col_hdr.trailing_magic != ColumnHeader::kTrailingMagic) {
			THROW_MSG(FileIOException, "Invalid ColumnHeader");
		}

		auto& col = slice.columns[col_hdr.column_id];
		col.ensure_fits(col_hdr.uncompr_payload_bytes);
		col.size = col_hdr.uncompr_payload_bytes;
		void* src_buffer = col.buffer;

		switch (col_hdr.codec) {
		case kCodecNone:
			ASSERT(col_hdr.compr_payload_bytes == col_hdr.uncompr_payload_bytes);
			break;

		case kCodecLZ4:
			if (buf.size() < col_hdr.compr_payload_bytes) {
				buf.resize(col_hdr.compr_payload_bytes);
			}
			src_buffer = &buf[0];
			break;

		default:
			THROW_MSG(FileIOException, "Unsupported codec");
			break;
		}


		f.read((char*)src_buffer, col_hdr.compr_payload_bytes);

		if (!f.good()) {
			THROW_MSG(FileIOException, "Cannot read Column");
		}

		switch (col_hdr.codec) {
		case kCodecLZ4:
			{
				auto decompressed = LZ4_decompress_safe((char*)src_buffer,
					col.buffer,
					col_hdr.compr_payload_bytes,
					col_hdr.uncompr_payload_bytes);
				ASSERT(decompressed == (int)col.size);
			}
			break;

		default:
			break;
		}

		auto crc = get_crc32(col.buffer, col.size);

		if (col_hdr.crc32 != crc) {
			THROW_MSG(FileIOException, "CRC32 does not match");
		}
	}

	return true;
}

ChunkedFileReader::~ChunkedFileReader() {
	f.close();
}



ChunkedFileWriter::ChunkedFileWriter(const std::string& file_name)
{
	if (g_scheduler.get_num_threads() <= 1) {
		parallel = false;
	}

	f.open(file_name.c_str(), std::ios::binary | std::ios::out);
	if (!f.is_open()) {
		LOG_ERROR("ChunkedFileWriter: Cannot open file '%s'",
			file_name.c_str());
		throw FileIOException("Couldn't open file");
	}
}

namespace engine::storage {
struct ChunkedFileWriterColState {
	ColumnHeader header;
	void* dest_buffer;

	std::vector<char> buf;
};
}

void
ChunkedFileWriter::write_slice(const TableSlice& chunk)
{
	{
		RowGroupHeader header;
		header.num_values = chunk.num_values;
		header.num_columns = chunk.columns.size();

		ASSERT(header.num_values > 0);
		ASSERT(header.num_columns > 0);

		f.write((char*)&header, sizeof(header));
	}

	if (col_states.size() < chunk.columns.size()) {
		col_states.resize(chunk.columns.size());
		for (size_t i=0; i<chunk.columns.size(); i++) {
			col_states[i] = std::make_unique<ChunkedFileWriterColState>();
		}
	}

	bool use_parallel = parallel && chunk.columns.size() > 1;

	size_t i;

	if (!use_parallel) {
		for (i=0; i<chunk.columns.size(); i++) {
			prepare_column(i, chunk.columns[i]);
			write_column(i);
		}
		return;
	}

	std::vector<std::future<void>> futures;
	futures.reserve(chunk.columns.size());

	for (i=0; i<chunk.columns.size(); i++) {
		futures.emplace_back(scheduler::async([&] (auto i) {
			prepare_column(i, chunk.columns[i], true);

			std::unique_lock<std::mutex> lock(mutex);
			write_column(i);
		}, i));
	}

	for (auto& f : futures) {
		f.wait();
	}
}

void
ChunkedFileWriter::write_column(size_t col_id)
{
	const auto& state = *col_states[col_id];

	f.write((char*)&state.header, sizeof(state.header));
	f.write((char*)state.dest_buffer, state.header.compr_payload_bytes);
}

void
ChunkedFileWriter::prepare_column(size_t col_id, const ColumnSlice& col,
	bool expensive)
{
	auto& state = *col_states[col_id];
	memset(&state, 0, sizeof(state));

	ColumnHeader& header = state.header;
	header.reset();

	header.uncompr_payload_bytes = col.size;
	header.compr_payload_bytes = col.size;
	header.column_id = col_id;
	header.crc32 = get_crc32(col.buffer, header.uncompr_payload_bytes);

	header.codec = kCodecLZ4;
	if (header.codec == kCodecLZ4 &&
			header.uncompr_payload_bytes > LZ4_MAX_INPUT_SIZE) {
		header.codec = kCodecNone;
	}

	state.dest_buffer = col.buffer;

	size_t compressed_sz;
	double ratio;

	switch (header.codec) {
	case kCodecLZ4:
		compressed_sz = LZ4_compressBound(header.uncompr_payload_bytes);
		if (state.buf.size() < compressed_sz) {
			state.buf.resize(compressed_sz);
		}

		if (expensive) {
			compressed_sz = LZ4_compress_HC(col.buffer,
				&state.buf[0], header.uncompr_payload_bytes, compressed_sz,
				LZ4HC_CLEVEL_OPT_MIN);
		} else {
			compressed_sz = LZ4_compress_default(col.buffer,
				&state.buf[0], header.uncompr_payload_bytes, compressed_sz);
		}

		ratio = (double)header.uncompr_payload_bytes /
			(double)compressed_sz;

		if (ratio > 1.5) {
			state.dest_buffer = &state.buf[0];
			header.compr_payload_bytes = compressed_sz;
		} else {
			// not at least 2x smaller, give up
			header.codec = kCodecNone;
			state.dest_buffer = col.buffer;
			header.compr_payload_bytes = header.uncompr_payload_bytes;
			LOG_WARN("Compression ratio (%f) not good enough. Fallback to uncompressed",
				ratio);
		}

		break;

	case kCodecNone:
		break;
	}
}


ChunkedFileWriter::~ChunkedFileWriter()
{
	f.close();
}
