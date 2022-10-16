#pragma once

#include <atomic>
#include <string>
#include <memory>
#include <vector>
#include <mutex>

namespace engine {
namespace catalog {
struct Table;
struct Column;
} /* storage */
} /* engine */

namespace engine {
namespace storage {

struct ScanInterface;
struct ScanTrackerInfo;

struct ScanThread {
	//! Forces ScanThread to update pointers to data
	virtual void set_columnar_data(char** data, size_t num_cols) = 0;

	// Calls ScanInterface::next
	size_t next(size_t& offset, ScanThread* thread, size_t morsel);

	// Calls ScanInterface::get_catalog_column()
	catalog::Column* get_catalog_column(size_t i) const;

	ScanThread(const std::shared_ptr<engine::storage::ScanInterface>& scan);
	virtual ~ScanThread();

	double get_progress() const;
private:
	friend struct ScanInterface;
	friend struct ScanTrackerInfo;

	size_t scan_interface_index = 0;
	bool scan_interface_registered = false;

	// modified from ScanThreadTracker
	ScanTrackerInfo* scan_tracker_info = nullptr;

	std::shared_ptr<engine::storage::ScanInterface> scan;

};

struct ScanInterface {
	//! Fetches next data, calls set_columnar_data() on 'thread'
	virtual size_t next(size_t& offset, ScanThread* thread, size_t morsel) = 0;

	virtual void register_scan(ScanThread* thread);
	virtual void unregister_scan(ScanThread* thread);

	virtual ~ScanInterface() = default;

	virtual size_t get_max_tuples() const {
		return table_size;
	}


	struct Args {
		catalog::Table* table;
		const std::vector<std::string>& cols;
	};

	static std::shared_ptr<ScanInterface> get(const Args& args);

	catalog::Column* get_catalog_column(size_t i) const {
		return col_catalog[i];
	}

	//! Progress as fraction
	double get_progress() const {
		auto read = progress_read_position.load();
		if (read >= table_size) {
			return 1.0;
		} else {
			return ((double)read) * table_size_inv;
		}
	}

protected:
	ScanInterface(const Args& args);

	std::atomic<size_t> progress_read_position;

	const size_t table_size;
	const double table_size_inv;

	std::vector<catalog::Column*> col_catalog;
	std::vector<size_t> col_width;
	std::vector<ScanThread*> threads;

	std::mutex mutex;
};

} /* storage */
} /* engine */