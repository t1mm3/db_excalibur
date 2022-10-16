#include "scan_interface.hpp"
#include "engine/catalog.hpp"
#include "system/system.hpp"
#include "flat_array_scan.hpp"
#include "scan_thread_tracker.hpp"

using namespace engine::storage;

size_t
ScanThread::next(size_t& offset, ScanThread* thread, size_t morsel)
{
	return scan->next(offset, thread, morsel);
}

engine::catalog::Column*
ScanThread::get_catalog_column(size_t i) const
{
	return scan->get_catalog_column(i);
}


double
ScanThread::get_progress() const
{
	return scan->get_progress();
}

ScanThread::ScanThread(const std::shared_ptr<engine::storage::ScanInterface>& scan)
 : scan(scan)
{
	scan->register_scan(this);
}

ScanThread::~ScanThread()
{
	scan->unregister_scan(this);
}



ScanInterface::ScanInterface(const Args& args)
 : table_size(args.table->get_num_rows()), table_size_inv(1.0/((double)args.table->get_num_rows()))
{
	progress_read_position = 0;
	threads.reserve(64);
}

void
ScanInterface::register_scan(ScanThread* thread)
{
	ASSERT(!thread->scan_interface_registered);
	ASSERT(!thread->scan_interface_index);

	std::unique_lock lock(mutex);


	// try to reuse empty slots
	size_t i=0;
	ScanThread** ts = &threads[0];
	for (i=0; i<threads.size(); i++) {
		if (!ts[i]) {
			break;
		}
	}
	thread->scan_interface_index = i;
	thread->scan_interface_registered = true;
	if (i == threads.size()) {
		threads.push_back(thread);
	} else {
		ASSERT(i < threads.size());
		threads[i] = thread;
	}
}

void
ScanInterface::unregister_scan(ScanThread* thread)
{
	ASSERT(thread->scan_interface_registered);

	std::unique_lock lock(mutex);

	ASSERT(thread->scan_interface_index < threads.size());
	threads[thread->scan_interface_index] = nullptr;

	thread->scan_interface_index = 0;
	thread->scan_interface_registered = false;
}



using FlatArrayTable = engine::catalog::FlatArrayTable;
std::shared_ptr<ScanInterface>
ScanInterface::get(const Args& args)
{
	auto flat_array_table = dynamic_cast<FlatArrayTable*>(args.table);
	if (flat_array_table) {
		return std::make_shared<ScanThreadTracker<FlatArrayScan>>(args);
	}
	ASSERT(false);
	return nullptr;
}
