#include "memory.hpp"
#include "system.hpp"
#include "build.hpp"
#include "scheduler.hpp"

#include <unordered_set>
#include <sstream>

#ifdef HAS_LIBNUMA
#include <numa.h>
#include <numaif.h>
#endif

#ifdef HAVE_POSIX_MMAP
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <mutex>
#include <cstring>

using namespace memory;

static const size_t kGigaByteDivider = 1024 * 1024 * 1024ull;

static std::mutex g_mutex;
static bool g_numa_initialized = false;
static bool g_has_numa = false;

static constexpr int kTypeBaseAlloc = 0;

#ifdef HAVE_POSIX_MMAP
static constexpr int kTypeMmap = 1;
#endif


#ifdef HAS_LIBNUMA
static constexpr int kTypeNumaAlloc = 2;
#endif

template<typename T>
T round_up(T x, T upto)
{
	return ((x + upto-1) / upto) * upto;
}

static const size_t kPageSize = 2*1024*1024;


#ifdef HAVE_POSIX_MMAP
static const int kPosixMmapProtectionFlags = PROT_READ|PROT_WRITE;
#endif

static void* posix_mmap(size_t size, int mmap_flags)
{
	void* p = MAP_FAILED;

#ifdef HAVE_POSIX_MAP_ANONYMOUS
	p = mmap(0, size, PROT_READ|PROT_WRITE,
		mmap_flags|MAP_ANONYMOUS, -1, 0);
#else
	int fd = open("/dev/zero", O_RDWR);
	p = mmap(0, size, PROT_READ|PROT_WRITE,
		mmap_flags, fd, 0);
	close(fd);
#endif
	return p;
}

Buffer::Buffer(size_t size, uint64_t flags)
 : m_flags(flags)
{
	bool already_zero = false;

	m_user_size = size;
	m_data = nullptr;
	m_alloc_type = kTypeBaseAlloc;

#ifdef HAVE_POSIX_MMAP
	if (!m_data && (size >= 32*1024*1024 || flags & kNumaInterleaved)) {
		int mmap_flags = MAP_PRIVATE;

#ifdef HAVE_LINUX_MAP_POPULATE
		mmap_flags |= MAP_POPULATE;
#endif

		m_allocated_size = round_up<size_t>(size, kPageSize);

		ASSERT(m_allocated_size > 0);

		void* p = posix_mmap(m_allocated_size, mmap_flags);
		if (p == MAP_FAILED) {
			GlobalAllocator::get().print_oom_information("Buffer");
			throw OutOfMemory();
		}

#ifdef HAVE_LINUX_MADV_HUGEPAGE
		madvise(p, m_allocated_size, MADV_HUGEPAGE);
#endif

#ifdef HAS_LIBNUMA
		// try to move memory when we already populated it
		if ((mmap_flags & MAP_POPULATE) && (flags & kNumaInterleaved)) {
			auto bitmask = numa_get_mems_allowed();
			auto mask = bitmask->maskp;
			auto maxnode = bitmask->size;

			int r = mbind(p, m_allocated_size, MPOL_INTERLEAVE,
				mask, maxnode, MPOL_MF_MOVE);
			ASSERT(r != -1);
		}
#endif


		m_alloc_type = kTypeMmap;
		already_zero = true;
		m_data = (char*)p;
	}
#endif

#ifdef HAS_LIBNUMA
	if (!m_data && flags & kNumaInterleaved) {
		if (!g_numa_initialized) {
			std::lock_guard<std::mutex> guard(g_mutex);

			g_has_numa = numa_available() != -1;
			g_numa_initialized = g_has_numa;
		}

		m_allocated_size = size;
		m_data = (char*)numa_alloc_interleaved(m_allocated_size);
		if (!m_data) {
			GlobalAllocator::get().print_oom_information("Buffer");
			throw NumaAllocOOM();
		}

		m_alloc_type = kTypeNumaAlloc;
	}
#endif

	if (!m_data) {
		m_allocated_size = round_up<size_t>(size, 8);
		m_data = (char*)BaseAllocator::aligned_alloc(m_allocated_size, 8);
		m_alloc_type = kTypeBaseAlloc;
		already_zero = false;
	}

	if (m_data && !already_zero && (flags & kZero)) {
		memset(m_data, 0, m_user_size);
	}
}

bool
Buffer::resize(size_t& final_new_size, size_t expected_new_size)
{
	size_t old_size = m_allocated_size;
	size_t new_size = round_up<size_t>(expected_new_size, kPageSize);

	if (new_size == old_size || expected_new_size == old_size) {
		final_new_size = expected_new_size;
		return true;
	}

	bool new_alloc_is_zero = false;

	ASSERT(!(m_flags & kZero) && "TODO: somehow handle zeroing");
	ASSERT(!(m_flags & kNumaInterleaved) && "TODO: somehow handle interleaving");

	switch (m_alloc_type) {
#ifdef HAVE_POSIX_MMAP
	case kTypeMmap:
		{
			// TODO: MAP_POPULATE too?
#ifdef HAVE_LINUX_MREMAP
			// for example see
			// https://stackoverflow.com/questions/23668080/memory-reallocation-using-mremap
			void* p = mremap((void*)m_data, m_allocated_size, new_size,
				MREMAP_MAYMOVE, nullptr);
			if (p == MAP_FAILED) {
				LOG_ERROR("Buffer::resize(mmap): failed");
				return false;
			}

			m_data = (char*)p;
			new_alloc_is_zero = true;
			m_allocated_size = new_size;
#else
			return false;
#endif
		}
		break;
#endif

#ifdef HAS_LIBNUMA
	case kTypeNumaAlloc:
		{
			void* p = numa_realloc((void*)m_data, old_size, new_size);
			if (!p) {
				return false;
			}
			m_data = (char*)p;
			m_allocated_size = new_size;
			new_alloc_is_zero = true;
			break;
		}

#endif

	default:
		// bail out
		return false;
	}

	final_new_size = new_size;

	if (!new_alloc_is_zero && old_size < new_size && (m_flags & kZero)) {
		memset(m_data + old_size, 0, new_size-old_size);
	}
	return true;
}

Buffer::~Buffer()
{
#ifdef HAVE_POSIX_MMAP
	if (m_data && m_alloc_type == kTypeMmap) {
		munmap(m_data, m_allocated_size);
		m_data = nullptr;
		m_allocated_size = 0;
	}
#endif

#ifdef HAS_LIBNUMA
	if (m_data && m_alloc_type == kTypeNumaAlloc) {
		numa_free(m_data, m_allocated_size);
		m_data = nullptr;
		m_allocated_size = 0;
	}
#endif

	if (m_data) {
		ASSERT(m_alloc_type == kTypeBaseAlloc);
		BaseAllocator::free(m_data);
		m_data = nullptr;
		m_allocated_size = 0;
	}
}


void*
BaseAllocator::calloc(size_t num, size_t width)
{
	void* p = ::calloc(num, width);
	if (!p) {
		throw OutOfMemory();
	}
	return p;
}

void*
BaseAllocator::aligned_alloc(size_t bytes, size_t align)
{
	size_t alloc_size = bytes;
	// must be multiple of alignment
	alloc_size += align-1;
	alloc_size /= align;
	alloc_size *= align;

	// allocate
	void* p = ::aligned_alloc(align, alloc_size);
	if (!p) {
		throw OutOfMemory();
	}
	return p;
}

void
BaseAllocator::free(void* p)
{
	if (!p) {
		return;
	}
	::free(p);
}

void
BaseAllocator::memset0(void* p, size_t bytes)
{
	memset(p, 0, bytes);
}

static const size_t kAssumedMallocOverhead = 128;
static const size_t kAllocGranularity = 2*1024*1024;
static const size_t kFirstAllocMinBytes = 8*1024 - kAssumedMallocOverhead;
static const size_t kFirstAllocMaxBytes = 64*1024 - kAssumedMallocOverhead;


inline static char*
align_pointer(char* p, size_t align)
{
	uintptr_t z = (uintptr_t)p;

	if (align && !(align & (align - 1))) {
		// is power of 2
		z = (z + align - 1) & (-align);
	} else {
		z = ((z + align - 1) / align) * align;
	}

	return (char*)z;
}

namespace memory {

struct ContextChunk {
	static const uint64_t kMagicCode = 0xDEADBEEF;
	uint64_t magic_code = kMagicCode;

	size_t filled_bytes = 0;
	size_t usable_bytes = 0;
	char* data_start = nullptr;
	int64_t num_allocations = 0;

	ContextChunk* next = nullptr;
	ContextChunk* prev = nullptr;

	bool self_allocated;
	char* alloc_ptr;

	// generic stuff
	size_t alloc_size;
	void* alloc_alloc;
	int64_t alloc_numa;

	struct Trailer {
		static const uint64_t kMagicCode = 0xBADF00D;
		uint64_t magic_code = kMagicCode;
	};
	Trailer* trailer = nullptr;

	struct ObjectTrailer;
	struct ObjectHeader {
		ContextChunk* chunk;
		size_t size;
		ObjectTrailer* trailer;
	};

	struct ObjectTrailer {
		ObjectHeader* header;
	};


	size_t free() const { return usable_bytes - filled_bytes; }

	static ObjectHeader* get_object_header(void* ptr) {
		return ((ObjectHeader*)ptr)-1;
	}

	void check(const char* dbg_path) const {
		bool t = trailer->magic_code == Trailer::kMagicCode;
		bool h = magic_code == kMagicCode;

		if (!h) {
			LOG_ERROR("check(%s): Header magic code does not match",
				dbg_path);
		}
		if (!t) {
			LOG_ERROR("check(%s): Trailer magic code does not match",
				dbg_path);
		}
		ASSERT(magic_code == kMagicCode);
		ASSERT(trailer && trailer->magic_code == Trailer::kMagicCode);
	}

	void* alloc(size_t bytes, size_t align, size_t* excl_max_bytes) {
		LOG_TRACE("ContextChunk: alloc(bytes=%llu, align=%llu) with free %llu",
			bytes, align, free());

		check("alloc");

		// get aligned pointer
		char* p = data_start + filled_bytes + sizeof(ObjectHeader);
		char* z = align_pointer(p, align);

		const size_t overhead = sizeof(ObjectHeader) +
			sizeof(ObjectTrailer) +
			// alignment overhead
			(z - p);

		size_t sz = bytes + overhead;

		if (UNLIKELY(free() < sz)) {
			return nullptr;
		}

		if (excl_max_bytes) {
			// allocate max size
			sz = free();

			// subtract overhead
			*excl_max_bytes = sz - overhead;
			bytes = *excl_max_bytes;
		}

		filled_bytes += sz;
		num_allocations++;

		// fill header
		auto obj_header = get_object_header((void*)z);
		obj_header->chunk = this;
		obj_header->size = bytes;
		obj_header->trailer = (ObjectTrailer*)(z + bytes);
		obj_header->trailer->header = obj_header;

		ASSERT((size_t)z >= (size_t)obj_header + sizeof(ObjectHeader));
		ASSERT((size_t)obj_header >= (size_t)data_start);
#ifdef IS_DEBUG_BUILD
		DBG_ASSERT((size_t)z % align == 0);
#endif
		if (excl_max_bytes) {
			ASSERT(free() == 0);
			ASSERT((size_t)z + *excl_max_bytes + sizeof(ObjectTrailer) == (size_t)data_start + usable_bytes);
		}
		return z;
	}

	ContextChunk(bool self_allocated, char* alloc_ptr)
	 : self_allocated(self_allocated),
		alloc_ptr(alloc_ptr)
	{

	}

	~ContextChunk() {
		trailer->~Trailer();
	}
};

}

Context::Context(Context* parent, const std::string& name, int64_t numa_node)
 : parent(parent), name(name), numa_node(numa_node)
{
	LOG_TRACE("Context::Construct %p %s",
		this, name.c_str());

	GlobalAllocator::get().register_context(*this);
}

void
Context::free_chunk(ContextChunk* curr)
{
	auto& global_alloc = GlobalAllocator::get();
	auto self_allocated = curr->self_allocated;
	auto alloc_ptr = curr->alloc_ptr;
	auto alloc_size = curr->alloc_size;
	auto alloc_numa = curr->alloc_numa;
	auto alloc_obj = curr->alloc_alloc;

	curr->~ContextChunk();
	if (self_allocated) {
		::free(alloc_ptr);
	} else {
		global_alloc.free_block(alloc_ptr,
			alloc_size, alloc_obj, alloc_numa);
	}
}

Context::~Context()
{
	ASSERT(!zombie);

	LOG_TRACE("Context::Destruct %p, %s, chunk_bytes %llu, user_bytes %llu",
		this, name.c_str(), chunk_max_bytes_allocated, user_max_bytes_allocated);

	if (parent) {
		ASSERT(!parent->zombie);
	}

	ASSERT(!num_allocations);

	ContextChunk* curr = head;
	while (curr) {
		auto next = curr->next;
		num_chunks--;
		free_chunk(curr);
		curr = next;
	}

	ASSERT(!num_chunks);
	head = nullptr;

	zombie = true;

	GlobalAllocator::get().unregister_context(*this);
}

#ifdef IS_DEBUG_BUILD
#define DBG_ENTER() ASSERT(dbg_num_parallel.fetch_add(1) == 0)
#define DBG_LEAVE() ASSERT(dbg_num_parallel.fetch_sub(1) == 1)
#else
#define DBG_ENTER() 
#define DBG_LEAVE()
#endif

void*
Context::generic_alloc(size_t* excl_max_bytes, size_t bytes,
	size_t align, AllocFlags flags)
{
	DBG_ENTER();

	void* ptr = nullptr;

	ASSERT(!zombie && bytes);

#ifdef IS_DEBUG_BUILD
	if (head) {
		head->check("generric_alloc1");
	}
#endif

	if (LIKELY(!ptr && head)) {
		ptr = head->alloc(bytes, align, excl_max_bytes);
	}

	if (UNLIKELY(!ptr)) {
		size_t min_alloc_bytes = bytes + align +
			sizeof(ContextChunk::ObjectHeader) + alignof(ContextChunk::ObjectHeader) +
			sizeof(ContextChunk) + alignof(ContextChunk) +
			sizeof(ContextChunk::Trailer) + alignof(ContextChunk::Trailer);

		// out of pre-allocated memory
		size_t min_block_size;

		if (suggested_next_block_size || min_alloc_bytes > kFirstAllocMaxBytes) {
			min_block_size = std::max(min_alloc_bytes, kAllocGranularity);
			// round up to kAllocGranularity
			min_block_size = (min_block_size + kAllocGranularity-1) / kAllocGranularity * kAllocGranularity;
		} else {
			min_block_size = std::max(kFirstAllocMinBytes, min_alloc_bytes);
			min_block_size = std::min(min_block_size, kFirstAllocMaxBytes);
			suggested_next_block_size = 1;
		}

		auto& global_alloc = GlobalAllocator::get();

		size_t alloc_bytes = 0;
		void* alloc_alloc = nullptr;
		int64_t alloc_numa = -1;

		char* block;
		bool self_allocated;
		if (numa_node >= 0 && min_block_size > kFirstAllocMaxBytes) {
			std::ostringstream ss;
			ss << name << "(" << std::hex << ((void*)this) << ")";

			block = global_alloc.new_block(min_block_size,
				alignof(ContextChunk), numa_node, ss.str(),
				&alloc_bytes, &alloc_alloc, &alloc_numa);
			self_allocated = false;

			ASSERT(!block || (alloc_bytes >= min_block_size));
		} else {
			block = (char*)malloc(min_block_size);
			self_allocated = true;
			alloc_bytes = min_block_size;
		}
		if (!block) {
			LOG_FATAL("Context::generic_alloc(bytes=%llu, align=%llu, flags=%llu): "
				"Out of memory (allocator=%s, min_block_size=%llu, alloc_bytes=%llu)",
				bytes, align, flags, self_allocated ? "malloc" : "BlockCache",
				min_block_size, alloc_bytes);

			GlobalAllocator::get().print_oom_information("Context");
			throw OutOfMemory();
		}

		chunk_cur_bytes_allocated += alloc_bytes;
		chunk_max_bytes_allocated = std::max(chunk_max_bytes_allocated, chunk_cur_bytes_allocated);

		auto chunk = new (block) ContextChunk(
			self_allocated, block);

		{
			char* obj = (char*)chunk;
			ASSERT((size_t)obj >= (size_t)block);

			size_t overhead = (obj - block) + sizeof(ContextChunk);

			ASSERT(alloc_bytes >= overhead + bytes);
			chunk->usable_bytes = alloc_bytes
				- (overhead + sizeof(ContextChunk::Trailer) +
					alignof(ContextChunk::Trailer));
			chunk->data_start = block + overhead;
			chunk->next = nullptr;
			chunk->prev = nullptr;

			chunk->alloc_size = alloc_bytes;
			chunk->alloc_alloc = alloc_alloc;
			chunk->alloc_numa = alloc_numa;

			char* trailer_ptr = chunk->data_start + chunk->usable_bytes;
			chunk->trailer = new (trailer_ptr) ContextChunk::Trailer();
			ASSERT((size_t)chunk->trailer % alignof(ContextChunk::Trailer) == 0);

			ASSERT((char*)chunk->trailer + sizeof(ContextChunk::Trailer) <= block + alloc_bytes);
		}

		// allocate
		ptr = chunk->alloc(bytes, align, excl_max_bytes);
		ASSERT(ptr);
		ASSERT(((size_t)ptr % align == 0));


		// insert
		chunk->next = head;
		chunk->prev = nullptr;

		if (head) {
			head->prev = chunk;
		}
		head = chunk;

		num_chunks++;
	}

	if (ptr) {
		// move head into right position
		ASSERT(head);

		// detach head
		auto old_head = head;
		ASSERT(!old_head->prev);
		old_head->next = nullptr;
		head = nullptr;

		auto first = old_head->next;

		// find last chunk with more free space
		auto curr = first;
		while (curr && curr->free() > old_head->free()) {
			curr = curr->next;
		}

		if (curr) {
			auto n = curr->next;
			curr->next = old_head;
			old_head->prev = curr;

			if (n) {
				n->prev = old_head;
			}
		}
		head = old_head;
		head->next = first;
		if (first) {
			first->prev = head;
		}

#ifdef IS_DEBUG_BUILD
		curr = head;
		while (curr) {
			if (curr->next) {
				ASSERT(curr->next->free() <= curr->free());
			}

			curr = curr->next;
		}
#endif
	}

	if (ptr && (flags & kZero)) {
#ifdef IS_DEBUG_BUILD
		{
			auto obj_header = ContextChunk::get_object_header(ptr);
			ASSERT(obj_header->trailer->header == obj_header);
		}
#endif

		BaseAllocator::memset0(ptr,
			excl_max_bytes ? *excl_max_bytes : bytes);

#ifdef IS_DEBUG_BUILD
		{
			auto obj_header = ContextChunk::get_object_header(ptr);
			ASSERT(obj_header->trailer->header == obj_header);
		}
#endif
	}

	if (ptr) {
		user_cur_bytes_allocated += excl_max_bytes ? *excl_max_bytes : bytes;
		user_max_bytes_allocated = std::max(user_max_bytes_allocated, user_cur_bytes_allocated);
		num_allocations++;
	}

#ifdef IS_DEBUG_BUILD
	if (head) {
		head->check("generric_alloc2");
	}
#endif

	LOG_TRACE("Context::generic_alloc(%p '%s', bytes=%llu,align=%llu,res=%p,#allocs=%llu",
		this, name.c_str(), bytes, align, ptr, num_allocations);

	DBG_LEAVE();

	return ptr;
}

void
Context::validate(const std::string& dbg_path) const
{
	ASSERT(!zombie);

	DBG_ENTER();
	ContextChunk* curr = head;
	while (curr) {
		curr->check(dbg_path.c_str());
		curr = curr->next;
	}

	DBG_LEAVE();
}

void
Context::free(void* p)
{
	if (!p) {
		return;
	}

	DBG_ENTER();

	ASSERT(!zombie);
	ASSERT(num_allocations);
	num_allocations--;

	auto obj_header = ContextChunk::get_object_header(p);
	ASSERT(obj_header->trailer->header == obj_header);

	ASSERT(user_cur_bytes_allocated >= obj_header->size);
	user_cur_bytes_allocated -= obj_header->size;
	auto chunk = obj_header->chunk;
	chunk->check("free");
	ASSERT(chunk->num_allocations);
	chunk->num_allocations--;
	if (chunk->num_allocations < 1) {
		ASSERT(!chunk->num_allocations);
		size_t chunk_bytes = chunk->alloc_size;
		// deallocate chunk
		if (chunk->prev) {
			chunk->prev->next = chunk->next;
		}
		if (chunk->next) {
			chunk->next->prev = chunk->prev;
		}
		if (chunk == head) {
			head = chunk->next;
			ASSERT(!chunk->prev);
		}

		num_chunks--;
		free_chunk(chunk);

		ASSERT(chunk_cur_bytes_allocated >= chunk_bytes);
		chunk_cur_bytes_allocated -= chunk_bytes;
	}

	DBG_LEAVE();
	// BaseAllocator::free(p);
}

#include <stdarg.h>
#include <stdio.h>

#ifndef HAVE__VSCPRINTF
static int
_vscprintf(const char *format, va_list pargs)
{
	// inspired by: https://stackoverflow.com/questions/4785381/replacement-for-ms-vscprintf-on-macos-linux
	int retval;
	va_list argcopy;
	va_copy(argcopy, pargs);
	retval = vsnprintf(NULL, 0, format, argcopy);
	va_end(argcopy);
	return retval;
}
#endif

char*
Context::sprintf(const char *format, ...)
{
	char *dest;
	int ret;
	int dest_len;
	va_list args;

	va_start(args, format);

	dest_len = _vscprintf(format, args);
	if (UNLIKELY(dest_len < 0)) {
		ASSERT(false);
		return nullptr;
	}

	// account for terminating zero
	dest_len++;

	dest = (char*)calloc(dest_len, sizeof(char*));

#ifdef HAVE_VSPRINTF_S
	ret = vsprintf_s(dest, dest_len, format, args);
#else
	ret = vsprintf(dest, format, args);
#endif
	if (UNLIKELY(ret < 0)) {
		ASSERT(false);
		return nullptr;
	}

	va_end(args);

	return dest;
}



void
UntypedArray::reserve_additional(size_t num)
{
	if (m_count + num < m_capacity) {
		return;
	}

	size_t new_capacity = m_capacity +
		std::max(m_min_grow, std::max(num, m_capacity/2));
	size_t new_size = new_capacity * m_width;
	size_t old_size = m_capacity * m_width;

	bool could_resize = false;

	if (m_buffer) {
		size_t final_new_size = 0;
		could_resize = m_buffer->resize(final_new_size, new_size);
		if (could_resize) {
			size_t final_new_capacity = final_new_size / m_width;
			ASSERT(final_new_capacity >= new_capacity);

			new_capacity = final_new_capacity;
		}
	}

	if (!could_resize) {
		auto new_buf = std::make_unique<Buffer>(new_size, kAllocFlags);
		if (m_buffer) {
			memcpy(new_buf->get_as<char>(), m_buffer->get_as<char>(), old_size);
		}
		m_buffer = std::move(new_buf);

		ASSERT(m_buffer);

		size_t final_new_capacity = m_buffer->get_allocated_size() / m_width;
		ASSERT(final_new_capacity >= new_capacity);

		new_capacity = final_new_capacity;
	}

	m_capacity = new_capacity;
	after_resize();

	ASSERT(m_buffer);
}



MemoryChunk::MemoryChunk(char* base_ptr, size_t sz)
{
	m_data_end = base_ptr + sz;

	char* alloc_ptr = (char*)this;
	alloc_ptr += sizeof(*this);

	// guess an upper bound of how many slots we need
	m_data_begin = align_pointer<char>(alloc_ptr, 8);
	m_num_slots = (m_data_end - m_data_begin) / kSlotWidth;

	// allocate slots
	m_slot_used = (bool*)alloc_ptr;
	alloc_ptr += sizeof(*m_slot_used) * m_num_slots;

	for (size_t i=0; i<m_num_slots; i++) {
		m_slot_used[i] = false;
	}

	// allocate payload data
	m_data_begin = align_pointer<char>(alloc_ptr, 8);
	m_base_ptr = base_ptr;
}

MemoryChunk::~MemoryChunk()
{

}

char*
MemoryChunk::allocate(size_t sz, size_t align)
{
	const size_t allocate_size = sizeof(AllocHeader) + sz + align;
	const size_t allocate_slots = (allocate_size + kSlotWidth-1) / kSlotWidth;

	// find matching run
	size_t run_start = 0;
	size_t run_length = 0;
	for (size_t i=0; i<m_num_slots; i++) {
		if (!m_slot_used[i]) {
			if (!run_length){
				run_start = i;
			}
			run_length++;
			if (run_length >= allocate_slots) {
				break;
			}
		} else {
			run_length = 0;
		}
	}

	// found enough slots
	if (run_length < allocate_slots) {
		LOG_DEBUG("MemoryChunk: Out of memory");
		return nullptr;
	}

	for (size_t i=run_start; i<allocate_slots; i++) {
		m_slot_used[i] = true;
	}

	char* first_data = &m_data_begin[run_start*kSlotWidth];

	first_data = align_pointer<char>(first_data, align);

	AllocHeader* header = (AllocHeader*)first_data;
	ASSERT((size_t)header >= (size_t)m_data_begin);

	header->num_slots_allocated = allocate_slots;

	return (char*)(header + 1);
}

void
MemoryChunk::free(char* ptr)
{
	AllocHeader* header = (AllocHeader*)ptr - 1;

	ASSERT((size_t)header >= (size_t)m_data_begin);
	size_t first_slot = ((char*)header - m_data_begin) / kSlotWidth;

	for (size_t i=first_slot; i<header->num_slots_allocated; i++) {
		m_slot_used[i] = false;
	}
}

MemoryChunk*
MemoryChunkFactory::alloc_chunk(size_t size)
{
	auto data = new char[size];

	return new(data) MemoryChunk(data, size);
}

void
MemoryChunkFactory::free_chunk(MemoryChunk* chunk)
{
	if (!chunk) {
		auto ptr = chunk->get_base_pointer();

		chunk->~MemoryChunk();
		delete[] ptr;
	}
}


#include <boost/core/demangle.hpp>


#define CACHE_TRACE(...) LOG_TRACE(__VA_ARGS__)

struct BlockCache {
	char* new_block(size_t bytes, size_t align,
			size_t* out_allocated_bytes,
			const std::string& dbg_name) {
		ASSERT(bytes >= sizeof(BlockNode));
		ASSERT(bytes == kAllocGranularity/* || bytes == kFirstAllocBytes*/);

		CACHE_TRACE("new_block(%p, %llu)", this, bytes);

		size_t alloced_size = 0;

		char* r;

		{
			std::unique_lock lock(mutex);
			r = get_cached_block(alloced_size, bytes);
			total_num_allocs++;
		}

		if (r) {
			LOG_TRACE("new_block(%p, %llu, free %llu)",
				r, alloced_size, large_list.length);
		} else {
			r = balloc(bytes, align, alloced_size);
			if (!r) {
				LOG_ERROR("BlockCache: OutOfMemory while allocating %llu "
					"bytes with alignment %llu. total_allocated_bytes %llu (%llu GiB)",
					bytes, align, total_allocated_bytes,
					total_allocated_bytes/(1024*1024*1024ull));
				return nullptr;
			}
		}

		if (!r) {
			alloced_size = 0;
		}

#ifdef BLOCK_INFO
		if (r) {
			auto& info = block_info[r];

			info.dbg_name = dbg_name;

#if 1
			std::ostringstream ss;

#ifdef HAS_LIBBACKTRACE
			const size_t g_backtrace_buffer_size = 128;
			void* g_backtrace_buffer[g_backtrace_buffer_size];

			int num = backtrace(g_backtrace_buffer, g_backtrace_buffer_size);

			char** strings = backtrace_symbols(g_backtrace_buffer, num);
			if (strings) {
				for (int i=0; i<num; i++) {
					auto& symbol = strings[i];

					boost::core::scoped_demangled_name demangler(symbol);

					ss << "#" << (i+1) << " " << (demangler.get() ? demangler.get() : symbol) << "\n";
				}
				free(strings);
			} else {
				ASSERT(false);
			}
#endif

			info.alloc_trace = ss.str();
#endif
		}
#endif

		if (out_allocated_bytes) {
			*out_allocated_bytes = alloced_size;
		}

		CACHE_TRACE("new_block(%llu) = %p", bytes, r);
		return r;
	}

	void free_block(char* block, size_t bytes) {
		ASSERT(bytes == kAllocGranularity);
		CACHE_TRACE("free(%p, %llu)", block, bytes);

		std::unique_lock lock(mutex);

		auto curr = (BlockNode*)block;
		curr->next = nullptr;
		curr->size = bytes;

#ifdef BLOCK_INFO
		block_info.erase(block);
#endif
		if (total_allocated_bytes >= max_size) {
			lock.unlock();
			bfree(block, bytes, false);
			return;
		}

		Item& default_block = large_list;

		curr->next = default_block.head;
		default_block.head = curr;
		default_block.length++;

		LOG_TRACE("free_block: block=%p, bytes=%llu, length=%llu",
			block, bytes, default_block.length);
	}

	void trim() {
		LOG_TRACE("trim");

		std::unique_lock lock(mutex);

		size_t num_trimmed = 0;
		size_t old_alloc = total_allocated_bytes;

		auto curr = large_list.head;
		while (curr && total_allocated_bytes >= max_size) {
			auto next = curr->next;

			curr->next = nullptr;

			bfree((char*)curr, curr->size, true);
			num_trimmed++;

			curr = next;
		}

		LOG_DEBUG("trimmed %llu blocks, freed %llu bytes",
			num_trimmed, old_alloc - total_allocated_bytes);

		large_list.head = curr;
	}

	void clear() {
		std::unique_lock lock(mutex);

		auto on_item = [&] (auto& item) {
			auto curr = item.head;
			while (curr) {
				auto next = curr->next;

				curr->next = nullptr;

				bfree((char*)curr, curr->size, true);

				curr = next;
			}

			item.head = nullptr;
		};

		on_item(large_list);
	}

	BlockCache(size_t max_size) : max_size(max_size) {

	}

	~BlockCache() {
		clear();
	}

#ifdef BLOCK_INFO
	struct BlockInfo {
		std::string alloc_trace;
		std::string dbg_name;
	};
	std::unordered_map<char*, BlockInfo> block_info;
#endif

	size_t total_allocated_bytes = 0;

	size_t get_num_free_blocks() const {
		return large_list.length;
	}

private:
	std::mutex mutex;

	struct BlockNode {
		BlockNode* next;
		size_t size;
	};

	struct Item {
		size_t size_class;
		size_t num_hits = 0;
		BlockNode* head = nullptr;
		size_t length = 0;
	};

	Item large_list;
	const size_t max_size;

	char* get_cached_block(size_t& out_alloced_size, size_t bytes) {
		Item& default_block = large_list;

		ASSERT(!default_block.length == !default_block.head);
		if (!default_block.head) {
			return nullptr;
		}
		auto curr = default_block.head;
		default_block.head = curr->next;
		default_block.length--;

		ASSERT(curr->size >= bytes);

		out_alloced_size = curr->size;

		return (char*)curr;
	}

	size_t total_num_allocs = 0;

	static const size_t kLargeSize = 2*1024*1024;
	static const size_t kMallocThreshold = 64*1024;

	enum AllocType {
		kMalloc,
		kMmap
	};

	static AllocType get_alloc_type_from_size(size_t bytes) {
#ifdef HAVE_POSIX_MMAP
		if (bytes > kMallocThreshold) {
			return AllocType::kMmap;
		}
#endif
		return AllocType::kMalloc;
	}

	char* balloc(size_t bytes, size_t align, size_t& allocated_bytes) {
		AllocType alloc_type = get_alloc_type_from_size(bytes);

		ASSERT(align <= 8);

		char* r = nullptr;

		switch (alloc_type) {
		case kMalloc:
			allocated_bytes = bytes;
			r = (char*)malloc(bytes);
			if (!r) {
				LOG_ERROR("BlockCache: balloc(malloc) failed");
			}
			break;

#ifdef HAVE_POSIX_MMAP
		case kMmap:
			{
				int mmap_flags = MAP_PRIVATE;

#ifdef HAVE_LINUX_MAP_POPULATE
				mmap_flags |= MAP_POPULATE;
#endif

				allocated_bytes = ((bytes + kLargeSize - 1) / kLargeSize) * kLargeSize;
				ASSERT(allocated_bytes > 0);

				// try portable version, fails on MacOS
				void* p = posix_mmap(allocated_bytes, mmap_flags);

				if (p != MAP_FAILED) {
					r = (char*)p;
#ifdef HAVE_LINUX_MADV_HUGEPAGE
					madvise(r, allocated_bytes, MADV_HUGEPAGE);
#endif
				} else {
					LOG_ERROR("BlockCache: balloc(mmap) failed");
				}
			}
			break;
#endif

		default:
			ASSERT(false && "wrong AllocType");
			break;
		}

		if (!r) {
			allocated_bytes = 0;
			return nullptr;
		}

		{
			std::unique_lock lock(mutex);
			ASSERT(get_alloc_type_from_size(allocated_bytes) == alloc_type);

			total_allocated_bytes += allocated_bytes;
		}


		LOG_DEBUG("real_alloc_block(%p, %llu): total_allocated_bytes=%llu (%llu GiB), free %llu",
			r, allocated_bytes, total_allocated_bytes,
			total_allocated_bytes / kGigaByteDivider,
			get_num_free_blocks());

		return r;
	}

	void bfree(char* b, size_t bytes, bool aleady_locked) {
		if (aleady_locked) {
			ASSERT(total_allocated_bytes >= bytes);
			total_allocated_bytes -= bytes;
		} else {
			std::unique_lock lock(mutex);
			ASSERT(total_allocated_bytes >= bytes);
			total_allocated_bytes -= bytes;
		}

		AllocType alloc_type = get_alloc_type_from_size(bytes);

		LOG_DEBUG("real_free_block(%p, %llu): total_allocated_bytes=%llu",
			b, bytes, total_allocated_bytes);

		switch (alloc_type) {
		case kMalloc:
			free(b);
			break;

#ifdef HAVE_POSIX_MMAP
		case kMmap:
			ASSERT((bytes % kLargeSize) == 0);
			munmap(b, bytes);
			break;
#endif

		default:
			ASSERT(false && "wrong AllocType");
			break;
		}
	}
};

#ifdef HAVE_LINUX_SYSINFO
#include <sys/sysinfo.h>
#else
#ifdef HAVE_POSIX_SYSCTL
#include <sys/types.h>
#include <sys/sysctl.h>
#else
#include <linux/sysctl.h>
#error Cannot detect total RAM
#endif
#endif

namespace memory {

struct NumaBlockCache {
private:
	struct NumaEntry {
		std::unique_ptr<BlockCache> cache;
		std::mutex mutex;
		int64_t numa_node;

		NumaEntry(int64_t id) : numa_node(id) {}
	};
	std::vector<NumaEntry*> numa_nodes;

	size_t max_memory;

public:
	char* new_block(size_t bytes, size_t align, int64_t numa, const std::string& dbg_name,
		size_t* out_allocated_bytes = nullptr, void** out_allocator = nullptr,
		int64_t* out_numa = nullptr)
	{
		ASSERT(numa >= 0 && numa <= (int64_t)numa_nodes.size());

		char* r;

		auto allocate_block = [&] (auto numa) {
			auto obj = numa_nodes[numa];

			{
				std::unique_lock lock(obj->mutex);

				if (!obj->cache) {
					obj->cache = std::make_unique<BlockCache>(
						max_memory / numa_nodes.size());

					LOG_DEBUG("New NUMACache on node %lld with %llu GiB, maxmem %llu GiB, nodes %llu",
						numa, max_memory / numa_nodes.size() / kGigaByteDivider,
						max_memory / kGigaByteDivider,
						numa_nodes.size());
				}
			}

			r = obj->cache->new_block(bytes, align, out_allocated_bytes, dbg_name);
			if (r) {
				*out_allocator = obj;
				*out_numa = numa;
			} else {
				*out_allocator = nullptr;
				*out_numa = -1;
			}
		};

		allocate_block(numa);
		if (r) {
			return r;
		}

		trim(true);

		// choose different NUMA node
		for (size_t k=0; k<numa_nodes.size(); k++) {
			int64_t new_node = (numa + k) % numa_nodes.size();
			allocate_block(new_node);
			if (r) {
				LOG_DEBUG("new_block: Allocated from different numa node %lld (given %lld)",
					new_node, numa)
				break;
			}
		}

		return r;
	}

	void free_block(char* block, size_t bytes, void* allocator, int64_t numa) {
		ASSERT(numa >= 0 && numa <= (int64_t)numa_nodes.size());

		auto obj = (NumaEntry*)allocator;
		ASSERT(obj->numa_node == numa);

		std::unique_lock lock(obj->mutex);
		obj->cache->free_block(block, bytes);
	}

	void trim(bool to_size) {
		for (auto& obj : numa_nodes) {
			std::unique_lock lock(obj->mutex);
			if (!obj->cache) {
				continue;
			}

			if (to_size) {
				obj->cache->trim();
			} else {
				obj->cache->clear();
			}
		}
	}

	void clear() {
		trim(false);
	}

	NumaBlockCache() {
		size_t total_ram = 0;
#ifdef HAVE_LINUX_SYSINFO
		{
			struct sysinfo info;
			int r = sysinfo(&info);
			ASSERT(!r);

			total_ram = info.totalram;
		}
#else
#ifdef HAVE_POSIX_SYSCTL
		{
			int mib[2] = { CTL_HW, HW_MEMSIZE };
			size_t size;
			size_t len = sizeof(size);

			int r = sysctl(mib, sizeof(mib) / sizeof(mib[0]),
				&size, &len, NULL, 0);
			ASSERT(!r);

			total_ram = size;
		}
#else
		#error Cannot detect total RAM
#endif
#endif

		ASSERT(total_ram > 0);
		max_memory = (double)total_ram * 0.60;

		LOG_INFO("NumaBlockCache: totalram %llu GiB, maxmem %llu GiB",
			total_ram / kGigaByteDivider,
			max_memory / kGigaByteDivider);

#ifdef HAS_LIBNUMA
		int r = numa_available();
		ASSERT(r != -1);

		size_t num_numa_nodes = numa_num_configured_nodes();

		// TODO: also check numa_get_mems_allowed()
#else
		size_t num_numa_nodes = 1;
#endif

		for (size_t i=0; i<num_numa_nodes; i++) {
			numa_nodes.push_back(new NumaEntry(i));
		}
	}

	~NumaBlockCache() {
		clear();
		/*
		for (auto& n : numa_nodes) {
			delete n;
		}
		*/
	}

	void print_oom_info(const std::string& prefix) {
		LOG_ERROR("NumaBlockCache: num_nodes %llu, max_memory %llu GiB",
			numa_nodes.size(), max_memory / kGigaByteDivider)

		for (auto& obj : numa_nodes) {
			std::unique_lock lock(obj->mutex);
			if (!obj->cache) {
				continue;
			}

			auto& c = *obj->cache;

			LOG_ERROR("NumaBlockCache: node %lld: allocated %llu (%llu GiB), freeblks %llu",
				obj->numa_node,
				c.total_allocated_bytes,
				c.total_allocated_bytes / kGigaByteDivider,
				c.get_num_free_blocks());
		}
	}
};
}


namespace memory {
struct GlobalAllocatorTracker {
	std::unordered_set<Context*> contexts;
};
}


GlobalAllocator&
GlobalAllocator::get()
{
	static GlobalAllocator g_allocator;
	return g_allocator;
}

GlobalAllocator::GlobalAllocator()
{
	block_cache = new NumaBlockCache();
	tracker = new GlobalAllocatorTracker();
}

GlobalAllocator::~GlobalAllocator()
{
	std::unique_lock lock(mutex);

	delete block_cache;
	block_cache = nullptr;

	delete tracker;
	tracker = nullptr;
}


void
GlobalAllocator::trim()
{
	block_cache->trim(true);
}

char*
GlobalAllocator::new_block(size_t bytes, size_t align, int64_t numa,
	const std::string& dbg_name, size_t* out_allocated_bytes, void** out_allocator,
	int64_t* out_numa)
{
	return block_cache->new_block(bytes, align, numa, dbg_name,
		out_allocated_bytes, out_allocator, out_numa);;
}

void
GlobalAllocator::free_block(char* block, size_t bytes,
	void* allocator, int64_t numa)
{
	block_cache->free_block(block, bytes, allocator, numa);
}

void
GlobalAllocator::register_context(Context& ctx)
{
#if 0
	std::unique_lock lock(mutex);

	ASSERT(tracker);
	tracker->contexts.insert(&ctx);
#endif
}

void
GlobalAllocator::unregister_context(Context& ctx)
{
#if 0
	std::unique_lock lock(mutex);

	ASSERT(tracker);
	tracker->contexts.erase(&ctx);
#endif
}

void
GlobalAllocator::print_oom_information(const std::string& prefix)
{
	std::unique_lock lock(mutex);

	ASSERT(tracker);
	for (auto& context : tracker->contexts) {
		size_t bytes = context->get_num_bytes_allocated();
		if (bytes / kGigaByteDivider < 1) {
			continue;
		}
		LOG_ERROR("%s: OOM: context '%s', bytes %llu (%llu GiB)",
			prefix.c_str(), context->get_name().c_str(),
			bytes, bytes / kGigaByteDivider);
	}

	ASSERT(block_cache);
	block_cache->print_oom_info(prefix);
}
