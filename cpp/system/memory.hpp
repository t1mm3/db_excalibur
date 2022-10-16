#pragma once

#include <stdint.h>
#include <string>
#include <memory>
#include <string.h>
#include <cstdlib>
#include <atomic>

#include <unordered_map>
#include <vector>
#include <type_traits>
#include <cstddef>
#include <mutex>

namespace memory {

struct OutOfMemory : std::bad_alloc {
	virtual const char* what() {
		return "Out of memory";
	}
};

struct NumaAllocOOM : OutOfMemory {
	virtual const char* what() {
		return "numa_alloc() failed)";
	}
};

struct Context;

struct GlobalAllocatorTracker;
struct NumaBlockCache;
struct GlobalAllocator {
	static GlobalAllocator& get();

	void trim();

	char* new_block(size_t bytes, size_t align, int64_t numa, const std::string& dbg_name,
		size_t* out_allocated_bytes = nullptr, void** out_allocator = nullptr,
		int64_t* out_numa = nullptr);

	void free_block(char* block, size_t bytes, void* allocator, int64_t numa);

	void register_context(Context& ctx);
	void unregister_context(Context& ctx);
	void print_oom_information(const std::string& prefix);
private:
	GlobalAllocator();
	~GlobalAllocator();

	std::mutex mutex;

	NumaBlockCache* block_cache;
	GlobalAllocatorTracker* tracker;
};

struct BaseAllocator {
	static void* calloc(size_t num, size_t width);
	static void* aligned_alloc(size_t bytes, size_t align);
	static void free(void* p);
	static void memset0(void* p, size_t bytes);
};

struct ContextChunk;

struct Context {
	Context(Context* parent, const std::string& name, int64_t numa_node);
	~Context();

	typedef uint64_t AllocFlags;

	static const AllocFlags kZero = 1ull << 1;
	static const AllocFlags kIsObject = 1ull << 2;
	static const AllocFlags kNoReallocGrow = 1ull << 3;
	static const AllocFlags kNoReallocShrink = 1ull << 4;
	static const AllocFlags kNoRealloc = kNoReallocGrow | kNoReallocShrink;

	template<typename T, typename... Args>
	T* newObj(Args&&... args)
	{
		AllocFlags flags = kNoRealloc;
		if (!std::is_pod<T>::value) {
			flags |= kIsObject;
		}
		void* p = aligned_alloc(sizeof(T), alignof(T), flags);
		return new (p) T(std::forward<Args>(args)...);
	}

	template<typename T>
	void deleteObj(T* obj)
	{
		if (!obj) {
			return;
		}
		obj->~T();
		free(obj);
	}

	template<typename T>
	T* typed_array_alloc(size_t num, size_t align = 0, AllocFlags flags = 0) {
		return (T*)array_alloc(num, sizeof(T), align, flags);
	}

	void* array_alloc(size_t num, size_t width, size_t align = 0, AllocFlags flags = 0) {
		return generic_alloc(nullptr, num*width, align ? align : width, flags);
	}

	void* aligned_alloc(size_t bytes, size_t align, AllocFlags flags = 0) {
		return generic_alloc(nullptr, bytes,
			align ? align : alignof(std::max_align_t),
			flags);
	}

	void* exclusive_alloc(size_t& bytes, size_t align,
			AllocFlags flags = 0) {
		size_t min_bytes = bytes;
		return generic_alloc(&bytes, min_bytes,
			align ? align : alignof(std::max_align_t),
			flags);
	}

	void free(void* p);

	char* sprintf(const char *format, ...);

	const std::string& get_name() const { return name; }
	int64_t get_numa_node() const { return numa_node; }

	size_t get_num_bytes_allocated() const { return user_cur_bytes_allocated; }

	void validate(const std::string& dbg_path) const;
	void dbg_validate(const std::string& dbg_path) const {
#ifdef IS_DEBUG_BUILD
		validate(dbg_path);
#else
		(void)dbg_path;
#endif
	}

	Context(const Context& c) = delete;

private:
	Context* parent;
	const std::string name;
	bool zombie = false;


	void* generic_alloc(size_t* excl_max_bytes, size_t bytes, size_t align, AllocFlags flags);

	size_t chunk_cur_bytes_allocated = 0;
	size_t chunk_max_bytes_allocated = 0;
	size_t user_cur_bytes_allocated = 0;
	size_t user_max_bytes_allocated = 0;

	void free_chunk(ContextChunk* chunk);


private:
	const int64_t numa_node;
	size_t suggested_next_block_size = 0;

	ContextChunk* head = nullptr;
	size_t num_chunks = 0;
	size_t num_allocations = 0;

	mutable std::atomic<int64_t> dbg_num_parallel = { 0 };
};

struct Buffer {
	static const uint64_t kNumaInterleaved = 1 << 1;
	static const uint64_t kZero = 1 << 2;

	Buffer(size_t size, uint64_t flags);
	~Buffer();

	template<typename T>
	T* get_as() { return (T*)m_data; }

	bool resize(size_t& final_new_size, size_t expected_new_size);
	size_t get_allocated_size() const { return m_allocated_size; }

private:
	char* m_data;

	size_t m_allocated_size;
	size_t m_user_size;
	int m_alloc_type;

	const uint64_t m_flags;
};

struct UntypedArray {
	UntypedArray(size_t width, size_t initial_size = 0) {
		m_capacity = 0;
		m_width = width;
		m_count = 0;

		reserve_additional(initial_size);
	}

	void push_back_data(void* data, size_t num) {
		prealloc(num);
		insert(data, num);
	}

	void prealloc(size_t num) {
		if (m_count + num > m_capacity || !m_buffer) {
			reserve_additional(num);
		}
	}

	void* get_tail() {
		return m_buffer->get_as<char>() + m_count*m_width;
	}

	void insert(void* data, size_t num) {
		if (m_count + num > m_capacity || !m_buffer) {
			reserve_additional(num);
		}

		memcpy(get_tail(), data, num*m_width);

		m_count += num;
	}

	void reserve_additional(size_t num);

	size_t size() const {
		return m_count;
	}

	size_t capacity() const {
		return m_capacity;
	}

	void* get_data() {
		return m_buffer->get_as<void>();
	}

	void clear() {
		m_count = 0;
	}

private:
	size_t m_count;
	size_t m_capacity;
	size_t m_width;
	size_t m_min_grow = 1024;

protected:
	const uint64_t kAllocFlags = 0;
	std::unique_ptr<Buffer> m_buffer;

	virtual void after_resize() {

	}
};

template<typename T>
struct ObjectPool {
	T* alloc() {
		if (!head) {
			return nullptr;
		}
		T* r = (T*)head;
		head = head->next;
		return r;
	}

	void free(T* t) {
		if (!t) {
			return;
		}
		auto item = (Item*)t;
		item->next = head;
		head = item;
	}

	template<typename F>
	void for_each(const F& fun) {
		Item* curr = head;
		while (curr) {
			Item* next = curr->next;
			fun(curr);
			curr = next;
		}
	}

private:
	struct Item {
		Item* next;
	};

	Item* head = nullptr;

	static_assert(sizeof(T) >= sizeof(Item), "Must be large enough for one pointer");
};

template<typename T>
struct BlockedPool {
	T* alloc() {
		T* r = pool.alloc();
		if (r) {
			return r;
		}

		auto blk = mem.newObj<Block>(mem, block_size);
		blk->for_each([&] (auto x) {
			pool.free(x);
		});

		blk->next = head;
		head = blk;

		return pool.alloc();
	}

	void free(T* p) {
		pool.free(p);
	}

	BlockedPool(Context& mem, size_t block_size = 16*1024)
	 : mem(mem), block_size(block_size)
	{
	}

	~BlockedPool() {
		Block* curr = head;
		while (curr) {
			Block* next = curr->next;
			mem.deleteObj<Block>(curr);
			curr = next;
		}
	}

private:
	ObjectPool<T> pool;

	struct Block {
		T* array;
		const size_t capacity;
		Context& mem;
		Block* next = nullptr;

		Block(Context& mem, size_t cap)
		 : capacity(cap), mem(mem)
		{
			array = (T*)mem.array_alloc(capacity, width);
		}

		template<typename F>
		void for_each(const F& f) {
			for (size_t i=0; i<capacity; i++) {
				f(&array[i]);
			}
		}

		~Block() {
			mem.free(array);
		}
	};

	Block* head = nullptr;
	Context& mem;
	size_t block_size;

	static constexpr size_t width = sizeof(T);
};

#if 0
template<typename T>
struct Array : UntypedArray {
	Array(size_t initial_size = 0) : UntypedArray(sizeof(T), initial_size)
	{
	}

	void push_back(const T& x) {
		push_back_data(&x, 1);
	}

private:
	T* m_array;

protected:
	virtual void after_resize() override {
		m_array = (T*)m_buffer->get_as<T>();
	}
};

struct StringHeap {
	template<typename T, typename SEL_T>
	void insert(char** strings, SEL_T* sel, size_t num) {

	}


private:
	template<typename T, typename SEL_T>
	static T prefix_sum(T* res, T* val, SEL_T* sel, size_t num)
	{
		T r = 0;

		if (sel) {
			for (size_t i=0; i<num; i++) {
				auto k = sel[i];
				res[k] = r;
				r += val[k];
			}
		} else {
			for (size_t i=0; i<num; i++) {
				res[i] = r;
				r += val[i];
			}
		}

		return r;
	}

	struct Block {
		Block* next;
		size_t size;
	};

	Block* head = nullptr;
	Block* tail = nullptr;
};
#endif

struct MemoryChunkFactory;

struct MemoryChunk {
	char* allocate(size_t sz, size_t align = 8);
	void free(char* ptr);

protected:
	static constexpr size_t kSlotWidth = 64;

	template<typename T>
	static T*
	align_size(T p, size_t align)
	{
		p += align-1;
		p /= align;
		p *= align;

		return (T*)p;
	}

	template<typename T>
	static T*
	align_pointer(T* ptr, size_t align)
	{
		return (T*)align_size<size_t>((size_t)ptr, align);
	}

	struct AllocHeader {
		size_t num_slots_allocated;
	};

	bool* m_slot_used;
	size_t m_num_slots;

	char* m_data_begin;
	char* m_data_end;
	char* m_base_ptr;

public:
	char* get_base_pointer() const { return m_base_ptr; }
	size_t get_total_size() const { return m_data_end - m_base_ptr; }

	MemoryChunk(char* base_ptr, size_t sz);
	~MemoryChunk();
};

struct MemoryChunkFactory {
	virtual MemoryChunk* alloc_chunk(size_t size);
	virtual void free_chunk(MemoryChunk* chunk);
};

} /* memory */

#include <sanitizer/asan_interface.h>

#define MEM_POISON_REGION(addr, size) ASAN_POISON_MEMORY_REGION(addr, size)
#define MEM_UNPOISON_REGION(addr, size) ASAN_POISON_MEMORY_REGION(addr, size)