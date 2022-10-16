#pragma once

#include <string>
#include <cstring>
#include <sstream>

namespace memory {
struct Context;
}

namespace engine {

struct Type;

struct XValue {
	void* first;
	size_t num;

#define EXPAND_XVALUE_TAG(F, ARG) \
	F(Undefined, 0, ARG) \
	F(Dummy, 1, ARG) \
	F(Vector, 2, ARG) \
	F(Position, 3, ARG) \
	F(Tuple, 4, ARG) \
	F(Predicate, 5, ARG)

	enum Tag {
#define F(NAME, VAL, ARG) k##NAME = VAL,
		EXPAND_XVALUE_TAG(F, _)
#undef F
	};

	static std::string tag2string(Tag t) {
		switch (t) {
#define F(NAME, VAL, ARG) case k##NAME: return #NAME;
		EXPAND_XVALUE_TAG(F, _)
#undef F
		default:
			return "";
		}
	}

#undef EXPAND_XVALUE_TAG

	Tag tag;

	bool done;

	//!< Read/write offset, if Position
	int64_t offset;

	const char* dbg_name;

	//! Keep some extra pointer for tranferring, e.g. ReadPosition
	void* side_pointer = nullptr;

private:
	Type* m_data_type;
	size_t m_capacity;
	bool m_self_allocated;
	char* m_data;
	size_t m_width;

	memory::Context& m_mem;

public:
	void alloc(Type& t, size_t vsize);
	void alloc(size_t width, size_t vsize);

	size_t get_num() const { return num; }
	size_t get_capacity() const { return m_capacity; }
	void* get_first() const { return first; }
	size_t get_width() const { return m_width; }

	template<typename T> T* get_first_as() const { return static_cast<T*>(first); }

	void set_data_type(Type* t);
	Type* get_data_type() const { return m_data_type; }

	void reset();

	void broadcast(char* data, size_t width, size_t num);

	template<bool CAN_HAVE_PREDICATE = true>
	static void copy(XValue& dest, const XValue& src, XValue* pred = nullptr) {
		dest.num = src.num;
		dest.tag = src.tag;
		dest.done = src.done;
		dest.dbg_name = src.dbg_name;
		if (src.first) {
			dest.first = dest.m_data;
		} else {
			dest.first = nullptr;
		}
		if (src.first && !CAN_HAVE_PREDICATE) {
			_copy_full_memcpy(dest, src);
			return;
		}

		return _copy(dest, src, pred);
	}

	void dbg_print(std::ostringstream& s, const std::string& line_prefix = "") const;
	void validate() const;


	XValue(memory::Context& mem, Tag tag);
	~XValue();

	void set_first_to_allocated_data() {
		first = m_data;
	}


	typedef uint64_t ModifyFlags;

	static constexpr ModifyFlags kConstStruct = 1ull << 1ull;
	static constexpr ModifyFlags kConstVector = 1ull << 2ull;

	void add_flags(ModifyFlags flags) {
		m_flags |= flags;
	}

	ModifyFlags get_flags() const { return m_flags; }

	bool match_flags(ModifyFlags flags) const { return m_flags & flags; }

private:
	ModifyFlags m_flags;

	static void _copy_full_memcpy(XValue& dest, const XValue& src) {
		memcpy(dest.first, src.first, src.m_width*src.m_capacity);
	}
	static void _copy(XValue& dest, const XValue& src, XValue* pred);
};
	
}
