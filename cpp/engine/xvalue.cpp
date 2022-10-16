#include "xvalue.hpp"
#include "types.hpp"

#include "system/system.hpp"
#include "system/memory.hpp"

#include "engine/util_kernels.hpp"

#include <cstring>

using namespace engine;

static const size_t kVectorAlignment = 64; 

XValue::XValue(memory::Context& mem, Tag tag)
 : tag(tag), m_mem(mem)
{
	m_self_allocated = false;
	m_data = nullptr;
	m_data_type = nullptr;
	m_width = 0;
	dbg_name = nullptr;

	reset();
}

void
XValue::alloc(Type& t, size_t vsize)
{
	reset();
	set_data_type(&t);

	alloc(t.get_width(), vsize);
}

void
XValue::alloc(size_t width, size_t vsize)
{
	m_self_allocated = true;
	m_capacity = vsize;
	m_width = width;

	if (m_width * vsize > 0) {
		m_data = (char*)m_mem.array_alloc(vsize, m_width,
			std::max(m_width, kVectorAlignment),
			memory::Context::kZero | memory::Context::kNoRealloc);
	} else {
		m_data = nullptr;
	}
	LOG_TRACE("XValue::alloc: width %llu, vsize %llu, data %p",
		m_width, vsize, m_data);

	first = m_data;
}

void
XValue::reset()
{
	if (m_self_allocated && m_data) {
		m_mem.free(m_data);
	}
	m_self_allocated = false;
	num = 0;
	m_capacity = 0;
	m_data = nullptr;
	first = nullptr;
	m_width = 0;
	done = false;
	offset = 0;
	m_flags = 0;
}

void
XValue::set_data_type(Type* t)
{
	ASSERT(t);
	m_width = t->get_width();
	m_data_type = t;
}

XValue::~XValue()
{
	reset();
}

void
XValue::broadcast(char* data, size_t width, size_t num)
{
	engine::util_kernels::broadcast((char*)first, data, width, num);
}

using namespace std;

void
XValue::dbg_print(std::ostringstream& s, const std::string& line_prefix) const
{
	const auto& lp = line_prefix;
	std::string type_str(m_data_type ? m_data_type->to_string() : "NULL");
	std::string dbg_name_str(dbg_name ? dbg_name : "NULL");
	s
		<< lp << "XValue {" << endl
		<< lp << "  dbg_name='" << dbg_name_str << "', tag=" << tag2string(tag) << endl
		<< lp << "  type=" << type_str << ", first=" << first << endl
		<< lp << "  num=" << num << ", capacity=" << m_capacity << ", done=" << done << endl;

	if (tag == kVector || tag == kPredicate) {
		Type* type = tag == kPredicate ? TypeSystem::new_predicate() : m_data_type;
		bool can_print = type->get_tag() != Type::Tag::kEmpty;

		if (can_print) {
			if (first) {
				s << lp << "  values={ ";
				for (size_t i=0; i<m_capacity; i++) {
					if (i) {
						s << ", ";
					}
					char* ptr = (char*)first + get_width() * i;
					type->print_to_stream(s, ptr);
				}
				s << " }" << endl;
			} else {
				s << lp << "  values=NULL " << endl;
			}
		}
	} else if (tag == kPosition) {
		s << lp << "  position_dest=" << first << endl;
	}

	s
		<< lp << "}";
}

void
XValue::validate() const
{
	if (tag == kPredicate) {
		engine::util_kernels::assert_valid_selection_vector<selvector_t>(
			get_first_as<selvector_t>(),
			get_num(),
			[&] (auto i) { ASSERT(false); });
	}
}

void
XValue::_copy(XValue& dest, const XValue& src, XValue* pred)
{
	ASSERT(dest.first && src.first);
	if (!pred || !pred->first) {
		_copy_full_memcpy(dest, src);
		return;
	}

	selvector_t* sel = nullptr;
	size_t num = src.m_capacity;

	if (pred && pred->first) {
		sel = (selvector_t*)pred->first;
		num = pred->num;
	}

	engine::util_kernels::copy<selvector_t>((char*)dest.first,
		(char*)src.first, src.m_width, sel, num);
}