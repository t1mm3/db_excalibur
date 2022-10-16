#include "types.hpp"

#include "system/system.hpp"

#include "string_heap.hpp"
#include <iostream>
#include <sstream>
#include <limits>
#include <string.h>
#include <math.h>

using namespace engine;

#define EXPAND_INTEGER_TYPES(F, ARGS) \
	F(ARGS, int8_t, kInt8, "i8") \
	F(ARGS, int16_t, kInt16, "i16") \
	F(ARGS, int32_t, kInt32, "i32") \
	F(ARGS, int64_t, kInt64, "i64")

template<typename C_TYPE, bool PRINT_HEX>
struct SpecificIntegerType : IntegerType {
	SpecificIntegerType(Type::Tag tag, const std::string& name)
	 : IntegerType(tag, name,
	 	std::numeric_limits<C_TYPE>::min(), std::numeric_limits<C_TYPE>::max())
	{

	}

	void print_to_stream(std::ostream& out, void* pointer_to_value) const final {
		auto p = static_cast<C_TYPE*>(pointer_to_value);
		if (PRINT_HEX) {
			out << std::hex;
		} else {
			out << std::dec;
		}
		out << +(*p);
	}

	bool parse_from_string(void* pointer_to_value, StringHeap* heap,
			const std::string& str_value) const final {
		auto p = static_cast<C_TYPE*>(pointer_to_value);
		try {
			auto val = std::stoll(str_value);
			ASSERT(val >= std::numeric_limits<C_TYPE>::min());
			ASSERT(val <= std::numeric_limits<C_TYPE>::max());

			*p = val;
			return true;
		} catch (...) {
			return false;
		}
	}

	static int64_t compare_normal(const char* ptr_a, const char* ptr_b) {
		const C_TYPE* a = (C_TYPE*)ptr_a;
		const C_TYPE* b = (C_TYPE*)ptr_b;

		return *a - *b;
	}

	static int64_t compare_inverted(const char* ptr_a, const char* ptr_b) {
		return compare_normal(ptr_b, ptr_a);
	}

	Type::compare_t get_compare_func(bool invert) const final {
		return invert ? compare_inverted : compare_normal;
	}

	bool get_min_max(double& out_min, double& out_max, void* data, size_t num) const final {
		auto p = static_cast<C_TYPE*>(data);
		if (!num) {
			return false;
		}
		out_min = p[0];
		out_max = p[0];
		for (size_t i=1; i<num; i++) {
			auto d = (double)p[i];
			out_min = std::min(out_min, d);
			out_max = std::max(out_max, d);
		}
		return true;
	}

};

namespace std {
	/* from https://stackoverflow.com/questions/25114597/how-to-print-int128-in-g */
	std::ostream&
	operator<<( std::ostream& dest, __int128_t value )
	{
		std::ostream::sentry s( dest );
		if ( s ) {
			__uint128_t tmp = value < 0 ? -value : value;
			char buffer[ 128 ];
			char* d = std::end( buffer );
			do {
				-- d;
				*d = "0123456789"[ tmp % 10 ];
				tmp /= 10;
			} while ( tmp != 0 );
			if ( value < 0 ) {
				-- d;
				*d = '-';
			}
			int len = std::end( buffer ) - d;
			if ( dest.rdbuf()->sputn( d, len ) != len ) {
				dest.setstate( std::ios_base::badbit );
			}
		}
		return dest;
	}
} /* std */


struct Int128 : IntegerType {
	Int128()
	 : IntegerType(kInt128, "i128", -(pow(2, 127)+1.0), pow(2, 127)-1.0)
	{

	}

	void print_to_stream(std::ostream& out, void* pointer_to_value) const final {
		auto p = static_cast<__int128*>(pointer_to_value);

		out << std::dec;
		out << +(*p);
	}

	bool parse_from_string(void* pointer_to_value, StringHeap* heap,
			const std::string& str_value) const final {
		auto p = static_cast<__int128*>(pointer_to_value);
		try {
			// TODO: Allow bigger numbers
			auto val = std::stoll(str_value);
			*p = val;
			return true;
		} catch (...) {
			return false;
		}
	}

	static int64_t compare_normal(const char* ptr_a, const char* ptr_b) {
		const __int128* a = (__int128*)ptr_a;
		const __int128* b = (__int128*)ptr_b;

		return *a - *b;
	}

	static int64_t compare_inverted(const char* ptr_a, const char* ptr_b) {
		return compare_normal(ptr_b, ptr_a);
	}

	Type::compare_t get_compare_func(bool invert) const final {
		return invert ? compare_inverted : compare_normal;
	}

	bool get_min_max(double& out_min, double& out_max, void* data, size_t num) const final {
		auto p = static_cast<__int128*>(data);
		if (!num) {
			return false;
		}
		out_min = p[0];
		out_max = p[0];
		for (size_t i=1; i<num; i++) {
			double d = p[i];
			out_min = std::min(out_min, d);
			out_max = std::max(out_max, d);
		}
		return true;
	}

};


static int
strcicmp(char const *a, char const *b)
{
	for (;; a++, b++) {
		int d = tolower((unsigned char)*a) - tolower((unsigned char)*b);
			if (d != 0 || !*a)
		return d;
	}
}

static TypeSystem g_type_system;

TypeSystem::TypeSystem()
{
#define TYPE(_1, _2, _3, _4) {\
		types.push_back(new SpecificIntegerType<_2, false>( \
			Type::Tag::_3, _4)); \
	}

	EXPAND_INTEGER_TYPES(TYPE, 0)
#undef TYPE

	types.push_back(new Int128());

	predicate_type = new SpecificIntegerType<uint32_t, false>(Type::Tag::kPredicate, "predicate");
	types.push_back(predicate_type);

	position_type = new PositionType();
	types.push_back(position_type);

	hash_type = new SpecificIntegerType<uint64_t, true>(Type::Tag::kHash, "hash");
	types.push_back(hash_type);

	bucket_type = new SpecificIntegerType<uint64_t, true>(Type::Tag::kBucket, "bucket");
	types.push_back(bucket_type);

	empty_type = new Type(Type::Tag::kEmpty, "empty");
	types.push_back(empty_type);

	string_type = new StringType();
	types.push_back(string_type);

}

TypeSystem::~TypeSystem()
{
	for (auto& t : types) {
		delete t;
	}
}



Type*
TypeSystem::from_cstring(const char* str)
{
	for (auto& t : g_type_system.types) {
		if (!strcicmp(str, t->to_cstring())) {
			return t;
		}
	}

	THROW(Exception, "Cannot find type with given name");
	return nullptr;
}

Type*
TypeSystem::new_position()
{
	Type* r = g_type_system.position_type;

	ASSERT(r && "We should have this type");
	return r;
}

Type*
TypeSystem::new_predicate()
{
	Type* r = g_type_system.predicate_type;

	ASSERT(r && "We should have this type");
	return r;
}

Type*
TypeSystem::new_hash()
{
	Type* r = g_type_system.hash_type;

	ASSERT(r && "We should have this type");
	return r;
}

Type*
TypeSystem::new_bucket()
{
	Type* r = g_type_system.bucket_type;

	ASSERT(r && "We should have this type");
	return r;
}

Type*
TypeSystem::new_string()
{
	Type* r = g_type_system.string_type;

	ASSERT(r && "We should have this type");
	return r;
}

Type*
TypeSystem::new_empty()
{
	Type* r = g_type_system.empty_type;

	ASSERT(r && "We should have this type");
	return r;
}


Type*
TypeSystem::new_integer(double min, double max)
{
	LOG_TRACE("TypeSystem::new_integer: Find integer with min %f max %f",
		min, max);
	ASSERT(min <= max);

	for (auto& t : g_type_system.types) {
		if (!t->is_integer()) {
			continue;
		}

		auto card = static_cast<IntegerType*>(t);
		if (card->dmin <= min && max <= card->dmax) {
			LOG_TRACE("TypeSystem: chose %s for [%f, %f]",
				card->to_cstring(), min, max);
			return card;
		}
	}

	LOG_ERROR("TypeSystem: Cannot find integer type with [%f, %f]",
		min, max);
	THROW(Exception, "Cannot find type for min/max");
	return nullptr;
}

Type*
TypeSystem::new_integer_bytes(size_t bytes)
{
	for (auto& t : g_type_system.types) {
		if (!t->is_integer()) {
			continue;
		}

		auto card = static_cast<IntegerType*>(t);
		if (card->get_width() == bytes) {
			return card;
		}
	}

	THROW(Exception, "Cannot find type with bits");
	return nullptr;
}

void
Type::print_to_stream(std::ostream& out, void* pointer_to_value) const
{
	ASSERT(false && "Not implemented");
}

Type::compare_t
Type::get_compare_func(bool invert) const
{
	ASSERT(false && "Not implemented");
	return nullptr;
}

bool
Type::parse_from_string(void* pointer_to_value, StringHeap* heap,
	const std::string& str_value) const
{
	ASSERT(false && "Not implemented");
	return false;
}

std::string
Type::print_to_string(void* pointer_to_value) const
{
	std::ostringstream s;
	print_to_stream(s, pointer_to_value);

	return s.str();
}



bool
StringType::parse_from_string(void* pointer_to_value, StringHeap* heap,
	const std::string& str_value) const
{
	ASSERT(heap);
	auto dest = (char**)pointer_to_value;

	*dest = heap->add(str_value);
	return true;
}

void
StringType::print_to_stream(std::ostream& out, void* pointer_to_value) const
{
	auto p = static_cast<char**>(pointer_to_value);
	ASSERT(p && *p);
	out << std::string(*p);
}

static int64_t
str_compare_normal(const char* ptr_a, const char* ptr_b)
{
	return strcmp(ptr_a, ptr_b);
}

static int64_t
str_compare_inverted(const char* ptr_a, const char* ptr_b)
{
	return strcmp(ptr_b, ptr_a);
}

Type::compare_t
StringType::get_compare_func(bool invert) const
{
	return invert ? str_compare_inverted : str_compare_normal;
}

