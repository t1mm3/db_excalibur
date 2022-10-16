#include "sql_types.hpp"
#include "types.hpp"
#include "utils.hpp"

#include <cmath>
#include <cstring>
#include <sstream>

#include "system/system.hpp"
#include "date_utils.hpp"

using namespace engine;

std::unique_ptr<SqlType>
SqlType::from_string(const std::string& _t)
{
	std::string t(StringUtils::to_lower_case(_t));

	if (!t.compare("bigint") || !t.compare("i64")) {
		return std::make_unique<Integer>(8);
	}

	if (!t.compare("integer") || !t.compare("int") || !t.compare("i32")) {
		return std::make_unique<Integer>(4);
	}

	if (!t.compare("smallint") || !t.compare("i16")) {
		return std::make_unique<Integer>(2);
	}

	if (!t.compare("tinyint") || !t.compare("i8")) {
		return std::make_unique<Integer>(1);
	}

	if (!t.compare("date")) {
		return std::make_unique<Date>();
	}

	if (!t.compare("text")) {
		return std::make_unique<String>(0);
	}

	int digits;
	int precision;

	int r = std::sscanf(t.c_str(), "decimal(%d,%d)", &digits, &precision);
	ASSERT(r >= 0 && r <= 2);

	if (r == 1) {
		precision = 0;
	}

	if (r == 1 || r == 2) {
		ASSERT(digits >= 0);
		ASSERT(precision >= 0);
		ASSERT(digits >= precision);
		return std::make_unique<Decimal>(digits, precision);
	}

	r = std::sscanf(t.c_str(), "varchar(%d)", &digits);
	ASSERT(r == 0 || r == 1);
	if (r == 1) {
		ASSERT(digits >= 0);
		return std::make_unique<String>(digits);
	}

	r = std::sscanf(t.c_str(), "char(%d)", &digits);
	ASSERT(r == 0 || r == 1);
	if (r == 1) {
		ASSERT(digits >= 0);
		if (digits == 1) {
			return std::make_unique<FixedLenChar>(1);
		}
		return std::make_unique<String>(digits);
	}

	LOG_ERROR("Did not find sql type '%s'", t.c_str());

	ASSERT(false && "Invalid type");
	return nullptr;
}

std::string
SqlType::internal_parse_into_string(const std::string& s) const
{
	const size_t kSize = 64;
	char buf[kSize];
	memset(&buf[0], 0, sizeof(kSize));
	bool can_parse = parse(&buf[0], s);
	ASSERT(can_parse);

	return internal_serialize_to_string(&buf[0]);
}

std::string
SqlType::internal_serialize_to_string(void* ptr) const
{
	ASSERT(type && ptr);
	return type->print_to_string(ptr);
}

Type*
SqlType::get_physical_type()
{
	ASSERT(type);
	return type;
}



Decimal::Decimal(size_t digits, size_t precision)
 : digits(digits), precision(precision)
{
	ASSERT(digits < 100);
	ASSERT(precision <= digits);

	multiplier = pow(10, precision);
	double max = pow(10, digits);
	type = TypeSystem::new_integer(-max, max+1);
	ASSERT(type);
}

template<typename T>
T pow10(int exp)
{
	T r = 1;
	for (int i=0; i<exp; i++) {
		r *= 10;
	}
	return r;
}

template<typename T>
int num_digits(T x)
{
	int r = 0;
	while (x > 0) {
		r++;
		x /= 10;
	}
	return r;
}

inline void
parse_digits(const char*& s, int64_t& value, int& count)
{
	value = 0;
	count = 0;
	while (isdigit(*s)) {
		value = value * 10 + (*s++ - '0');
		count++;
	}
}

static void
parse_decimal(int64_t& val, int64_t& frac, int& frac_digits, const char* s)
{
	bool sign;
	switch (*s) {
	case '-':
		s++; 
		sign = false;
		break;
	case '+':
		s++;
		sign = true;
		break;
	default:
		sign = true;
		break;
	}

	int val_digits;
	parse_digits(s, val, val_digits);
	if (!sign) {
		val = -val;
	}

	frac_digits = 0;
	frac = 0;

	if (*s && *s == '.') {
		++s;
		parse_digits(s, frac, frac_digits);
	}
}

bool
Decimal::parse(void* out_ptr, const std::string& s) const
{
	ASSERT(precision <= digits);

	int64_t value, frac;
	int frac_digits;

	parse_decimal(value, frac, frac_digits, s.c_str());

	int64_t k = 1;
	int64_t trail_miss_digits = precision - frac_digits;

	if (frac) {
		ASSERT(trail_miss_digits >= 0);
		while (trail_miss_digits > 0) {
			k *= 10;
			trail_miss_digits--;
		}
	}

	int64_t integer = value * multiplier + frac * k;

	LOG_TRACE("'%s' trail_miss_digits %lld, frac_digits %lld, diff %lld, digits %lld, precision %lld -> %lld",
		s.c_str(), trail_miss_digits, frac_digits, digits - precision, digits, precision, integer);

	return type->parse_from_string(out_ptr, nullptr,
		std::to_string(integer));
}



Integer::Integer(size_t bytes)
{
	type = TypeSystem::new_integer_bytes(bytes);
	ASSERT(type);
}

bool
Integer::parse(void* out_ptr, const std::string& s) const
{
	return type->parse_from_string(out_ptr, nullptr, s);
}



FixedLenChar::FixedLenChar(size_t len)
{
	ASSERT(len == 1 && "Support bigger lengths for fixed-length strings");
	type = TypeSystem::new_integer_bytes(len);
	ASSERT(type);
}

bool
FixedLenChar::parse(void* out_ptr, const std::string& s) const
{
	ASSERT(type->get_width() == 1);
	ASSERT(s.size() == 1);

	auto p = (char*)out_ptr;
	*p = s[0];

	return true;
}



String::String(size_t len)
{
	type = TypeSystem::new_string();
	ASSERT(type);
}

bool
String::parse(void* out_ptr, const std::string& s) const
{
	auto out = (void**)out_ptr;
	*out = (void*)s.c_str();
	return true;
}



Date::Date()
{
	type = TypeSystem::new_integer_bytes(4);
	ASSERT(type);
}


bool
Date::parse(void* out_ptr, const std::string& s) const
{
	int year = 0;
	int month = 0;
	int day = 0;

	int r = std::sscanf(s.c_str(), "%d-%d-%d", &year, &month, &day);
	ASSERT(r == 3);

	auto out = (int32_t*)out_ptr;
	*out = DateUtils::mergeJulianDay(year, month, day);
	return true;
}
