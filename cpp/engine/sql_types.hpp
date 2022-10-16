#pragma once

#include <stdint.h>
#include <string>
#include <memory>
#include <vector>

namespace engine {
struct Type;

struct SqlType {
	virtual Type* get_physical_type();

	virtual bool parse(void* out_ptr, const std::string& s) const = 0;
	virtual std::string internal_serialize_to_string(void* ptr) const;

	std::string internal_parse_into_string(const std::string& s) const;

	static std::unique_ptr<SqlType> from_string(const std::string& t);

	virtual ~SqlType() = default;
protected:
	SqlType() {}

	Type* type = nullptr;
};

struct Decimal : SqlType {
	size_t digits;
	size_t precision;
	
	int64_t multiplier;

	Decimal(size_t digits, size_t precision);

	bool parse(void* out_ptr, const std::string& s) const override;
};

struct String : SqlType {
	String(size_t len = 0);

	bool parse(void* out_ptr, const std::string& s) const override;
};

struct FixedLenChar : SqlType {
	FixedLenChar(size_t len);

	bool parse(void* out_ptr, const std::string& s) const override;
};

struct Integer : SqlType {
	Integer(size_t bytes);

	bool parse(void* out_ptr, const std::string& s) const override;
};

struct Date : SqlType {
	Date();

	bool parse(void* out_ptr, const std::string& s) const override;
};

} /* engine */
