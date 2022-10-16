#pragma once

#include <string>
#include <vector>
#include <memory>

#include "system/build.hpp"

namespace engine {
struct StringHeap;
}

namespace engine {
struct StructThatFitsBiggestType {
	int64_t a;
	int64_t b;
};

struct Type {
	enum Tag {
		kUnknown = 0,
		kInt8,
		kInt16,
		kInt32,
		kInt64,
		kInt128,
		kString,
		kPredicate,
		kPosition,
		kHash,
		kBucket,
		kEmpty
	};

	typedef int64_t (*compare_t)(const char* ptr_a, const char* ptr_b);

	Tag get_tag() const {
		return tag;
	}

	bool is_predicate() const {
		return tag == kPredicate;
	}

	bool is_position() const {
		return tag == kPosition;
	}

	bool is_integer() const {
		return tag == kInt8 || tag == kInt16 || tag == kInt32 || tag == kInt64 || tag == kInt128;
	}

	bool is_hash() const {
		return tag == kHash;
	}

	bool is_bucket() const {
		return tag == kBucket;
	}

	bool is_empty() const {
		return tag == kEmpty;
	}

	bool is_supported_output_type() const {
		return !is_hash() && !is_bucket() && !is_position();
	}

	bool is_var_len() const {
		return tag == kString;
	}

	bool is_string() const {
		return tag == kString;
	}

	size_t num_bits() const {
		switch (tag) {
		case kInt8:		return 8;
		case kInt16:	return 16;
		case kInt32:	return 32;
		case kInt64:	return 64;
		case kInt128:	return 128;

		case kPredicate:return 8*sizeof(selvector_t);

		case kHash:
		case kBucket:
		case kString:	return 8*sizeof(char*);


		case kPosition: return 64;
		default:		return 0;
		}
	}

	size_t get_width() const {
		return num_bits()/8;
	}

	const char* to_cstring() const {
		return name.c_str();
	};

	std::string to_string() const {
		return name;
	}

	virtual void print_to_stream(std::ostream& out, void* pointer_to_value) const;
	virtual std::string print_to_string(void* pointer_to_value) const;
	virtual compare_t get_compare_func(bool invert) const;
	virtual bool parse_from_string(void* pointer_to_value, StringHeap* heap, const std::string& str_value) const;
	virtual bool parse_from_cstring(void* pointer_to_value, StringHeap* heap, const char* str_value, size_t len) const {
		return parse_from_string(pointer_to_value, heap, std::string(str_value, str_value + len));
	}

	virtual bool get_min_max(double& out_min, double& out_max, void* data, size_t num) const {
		return false;
	}

protected:
	friend struct TypeSystem;

	const Tag tag;
	const std::string name;

	Type(Tag tag, const std::string& name) : tag(tag), name(name) {
	}

	virtual ~Type() = default;
};

struct TypeSystem {
private:
	std::vector<Type*> types;
	Type* predicate_type = nullptr;
	Type* position_type = nullptr;
	Type* hash_type = nullptr;
	Type* empty_type = nullptr;
	Type* bucket_type = nullptr;
	Type* string_type = nullptr;

public:
	static Type* new_integer(double min, double max);
	static Type* new_integer_bytes(size_t bytes);
	static Type* new_position();
	static Type* new_predicate();
	static Type* new_hash();
	static Type* new_bucket();
	static Type* new_string();
	static Type* new_empty();
	static Type* from_cstring(const char* str);

	static Type* from_string(const std::string& s) {
		return from_cstring(s.c_str());
	}

	TypeSystem();
	~TypeSystem();

	static constexpr size_t kMaxTypeWidth = 16;
};

struct IntegerType : Type {
protected:
	friend struct TypeSystem;

	const double dmin;
	const double dmax;

	IntegerType(Type::Tag tag, const std::string& name, double min, double max)
	 : Type(tag, name), dmin(min), dmax(max) {

	}
};

struct StringType : Type {
protected:
	friend struct TypeSystem;

	StringType() : Type(Type::Tag::kString, "str") {
	}

public:
	bool parse_from_string(void* pointer_to_value, StringHeap* heap,
		const std::string& str_value) const final;
	void print_to_stream(std::ostream& out, void* pointer_to_value) const final;
	Type::compare_t get_compare_func(bool invert) const final ;
};

struct PositionType : Type {
private:
	friend struct TypeSystem;

	PositionType() : Type(Type::Tag::kPosition, "position") {

	}

};

} /* engine */
