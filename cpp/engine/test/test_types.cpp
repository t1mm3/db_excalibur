#include "catch.hpp"

#include "engine/types.hpp"
#include "engine/sql_types.hpp"

using namespace engine;

TEST_CASE("Same types should get same pointers", "[engine][types]")
{
	auto a = TypeSystem::new_integer(0, 56);
	auto b = TypeSystem::new_integer(0, 56);

	REQUIRE(a->is_integer());
	REQUIRE(a->num_bits() == 8);
	REQUIRE(b->is_integer());
	REQUIRE(b->num_bits() == 8);

	REQUIRE(a == b);
}


TEST_CASE("Test type from min/max", "[engine][types]")
{
	std::vector<std::tuple<double, double, size_t>> tuples = {
		{ 0.0, 56.0, 8 },
		{ -128, 127, 8 },

		{ -32768, 32767, 16 }
	};

	for (auto& t : tuples) {
		auto min = std::get<0>(t);
		auto max = std::get<1>(t);
		auto bits = std::get<2>(t);

		auto type = TypeSystem::new_integer(min, max);

		REQUIRE(type->is_integer());
		REQUIRE(type->num_bits() == bits);		
	}
}


TEST_CASE("Test type from name", "[engine][types]")
{
	std::vector<std::tuple<std::string, size_t>> tuples = {
		{ "i8", 8 },
		{ "i16", 16 },
		{ "i32", 32 },
		{ "i64", 64 }
	};

	for (auto& t : tuples) {
		auto name = std::get<0>(t);
		auto bits = std::get<1>(t);

		auto type = TypeSystem::from_string(name);

		REQUIRE(type->is_integer());
		REQUIRE(type->num_bits() == bits);
	}
}





TEST_CASE("Parse SQL types", "[engine][sqltypes]")
{
	std::vector<std::tuple<std::string, size_t>> tuples = {
		{ "i8", 8},
		{ "i16", 16 },
		{ "i32", 32 },
		{ "i64", 64 }
	};

	std::vector<char> buf;
	buf.resize(1024);

	for (auto& t : tuples) {
		auto name = std::get<0>(t);
		auto type = SqlType::from_string(name);

		REQUIRE(type);

		REQUIRE(type->parse(&buf[0], "0"));
	}

	auto type = SqlType::from_string("decimal(1,0)");
	REQUIRE(type);
	REQUIRE(type->parse(&buf[0], "0.0"));
	REQUIRE(type->parse(&buf[0], "0"));
	REQUIRE(type->parse(&buf[0], ".0"));


	type = SqlType::from_string("varchar(5)");
	REQUIRE(type);

	REQUIRE(type->parse(&buf[0], "Hallo"));
}
