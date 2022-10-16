#include "catch.hpp"

#include <cstring>
#include <stdint.h>

#include "system/system.hpp"
#include "engine/catalog.hpp"
#include "engine/storage/csv_loader.hpp"
#include "tpch_test_utils.hpp"
#include "test_utils.hpp"

#include <map>

static std::map<std::string, engine::catalog::Catalog> catalogs;

using namespace plan;

static bool
string_contains(const std::string& a, const std::string& b)
{
	return a.find(b) != std::string::npos;
}

TEST_CASE("TPC-H SF0.01 string", "[engine][basic][string]")
{
	const std::string scale_factor = "0.01";
	const auto tpch_csv_files = TpchUtils::get_files(scale_factor);
	auto& cat = catalogs[scale_factor];
	if (!cat.get_num_tables()) {
		TpchUtils::create_schema(cat);

		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;
			auto& file_name = file_mapping.second;

			auto table = cat.get_table(table_name);
			ASSERT(table);

			engine::storage::CsvLoader csv(*table);

			csv.from_file(file_name);
			csv.flush();

			REQUIRE(table->get_num_rows() > 0);
		}
	}

	Builder builder;

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("customer equal") {
		RelOpPtr customer = builder.scan("customer", { "c_mktsegment" });

		customer = builder.select(customer,
			builder.function("=",
				builder.column("c_mktsegment"),
				builder.constant("BUILDING")
		));

		auto got_rows = TestUtils::run_query(cat, customer);
		REQUIRE(got_rows.size() == 337);

		for (auto row : got_rows) {
			REQUIRE(!row.compare("BUILDING"));
		}
	}

	SECTION("customer not equal") {
		RelOpPtr customer = builder.scan("customer", { "c_mktsegment" });

		customer = builder.select(customer,
			builder.function("!=",
				builder.column("c_mktsegment"),
				builder.constant("BUILDING")
		));

		auto got_rows = TestUtils::run_query(cat, customer);

		size_t num_rows = 1500 - 337;
		REQUIRE(got_rows.size() == num_rows);

		for (auto row : got_rows) {
			REQUIRE(row.compare("BUILDING"));
		}
	}

	SECTION("part contains") {
		RelOpPtr customer = builder.scan("part", { "p_name" });

		customer = builder.select(customer,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		auto got_rows = TestUtils::run_query(cat, customer);
		REQUIRE(got_rows.size() == 107);
		for (auto& row : got_rows) {
			REQUIRE(string_contains(row, "green"));
		}
	}

	SECTION("part not contains") {
		RelOpPtr customer = builder.scan("part", { "p_name" });

		customer = builder.select(customer,
			builder.function("!",
				builder.function("contains",
					builder.column("p_name"),
					builder.constant("green")
				)
			)
		);

		auto got_rows = TestUtils::run_query(cat, customer);
		size_t num_rows = 2000 - 107;
		REQUIRE(got_rows.size() == num_rows);
		for (auto& row : got_rows) {
			REQUIRE(!string_contains(row, "green"));
		}
	}
}
