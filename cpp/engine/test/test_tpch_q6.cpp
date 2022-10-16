#include "catch.hpp"

#include "system/system.hpp"
#include "engine/catalog.hpp"
#include "engine/storage/csv_loader.hpp"
#include "tpch_test_utils.hpp"
#include "test_utils.hpp"

#include <map>
#if 1
static std::map<std::string, engine::catalog::Catalog> catalogs;

using namespace plan;

typedef std::vector<ExprPtr> ExprPtrVec;

static const int kShipdateLt = 1 << 1;
static const int kShipdateGe = 1 << 2;
static const int kDiscountLt = 1 << 3;
static const int kDiscountGe = 1 << 4;
static const int kQunatityLt = 1 << 5;


static RelOpPtr
query6_builder(int variant)
{
	Builder builder;

	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1994-01-01");
	auto c2 = sql_date->internal_parse_into_string("1995-01-01");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto c3 = sql_decimal->internal_parse_into_string("0.05");
	auto c4 = sql_decimal->internal_parse_into_string("0.07");
	auto c5 = sql_decimal->internal_parse_into_string("24.00");

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_shipdate", "l_quantity", "l_extendedprice", "l_discount"
	});

	auto ge_shipdate = builder.function(">=",
		builder.column("l_shipdate"), builder.constant(c1));

	auto lt_shipdate = builder.function("<",
		builder.column("l_shipdate"), builder.constant(c2));

	auto ge_discount = builder.function(">=",
		builder.column("l_discount"), builder.constant(c3));

	auto le_discount = builder.function("<=",
		builder.column("l_discount"), builder.constant(c4));

	auto lt_quantity = builder.function("<",
		builder.column("l_quantity"), builder.constant(c5));

	if (variant & kShipdateLt) {
		lineitem = builder.select(lineitem, lt_shipdate);
	}
	if (variant & kShipdateGe) {
		lineitem = builder.select(lineitem, ge_shipdate);
	}
	if (variant & kQunatityLt) {
		lineitem = builder.select(lineitem, lt_quantity);
	}
	if (variant & kDiscountGe) {
		lineitem = builder.select(lineitem, ge_discount);
	}
	if (variant & kDiscountLt) {
		lineitem = builder.select(lineitem, le_discount);
	}

	RelOpPtr project = builder.project(lineitem, ExprPtrVec {
		builder.assign("revenue",
			builder.function("*",
				builder.column("l_extendedprice"), builder.column("l_discount")
			)
		)
	});


	RelOpPtr aggr = builder.hash_group_by(project,
		ExprPtrVec {},
		ExprPtrVec {
			builder.assign("sum", builder.function("sum", builder.column("revenue"))
		),
		});

	return aggr;
}

TEST_CASE("TPC-H SF1 Q6", "[engine][tpch][q6]")
{
	const std::string scale_factor = "1";
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


	SECTION("Q6a - whole table") {
		auto aggr = query6_builder(0);
		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		std::vector<std::string> expected_rows = {"114750870161999"};

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6b - filter only shipdate") {
		auto aggr = query6_builder(kShipdateGe | kShipdateLt);

		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		std::vector<std::string> expected_rows = {"17362391118409"};

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6b - filter only discount") {
		auto aggr = query6_builder(kDiscountLt | kDiscountGe);

		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		std::vector<std::string> expected_rows = {"37606607335613"};

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6b - filter only quantity") {
		auto aggr = query6_builder(kQunatityLt);

		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		std::vector<std::string> expected_rows = {"24833351617393"};

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6b - filter shipdate & discount") {
		auto aggr = query6_builder(kShipdateLt | kShipdateGe | kDiscountGe | kDiscountLt);

		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		std::vector<std::string> expected_rows = {"5682643148435"};

		TestUtils::compare_results(got_rows, expected_rows);
	}
}
#endif