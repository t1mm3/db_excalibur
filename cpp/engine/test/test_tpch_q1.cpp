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

const int kGroupBy = 1 << 1;
const int kSelection = 1 << 2;

static RelOpPtr
q1_builder(int variant)
{
	Builder builder;

	RelOpPtr scan = builder.scan("lineitem", {
		"l_shipdate", "l_returnflag", "l_linestatus",
		"l_extendedprice", "l_quantity", "l_discount",
		"l_tax"
	});

	auto sql_date = SqlType::from_string("date");
	auto shipdate = sql_date->internal_parse_into_string("1998-09-02");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	if (variant & kSelection) {
		scan = builder.select(scan,
			builder.function("<=", builder.column("l_shipdate"),
				builder.constant(shipdate)));		
	}

	auto project = builder.project(scan, ExprPtrVec {
		builder.assign("_TRSDM_6",
			builder.function("-", builder.constant(one), builder.column("l_discount"))),
		builder.assign("_TRSDM_7",
			builder.function("*", builder.column("_TRSDM_6"), builder.column("l_extendedprice"))),
		builder.assign("_TRSDM_8",
			builder.function("*",
				builder.function("*",
					builder.function("+",
						builder.constant(one),
						builder.column("l_tax")
					),
					builder.column("_TRSDM_6")
				),
				builder.column("l_extendedprice")
		)),
		builder.column("l_quantity"),
		builder.column("l_discount"),
		builder.column("l_extendedprice"),
		builder.column("l_returnflag"),
		builder.column("l_linestatus")
	});

	std::vector<ExprPtr> keys {builder.column("l_returnflag"), builder.column("l_linestatus")};
	if (!(variant & kGroupBy)) {
		keys.clear();
	}

	return builder.hash_group_by(project,
		keys,
		std::vector<ExprPtr> {
			builder.assign("count", builder.function("count", builder.column("l_quantity"))),
			builder.assign("sum_1", builder.function("sum", builder.column("l_quantity"))),
			builder.assign("sum_2", builder.function("sum", builder.column("l_extendedprice"))),
			builder.assign("sum_3", builder.function("sum", builder.column("_TRSDM_7"))),
			builder.assign("sum_4", builder.function("sum", builder.column("_TRSDM_8"))),
			builder.assign("sum_5", builder.function("sum", builder.column("l_discount")))
		}
	);
}

TEST_CASE("TPC-H SF1 Q1 tests", "[engine][tpch][q1]")
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

	SECTION("SUM l_extendedprice & l_discount") {

		Builder builder;

		RelOpPtr scan = builder.scan("lineitem", { "l_extendedprice", "l_discount" });

		auto sum = builder.hash_group_by(scan,
			{},
			std::vector<ExprPtr> {
				builder.assign("sum1", builder.function("sum", builder.column("l_extendedprice"))),
				builder.assign("sum2", builder.function("sum", builder.column("l_discount")))
			});
		auto got_rows = TestUtils::run_query(cat, sum);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"22957731090120|30005733"});
	}
#if 0
	SECTION("Q1b - global aggregates, no select") {
		auto got_rows = TestUtils::run_query(cat, q1_builder(0));
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows,
			{"6001215|15307879500|22957731090120|2181022238850001|226829357828867781|30005733"});
	}

	SECTION("Q1a - global aggregates, select") {
		auto got_rows = TestUtils::run_query(cat, q1_builder(kSelection));
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows,
			{"5916591|15092131700|22634383018975|2150308622951337|223635377438351009|29581538"});
	}
#endif
}
#endif