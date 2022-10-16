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

TEST_CASE("TPC-H SF1 Q18 tests", "[engine][tpch][q18]")
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

	auto sql_decimal = SqlType::from_string("decimal(15,2)");
	auto three_hundred = sql_decimal->internal_parse_into_string("300.00");

	SECTION("sum l_quantity") {
		Builder builder;

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_quantity" });

		lineitem1 = builder.hash_group_by(lineitem1,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
			});

		auto got_rows = TestUtils::run_query(cat, lineitem1);
		TestUtils::compare_results(got_rows, {"15307879500"});
	}

	SECTION("sum l_quantity >30") {
		Builder builder;

		auto thirty = sql_decimal->internal_parse_into_string("30.00");

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_quantity" });

		lineitem1 = builder.select(lineitem1, builder.function(">",
			builder.column("l_quantity"), builder.constant(thirty)));

		lineitem1 = builder.hash_group_by(lineitem1,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
			});


		auto got_rows = TestUtils::run_query(cat, lineitem1);
		TestUtils::compare_results(got_rows, {"9727349500"});
	}

	SECTION("sum l_quantity <30") {
		Builder builder;

		auto thirty = sql_decimal->internal_parse_into_string("30.00");

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_quantity" });

		lineitem1 = builder.select(lineitem1, builder.function("<",
			builder.column("l_quantity"), builder.constant(thirty)));

		lineitem1 = builder.hash_group_by(lineitem1,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
			});


		auto got_rows = TestUtils::run_query(cat, lineitem1);
		TestUtils::compare_results(got_rows, {"5221517000"});
	}

	SECTION("group lineitem") {
		Builder builder;

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_orderkey", "l_quantity" });

		lineitem1 = builder.hash_group_by(lineitem1,
			std::vector<ExprPtr> {builder.column("l_orderkey")},
			std::vector<ExprPtr> {
				builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
			});

		auto got_rows = TestUtils::run_query(cat, lineitem1);

		REQUIRE(got_rows.size() == 1500000);
	}

	SECTION("group select lineitem ") {
		Builder builder;

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_orderkey", "l_quantity" });

		lineitem1 = builder.hash_group_by(lineitem1,
			std::vector<ExprPtr> {builder.column("l_orderkey")},
			std::vector<ExprPtr> {
				builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
			});

		lineitem1 = builder.select(lineitem1, builder.function(">",
			builder.column("sum_quantity"), builder.constant(three_hundred)));

		lineitem1 = builder.project(lineitem1, std::vector<ExprPtr> {
				builder.assign("s", builder.column("sum_quantity")),
			});

		auto got_rows = TestUtils::run_query(cat, lineitem1);

		REQUIRE(got_rows.size() == 57);

		std::vector<std::string> results({
"307","305","323","308","311","304","328","303",
"313","301","303","317","310","302","305","312",
"302","302","317","320","320","305","305","305",
"303","309","302","318","327","302","307","301",
"304","304","309","307","305","304","309","304",
"308","308","301","309","303","301","302","301",
"302","301","302","312","301","304","304","302",
"320"});

		std::vector<std::string> result_cmp;
		for (auto& r : results) {
			result_cmp.emplace_back(std::to_string(100*std::stoi(r)));
		}

		TestUtils::compare_results(got_rows, result_cmp);
	}

	SECTION("select lineitem") {
		Builder builder;

		RelOpPtr lineitem1 = builder.scan("lineitem", { "l_quantity" });

		lineitem1 = builder.select(lineitem1, builder.function(">",
			builder.column("l_quantity"), builder.constant(three_hundred)));

		auto got_rows = TestUtils::run_query(cat, lineitem1);

		size_t x = 10;
		for (auto& r : got_rows) {
			if (!--x) {
				break;
			}
			LOG_WARN("r: %s", r.c_str());
		}

		REQUIRE(got_rows.size() == 0);
	}

}
#endif