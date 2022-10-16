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

TEST_CASE("TPC-H SF1 Q4 tests", "[engine][tpch][q4]")
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

	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1993-10-01");
	auto c2 = sql_date->internal_parse_into_string("1993-07-01");

	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_orderkey", "l_commitdate", "l_receiptdate"
	});

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_commitdate"), builder.column("l_receiptdate")));


	RelOpPtr orders = builder.scan("orders", {
		"o_orderkey", "o_orderdate", "o_orderpriority"
	});

	orders = builder.select(orders, builder.function("<",
		builder.column("o_orderdate"), builder.constant(c1)));

	orders = builder.select(orders, builder.function(">=",
		builder.column("o_orderdate"), builder.constant(c2)));


	SECTION("filter orders") {
		RelOpPtr aggr = builder.hash_group_by(orders,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count", builder.function("count", builder.column("o_orderkey"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"57218"});
	}


	RelOpPtr join = builder.hash_join(
		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec {  },

		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderpriority") },


		true
	);

	SECTION("join") {
		auto got_rows = TestUtils::run_query(cat, join);
		REQUIRE(got_rows.size() == 52523);
	}

	SECTION("SUM(join)") {
		RelOpPtr aggr = builder.hash_group_by(join,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count", builder.function("count", builder.column("o_orderkey"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"52523"});
	}
}
#endif