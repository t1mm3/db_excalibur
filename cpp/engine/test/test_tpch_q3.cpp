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

TEST_CASE("TPC-H SF1 Q3 tests", "[engine][tpch][q3]")
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
	auto c1 = sql_date->internal_parse_into_string("1995-03-15");
	auto c2 = sql_date->internal_parse_into_string("1995-03-15");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	SECTION("SUM revenue") {
		Builder builder;

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"
		});

		RelOpPtr project = builder.project(lineitem, ExprPtrVec {
			builder.assign("revenue",
				builder.function("*",
					builder.column("l_extendedprice"),
					(builder.function("-", builder.constant(one), builder.column("l_discount")))
				)
			)
		});

		RelOpPtr aggr = builder.hash_group_by(project,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("sum", builder.function("sum", builder.column("revenue"))),
			});


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"2181022238850001"});
	}

	SECTION("SUM revenue, filter l_shipdate") {
		Builder builder;

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"
		});

		lineitem = builder.select(lineitem,
			builder.function(">",
				builder.column("l_shipdate"),
				builder.constant(c2)
		));

		RelOpPtr project = builder.project(lineitem, ExprPtrVec {
			builder.assign("revenue",
				builder.function("*",
					builder.column("l_extendedprice"),
					(builder.function("-", builder.constant(one), builder.column("l_discount")))
				)
			)
		});

		RelOpPtr aggr = builder.hash_group_by(project,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("sum", builder.function("sum", builder.column("revenue"))),
				builder.assign("count", builder.function("count", builder.column("revenue"))),
			});


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"1177959046148389|3241776"});
	}

	SECTION("Q3a") {
		Builder builder;

		RelOpPtr customer = builder.scan("customer", {
			"c_mktsegment", "c_custkey"
		});

		customer = builder.select(customer,
			builder.function("=",
				builder.column("c_mktsegment"),
				builder.constant("BUILDING")
		));

		RelOpPtr orders = builder.scan("orders", {
			"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"
		});

		orders = builder.select(orders,
			builder.function("<",
				builder.column("o_orderdate"),
				builder.constant(c2)
		));

		RelOpPtr customerorders = builder.hash_join(
			customer,
			ExprPtrVec { builder.column("c_custkey") },
			ExprPtrVec { },

			orders,
			ExprPtrVec { builder.column("o_custkey") },
			ExprPtrVec { builder.column("o_orderdate"),
				builder.column("o_shippriority"), builder.column("o_orderkey") },

			true
		);

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"
		});

		lineitem = builder.select(lineitem,
			builder.function(">",
				builder.column("l_shipdate"),
				builder.constant(c2)
		));

		RelOpPtr lineitemcustomerorders = builder.hash_join(
			customerorders,
			ExprPtrVec { builder.column("o_orderkey") },
			ExprPtrVec { builder.column("o_orderdate"), builder.column("o_shippriority") },

			lineitem,
			ExprPtrVec { builder.column("l_orderkey") },
			ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_discount") },

			true
		);

		RelOpPtr project = builder.project(lineitemcustomerorders, ExprPtrVec {
			builder.assign("revenue",
				builder.function("*",
					builder.column("l_extendedprice"),
					(builder.function("-", builder.constant(one), builder.column("l_discount")))
				)
			)
		});

		RelOpPtr aggr = builder.hash_group_by(project,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("sum", builder.function("sum", builder.column("revenue"))),
				builder.assign("count", builder.function("count", builder.column("revenue"))),
			});


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"11152712435141|30519"});
	}
}
#endif