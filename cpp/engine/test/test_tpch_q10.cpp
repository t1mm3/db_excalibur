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

TEST_CASE("TPC-H SF1 Q10 tests", "[engine][tpch][q10]")
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
	auto c1 = sql_date->internal_parse_into_string("1994-01-01");
	auto c2 = sql_date->internal_parse_into_string("1993-10-01");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_returnflag", "l_discount", "l_extendedprice"
	});

	lineitem = builder.select(lineitem, builder.function("=",
		builder.column("l_returnflag"), builder.constant("82")));


	SECTION("filter lineitem") {
		RelOpPtr aggr = builder.hash_group_by(lineitem,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count", builder.function("count", builder.column("l_returnflag"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"1478870"});
	}



	RelOpPtr orders = builder.scan("orders", {
		"o_custkey", "o_orderkey", "o_orderdate"
	});

	orders = builder.select(orders, builder.function("<",
		builder.column("o_orderdate"), builder.constant(c1)));
	orders = builder.select(orders, builder.function(">=",
		builder.column("o_orderdate"), builder.constant(c2)));


	SECTION("filter orders") {
		RelOpPtr aggr = builder.hash_group_by(orders,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count", builder.function("count", builder.column("o_orderdate"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"57069"});
	}



	RelOpPtr join = builder.hash_join(
		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_custkey") },

		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice") },

		true
	);


	SECTION("ĵoin") {
		RelOpPtr aggr = builder.hash_group_by(join,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count", builder.function("count", builder.column("l_orderkey"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"114705"});
	}




	RelOpPtr customer = builder.scan("customer", {
		"c_custkey", "c_nationkey", "c_name", "c_acctbal",
		"c_phone", "c_address", "c_comment"
	});

	RelOpPtr join2 = builder.hash_join(
		customer,
		ExprPtrVec { builder.column("c_custkey") },
		ExprPtrVec { builder.column("c_nationkey"), builder.column("c_name"),
			builder.column("c_acctbal"), builder.column("c_phone"),
			builder.column("c_address"), builder.column("c_comment") },

		join,
		ExprPtrVec { builder.column("o_custkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice") },

		true
	);


	SECTION("join lineitem orders customer") {
		RelOpPtr aggr = builder.hash_group_by(join2,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("o_custkey"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"114705"});
	}



	RelOpPtr nation = builder.scan("nation", { "n_nationkey", "n_name" });

	RelOpPtr join3 = builder.hash_join(
		nation,
		ExprPtrVec { builder.column("n_nationkey") },
		ExprPtrVec { builder.column("n_name") },

		join2,
		ExprPtrVec { builder.column("c_nationkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice"),
			builder.column("c_name"), builder.column("c_acctbal"),
			builder.column("c_phone"), builder.column("c_address"),
			builder.column("c_comment") },

		false // TODO: why? should be FK join, works on whole query,
		// but not in these tests, tracked by #56
	);


	SECTION("join lineitem orders customer nation") {
		RelOpPtr aggr = builder.hash_group_by(join3,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_discount"))),
				builder.assign("sum_disc",
					builder.function("sum", builder.column("l_discount"))),
				builder.assign("sum_price",
					builder.function("sum", builder.column("l_extendedprice"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"114705|574333|438600670423"});
	}



	RelOpPtr project = builder.project(join3, ExprPtrVec {
		builder.assign("_TRSDM_1",
			builder.function("*",
				builder.function("-",
					builder.constant(one), builder.column("l_discount")),
				builder.column("l_extendedprice")
			)
		),
		builder.column("c_comment"),
		builder.column("c_address"),
		builder.column("n_name"),
		builder.column("c_phone"),
		builder.column("c_acctbal"),
		builder.column("c_name"),
		builder.column("c_custkey")
	});

	SECTION("pre-revenue") {
		RelOpPtr aggr = builder.hash_group_by(project,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("_TRSDM_1"))),
				builder.assign("sum",
					builder.function("sum", builder.column("_TRSDM_1"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"114705|41664005485255"});
	}

	RelOpPtr groupby_dumb = builder.hash_group_by(project,
		ExprPtrVec {
			builder.column("c_custkey"), // only real key
			builder.column("c_comment"),
			builder.column("c_address"),
			builder.column("n_name"),
			builder.column("c_phone"),
			builder.column("c_acctbal"),
			builder.column("c_name"),
		},
		ExprPtrVec {
			builder.assign("_revenue_3",
				builder.function("sum", builder.column("_TRSDM_1"))),
		}
	);

	SECTION("groupby_dumb") {
		auto got_rows = TestUtils::run_query(cat, groupby_dumb);
		REQUIRE(got_rows.size() == 37967);
	}

	RelOpPtr groupby = builder.hash_group_by(project,
		ExprPtrVec {
			builder.column("c_custkey"),
		},
		ExprPtrVec {
			builder.column("c_comment"),
			builder.column("c_address"),
			builder.column("n_name"),
			builder.column("c_phone"),
			builder.column("c_acctbal"),
			builder.column("c_name"),
		},
		ExprPtrVec {
			builder.assign("_revenue_3",
				builder.function("sum", builder.column("_TRSDM_1"))),
		}
	);

	SECTION("groupby") {
		auto got_rows_dumb = TestUtils::run_query(cat, groupby_dumb);
		auto got_rows_smart = TestUtils::run_query(cat, groupby);
		REQUIRE(got_rows_dumb.size() == 37967);
		REQUIRE(got_rows_smart.size() == 37967);

		TestUtils::compare_results(got_rows_smart, got_rows_dumb);
	}

}
#endif