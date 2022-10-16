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

static RelOpPtr
q9f_builder()
{
	Builder builder;

	RelOpPtr nation = builder.scan("nation", {
		"n_nationkey", "n_name"
	});

	RelOpPtr supplier = builder.scan("supplier", {
		"s_nationkey", "s_suppkey"
	});

	RelOpPtr nationsupplier = builder.hash_join(
		nation,
		ExprPtrVec { builder.column("n_nationkey") },
		ExprPtrVec { builder.column("n_name") },

		supplier,
		ExprPtrVec { builder.column("s_nationkey") },
		ExprPtrVec { builder.column("s_suppkey") },

		true
	);

	RelOpPtr part = builder.scan("part", {
		"p_partkey", "p_name"
	});

	RelOpPtr partsupp = builder.scan("partsupp", {
		"ps_partkey", "ps_suppkey", "ps_supplycost"
	});

	RelOpPtr partpartsupp = builder.hash_join(
		part,
		ExprPtrVec { builder.column("p_partkey") },
		ExprPtrVec { },

		partsupp,
		ExprPtrVec { builder.column("ps_partkey") },
		ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

		true
	);

	RelOpPtr nspps = builder.hash_join(
		nationsupplier,
		ExprPtrVec { builder.column("s_suppkey") },
		ExprPtrVec { builder.column("n_name") },

		partpartsupp,
		ExprPtrVec { builder.column("ps_suppkey") },
		ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

		true
	);

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
	});

	RelOpPtr lineitem_nspps = builder.hash_join(
		nspps,
		ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
		ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

		lineitem,
		ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
		ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
		builder.column("l_extendedprice"), builder.column("l_discount") },

		true
	);

	RelOpPtr orders = builder.scan("orders", {
		"o_orderkey", "o_orderdate"
	});

	RelOpPtr lineitem_orders = builder.hash_join(
		lineitem_nspps,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
		builder.column("l_discount"), builder.column("ps_supplycost"),
		builder.column("n_name") },

		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderdate") },

		false
	);

	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	RelOpPtr project;

	auto a = builder.function("-", builder.constant(one), builder.column("l_discount"));
	auto b = builder.column("l_extendedprice");

	auto c = builder.column("ps_supplycost");
	auto d = builder.column("l_quantity");

	project = builder.project(lineitem_orders, ExprPtrVec {
		builder.assign("amount1", b),
		builder.assign("amount2", c),
		builder.assign("amount3", builder.function("*", a, b)),
		builder.assign("amount4", builder.function("*", c, d)),
		builder.assign("amount5", builder.function("-", b, c)),
		builder.assign("amount6", builder.function("-", c, b)),
		builder.assign("amountAll",
			builder.function("-",
				builder.function("*", a, b),
				builder.function("*", c, d))
		),
	});

	return builder.hash_group_by(project,
		std::vector<ExprPtr> {},
		std::vector<ExprPtr> {
			builder.assign("sum1", builder.function("sum", builder.column("amount1"))),
			builder.assign("sum2", builder.function("sum", builder.column("amount2"))),
			builder.assign("sum3", builder.function("sum", builder.column("amount3"))),
			builder.assign("sum4", builder.function("sum", builder.column("amount4"))),
			builder.assign("sum5", builder.function("sum", builder.column("amount5"))),
			builder.assign("sum6", builder.function("sum", builder.column("amount6"))),
			builder.assign("sumAll", builder.function("sum", builder.column("amountAll"))),
			builder.assign("count", builder.function("count", builder.column("amount1"))),
		}
	);
}

static std::vector<RelOpPtr>
q9_pp_builder(int ps_filter, int p_filter, bool payload = false)
{
	Builder builder;

	RelOpPtr part = builder.scan("part", {
		"p_partkey", "p_name"
	});

	RelOpPtr partsupp = builder.scan("partsupp", {
		"ps_partkey", "ps_suppkey", "ps_supplycost"
	});

	if (ps_filter >= 0) {
		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant(std::to_string(ps_filter))
		));
	}

	if (p_filter >= 0) {
		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant(std::to_string(p_filter))
		));
	}

	ExprPtrVec payload_cols;
	if (payload) {
		payload_cols = { builder.column("ps_suppkey"),
				builder.column("ps_supplycost") };
	}

	RelOpPtr partpartsupp = builder.hash_join(
		partsupp,
		ExprPtrVec { builder.column("ps_partkey") },
		payload_cols,

		part,
		ExprPtrVec { builder.column("p_partkey") },
		ExprPtrVec { },

		false
	);

	partpartsupp = builder.project(partpartsupp, ExprPtrVec {
		builder.column("p_partkey"),
		builder.column("ps_partkey")
	});

	RelOpPtr partpartsupp_rev = builder.hash_join(
		part,
		ExprPtrVec { builder.column("p_partkey") },
		ExprPtrVec { },

		partsupp,
		ExprPtrVec { builder.column("ps_partkey") },
		payload_cols,

		false
	);

	partpartsupp_rev = builder.project(partpartsupp_rev, ExprPtrVec {
		builder.column("p_partkey"),
		builder.column("ps_partkey")
	});


	return { partpartsupp, partpartsupp_rev };

}

TEST_CASE("TPC-H SF1 Q9 sub tests", "[engine][tpch][q9]")
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

	Builder builder;

	RelOpPtr nation = builder.scan("nation", {
		"n_name", "n_regionkey", "n_nationkey"
	});

	RelOpPtr region = builder.scan("region", {
		"r_name", "r_regionkey"
	});

	const std::vector<std::string> resultNationJoinRegion({
"ALGERIA|AFRICA",
"ARGENTINA|AMERICA",
"BRAZIL|AMERICA",
"CANADA|AMERICA",
"EGYPT|MIDDLE EAST",
"ETHIOPIA|AFRICA",
"FRANCE|EUROPE",
"GERMANY|EUROPE",
"INDIA|ASIA",
"INDONESIA|ASIA"
	});

	SECTION("nation join region") {
		auto nselect = builder.select(nation,
			builder.function("<",
				builder.column("n_nationkey"),
				builder.constant("10")));

		RelOpPtr join = builder.hash_join(
			nselect,
			ExprPtrVec { builder.column("n_regionkey") },
			ExprPtrVec { builder.column("n_name") },

			region,
			ExprPtrVec { builder.column("r_regionkey") },
			ExprPtrVec { builder.column("r_name") },

			false
		);

		join = builder.project(join, ExprPtrVec {
			builder.column("n_name"),
			builder.column("r_name")
		});

		auto got_rows = TestUtils::run_query(cat, join);
		TestUtils::compare_results(got_rows, resultNationJoinRegion);
	}

	SECTION("region join nation") {
		auto nselect = builder.select(nation,
			builder.function("<",
				builder.column("n_nationkey"),
				builder.constant("10")));

		RelOpPtr join = builder.hash_join(
			region,
			ExprPtrVec { builder.column("r_regionkey") },
			ExprPtrVec { builder.column("r_name") },

			nselect,
			ExprPtrVec { builder.column("n_regionkey") },
			ExprPtrVec { builder.column("n_name") },

			false
		);

		join = builder.project(join, ExprPtrVec {
			builder.column("n_name"),
			builder.column("r_name")
		});

		auto got_rows = TestUtils::run_query(cat, join);
		TestUtils::compare_results(got_rows, resultNationJoinRegion);
	}
}


TEST_CASE("TPC-H SF1 Q9 tests", "[engine][tpch][q9]")
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
#if 1
	SECTION("Q9_nation") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_name", "n_regionkey"
		});

		nation = builder.select(nation,
			builder.function("=",
				builder.column("n_regionkey"),
				builder.constant("3")));

		auto aggr = builder.hash_group_by(nation,
			std::vector<ExprPtr> {builder.column("n_name")},
			std::vector<ExprPtr> {
				builder.assign("count", builder.function("count", builder.column("n_name"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);

		TestUtils::compare_results(got_rows, {
			"RUSSIA|1",
			"UNITED KINGDOM|1",
			"GERMANY|1",
			"FRANCE|1",
			"ROMANIA|1"
		});
	}

	SECTION("Q9a - partpartsupp") {
		Builder builder;

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("10000")));

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		auto got_rows = TestUtils::run_query(cat, partpartsupp);
		auto expected_rows = TestUtils::get_results_from_file(
			TestUtils::get_prefix_directory() + "/tpch_sf1_q9a.txt");

		TestUtils::compare_results(got_rows, expected_rows);
	}


	SECTION("join partpartsupp, no filter") {
		auto answers = TestUtils::run_queries(cat, q9_pp_builder(-1, -1));

		REQUIRE(answers.size() == 2);
		for (auto& got_rows : answers) {
			REQUIRE(got_rows.size() == 800000);
		}
	}

	SECTION("join partpartsupp, filter partsupp") {
		auto answers = TestUtils::run_queries(cat, q9_pp_builder(10000, -1));

		REQUIRE(answers.size() == 2);
		for (auto& got_rows : answers) {
			REQUIRE(got_rows.size() == 40000);
		}
	}

	SECTION("join partpartsupp, filter part") {
		auto answers = TestUtils::run_queries(cat, q9_pp_builder(-1, 1000));

		REQUIRE(answers.size() == 2);
		for (auto& got_rows : answers) {
			REQUIRE(got_rows.size() == 4000);
		}
	}

	SECTION("join partpartsupp, filter both") {
		auto answers = TestUtils::run_queries(cat, q9_pp_builder(10000, 1000));

		REQUIRE(answers.size() == 2);
		for (auto& got_rows : answers) {
			REQUIRE(got_rows.size() == 4000);
		}
	}

	SECTION("green parts") {
		Builder builder;

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		auto aggr = builder.hash_group_by(part,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum", builder.function("sum", builder.column("p_partkey"))),
				builder.assign("count", builder.function("count", builder.column("p_name"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);

		TestUtils::compare_results(got_rows, {"1062298237|10664"});
	}

	SECTION("Q9a'' rev - partsupppart, no green part filter, no partsupp filter") {
		Builder builder;

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp  = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("1000000")
		));

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"),
				builder.column("ps_supplycost") },

			false
		);

		partpartsupp = builder.project(partpartsupp, ExprPtrVec {
			builder.column("p_partkey")
		});

		auto got_rows = TestUtils::run_query(cat, partpartsupp);
		REQUIRE(got_rows.size() == 800000);
	}

	SECTION("Q9a'' - partsupppart, no green part filter, no partsupp filter") {
		Builder builder;

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("1000000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		RelOpPtr partpartsupp = builder.hash_join(
			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"),
				builder.column("ps_supplycost") },

			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			false
		);

		partpartsupp = builder.project(partpartsupp, ExprPtrVec {
			builder.column("p_partkey")
		});

		auto got_rows = TestUtils::run_query(cat, partpartsupp);
		REQUIRE(got_rows.size() == 800000);
	}

	SECTION("Q9b - nspps") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_nationkey", "n_name"
		});

		RelOpPtr supplier = builder.scan("supplier", {
			"s_nationkey", "s_suppkey"
		});

		RelOpPtr nationsupplier = builder.hash_join(
			nation,
			ExprPtrVec { builder.column("n_nationkey") },
			ExprPtrVec { builder.column("n_name") },

			supplier,
			ExprPtrVec { builder.column("s_nationkey") },
			ExprPtrVec { builder.column("s_suppkey") },

			true
		);

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("n_nationkey"),
				builder.constant("10")
		));

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("s_suppkey"),
				builder.constant("100")
		));

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr nspps = builder.hash_join(
			nationsupplier,
			ExprPtrVec { builder.column("s_suppkey") },
			ExprPtrVec { builder.column("n_name") },

			partpartsupp,
			ExprPtrVec { builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

			true
		);


		auto got_rows = TestUtils::run_query(cat, nspps);
		auto expected_rows = TestUtils::get_results_from_file(
			TestUtils::get_prefix_directory() + "/tpch_sf1_q9b.txt");

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q9c - lineitem_nspps") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_nationkey", "n_name"
		});

		RelOpPtr supplier = builder.scan("supplier", {
			"s_nationkey", "s_suppkey"
		});

		RelOpPtr nationsupplier = builder.hash_join(
			nation,
			ExprPtrVec { builder.column("n_nationkey") },
			ExprPtrVec { builder.column("n_name") },

			supplier,
			ExprPtrVec { builder.column("s_nationkey") },
			ExprPtrVec { builder.column("s_suppkey") },

			true
		);

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("n_nationkey"),
				builder.constant("10")
		));

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("s_suppkey"),
				builder.constant("100")
		));

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr nspps = builder.hash_join(
			nationsupplier,
			ExprPtrVec { builder.column("s_suppkey") },
			ExprPtrVec { builder.column("n_name") },

			partpartsupp,
			ExprPtrVec { builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
		});

		RelOpPtr lineitem_nspps = builder.hash_join(
			nspps,
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

			lineitem,
			ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
			ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
			builder.column("l_extendedprice"), builder.column("l_discount") },

			true
		);

		auto got_rows = TestUtils::run_query(cat, lineitem_nspps);
		REQUIRE(got_rows.size() == 53);
	}
#endif
	SECTION("Q9d - lineitem_orders") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_nationkey", "n_name"
		});

		RelOpPtr supplier = builder.scan("supplier", {
			"s_nationkey", "s_suppkey"
		});

		RelOpPtr nationsupplier = builder.hash_join(
			nation,
			ExprPtrVec { builder.column("n_nationkey") },
			ExprPtrVec { builder.column("n_name") },

			supplier,
			ExprPtrVec { builder.column("s_nationkey") },
			ExprPtrVec { builder.column("s_suppkey") },

			true
		);

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("n_nationkey"),
				builder.constant("10")
		));

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("s_suppkey"),
				builder.constant("100")
		));

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr nspps = builder.hash_join(
			nationsupplier,
			ExprPtrVec { builder.column("s_suppkey") },
			ExprPtrVec { builder.column("n_name") },

			partpartsupp,
			ExprPtrVec { builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
		});

		RelOpPtr lineitem_nspps = builder.hash_join(
			nspps,
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

			lineitem,
			ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
			ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
			builder.column("l_extendedprice"), builder.column("l_discount") },

			true
		);

		RelOpPtr orders = builder.scan("orders", {
			"o_orderkey", "o_orderdate"
		});

		RelOpPtr lineitem_orders = builder.hash_join(
			lineitem_nspps,
			ExprPtrVec { builder.column("l_orderkey") },
			ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
			builder.column("l_discount"), builder.column("ps_supplycost"),
			builder.column("n_name") },

			orders,
			ExprPtrVec { builder.column("o_orderkey") },
			ExprPtrVec { builder.column("o_orderdate") },

			false
		);


		auto got_rows = TestUtils::run_query(cat, lineitem_orders);
		REQUIRE(got_rows.size() == 53);
	}

	SECTION("Q9e - global aggregate") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_nationkey", "n_name"
		});

		RelOpPtr supplier = builder.scan("supplier", {
			"s_nationkey", "s_suppkey"
		});

		RelOpPtr nationsupplier = builder.hash_join(
			nation,
			ExprPtrVec { builder.column("n_nationkey") },
			ExprPtrVec { builder.column("n_name") },

			supplier,
			ExprPtrVec { builder.column("s_nationkey") },
			ExprPtrVec { builder.column("s_suppkey") },

			true
		);


		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("n_nationkey"),
				builder.constant("10")
		));

		nationsupplier = builder.select(nationsupplier,
			builder.function("<=",
				builder.column("s_suppkey"),
				builder.constant("100")
		));

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		part = builder.select(part,
			builder.function("<=",
				builder.column("p_partkey"),
				builder.constant("10000")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		partsupp = builder.select(partsupp,
			builder.function("<=",
				builder.column("ps_partkey"),
				builder.constant("10000")
		));


		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr nspps = builder.hash_join(
			nationsupplier,
			ExprPtrVec { builder.column("s_suppkey") },
			ExprPtrVec { builder.column("n_name") },

			partpartsupp,
			ExprPtrVec { builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
		});

		RelOpPtr lineitem_nspps = builder.hash_join(
			nspps,
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

			lineitem,
			ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
			ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
			builder.column("l_extendedprice"), builder.column("l_discount") },

			true
		);

		RelOpPtr orders = builder.scan("orders", {
			"o_orderkey", "o_orderdate"
		});

		RelOpPtr lineitem_orders = builder.hash_join(
			lineitem_nspps,
			ExprPtrVec { builder.column("l_orderkey") },
			ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
			builder.column("l_discount"), builder.column("ps_supplycost"),
			builder.column("n_name") },

			orders,
			ExprPtrVec { builder.column("o_orderkey") },
			ExprPtrVec { builder.column("o_orderdate") },

			false
		);

		auto sql_decimal = SqlType::from_string("decimal(12,2)");
		auto one = sql_decimal->internal_parse_into_string("1.00");

		auto a = builder.function("-", builder.constant(one), builder.column("l_discount"));
		auto b = builder.column("l_extendedprice");

		auto c = builder.column("ps_supplycost");
		auto d = builder.column("l_quantity");

		RelOpPtr project = builder.project(lineitem_orders, ExprPtrVec {
			builder.assign("amount",
				builder.function("-",
					builder.function("*", a, b),
					builder.function("*", c, d))
			),
		});

		auto aggr = builder.hash_group_by(project,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum", builder.function("sum", builder.column("amount"))),
			}
		);


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"8332387260"});
	}


	SECTION("Q9f - global aggregate, no filter") {
		auto aggr = q9f_builder();

		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {
			"22957731090120|300300266697|2181022238850001|765873903109300|22657430823423|"
			"-22657430823423|1415148335740701|6001215"
		});
	}

	SECTION("Q9g - lineitem orders, global aggregate, no filter") {
		Builder builder;

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
		});

		RelOpPtr orders = builder.scan("orders", {
			"o_orderkey", "o_orderdate"
		});

		RelOpPtr lineitem_orders = builder.hash_join(
			lineitem,
			ExprPtrVec { builder.column("l_orderkey") },
			ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
			builder.column("l_discount") },

			orders,
			ExprPtrVec { builder.column("o_orderkey") },
			ExprPtrVec { builder.column("o_orderdate") },

			false
		);

		auto sql_decimal = SqlType::from_string("decimal(12,2)");
		auto one = sql_decimal->internal_parse_into_string("1.00");

		auto a = builder.function("-", builder.constant(one), builder.column("l_discount"));
		auto b = builder.column("l_extendedprice");

		RelOpPtr project = builder.project(lineitem_orders, ExprPtrVec {
			builder.assign("amount",
				builder.function("*", a, b)
			),
		});

		auto aggr = builder.hash_group_by(project,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum", builder.function("sum", builder.column("amount"))),
				builder.assign("count", builder.function("count", builder.column("amount"))),
			}
		);


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"2181022238850001|6001215"});
	}

	SECTION("Q9h - global aggregate, filters") {
		Builder builder;

		RelOpPtr nation = builder.scan("nation", {
			"n_nationkey", "n_name"
		});

		RelOpPtr supplier = builder.scan("supplier", {
			"s_nationkey", "s_suppkey"
		});

		RelOpPtr nationsupplier = builder.hash_join(
			nation,
			ExprPtrVec { builder.column("n_nationkey") },
			ExprPtrVec { builder.column("n_name") },

			supplier,
			ExprPtrVec { builder.column("s_nationkey") },
			ExprPtrVec { builder.column("s_suppkey") },

			true
		);

		RelOpPtr part = builder.scan("part", {
			"p_partkey", "p_name"
		});

		part = builder.select(part,
			builder.function("contains",
				builder.column("p_name"),
				builder.constant("green")
		));

		RelOpPtr partsupp = builder.scan("partsupp", {
			"ps_partkey", "ps_suppkey", "ps_supplycost"
		});

		RelOpPtr partpartsupp = builder.hash_join(
			part,
			ExprPtrVec { builder.column("p_partkey") },
			ExprPtrVec { },

			partsupp,
			ExprPtrVec { builder.column("ps_partkey") },
			ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr nspps = builder.hash_join(
			nationsupplier,
			ExprPtrVec { builder.column("s_suppkey") },
			ExprPtrVec { builder.column("n_name") },

			partpartsupp,
			ExprPtrVec { builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

			true
		);

		RelOpPtr lineitem = builder.scan("lineitem", {
			"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
		});

		RelOpPtr lineitem_nspps = builder.hash_join(
			nspps,
			ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
			ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

			lineitem,
			ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
			ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
			builder.column("l_extendedprice"), builder.column("l_discount") },

			true
		);

		RelOpPtr orders = builder.scan("orders", {
			"o_orderkey", "o_orderdate"
		});

		RelOpPtr lineitem_orders = builder.hash_join(
			lineitem_nspps,
			ExprPtrVec { builder.column("l_orderkey") },
			ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
			builder.column("l_discount"), builder.column("ps_supplycost"),
			builder.column("n_name") },

			orders,
			ExprPtrVec { builder.column("o_orderkey") },
			ExprPtrVec { builder.column("o_orderdate") },

			false
		);

		auto sql_decimal = SqlType::from_string("decimal(12,2)");
		auto one = sql_decimal->internal_parse_into_string("1.00");

		auto a = builder.function("-", builder.constant(one), builder.column("l_discount"));
		auto b = builder.column("l_extendedprice");

		auto c = builder.column("ps_supplycost");
		auto d = builder.column("l_quantity");

		RelOpPtr project = builder.project(lineitem_orders, ExprPtrVec {
			builder.assign("amount",
				builder.function("-",
					builder.function("*", a, b),
					builder.function("*", c, d))
			),
		});

		auto aggr = builder.hash_group_by(project,
			std::vector<ExprPtr> {},
			std::vector<ExprPtr> {
				builder.assign("sum", builder.function("sum", builder.column("amount"))),
				builder.assign("count", builder.function("count", builder.column("amount"))),
			}
		);


		auto got_rows = TestUtils::run_query(cat, aggr);
		REQUIRE(got_rows.size() == 1);

		TestUtils::compare_results(got_rows, {"75404610361232|319404"});
	}
}
#endif