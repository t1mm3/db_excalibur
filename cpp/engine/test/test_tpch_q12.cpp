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

TEST_CASE("TPC-H SF1 Q12 tests", "[engine][tpch][q12]")
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
	auto c1 = sql_date->internal_parse_into_string("1995-01-01");
	auto c2 = sql_date->internal_parse_into_string("1994-01-01");

	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"
	});

	SECTION("filter lineitem (MAIL)") {
		auto mail = builder.select(lineitem, builder.function("=",
			builder.column("l_shipmode"), builder.constant("MAIL")));
		auto aggr = builder.hash_group_by(mail,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_shipmode"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"857401"});
	}

	SECTION("filter lineitem (SHIP)") {
		auto ship = builder.select(lineitem, builder.function("=",
			builder.column("l_shipmode"), builder.constant("SHIP")));
		auto aggr = builder.hash_group_by(ship,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_shipmode"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"858036"});
	}

	lineitem = builder.select(lineitem, builder.function("|",
		builder.function("=", builder.column("l_shipmode"), builder.constant("MAIL")),
		builder.function("=", builder.column("l_shipmode"), builder.constant("SHIP"))
		));

	SECTION("filter lineitem (strings)") {
		RelOpPtr aggr = builder.hash_group_by(lineitem,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_shipmode"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"1715437"});
	}

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_receiptdate"), builder.constant(c1)));

	lineitem = builder.select(lineitem, builder.function(">=",
		builder.column("l_receiptdate"), builder.constant(c2)));

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_commitdate"), builder.column("l_receiptdate")));

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_shipdate"), builder.column("l_commitdate")));

	SECTION("filter lineitem") {
		RelOpPtr aggr = builder.hash_group_by(lineitem,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_shipdate"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"30988"});
	}


	SECTION("ifthenelse") {
		auto nation = builder.scan("nation", { "n_name" });

		auto project = builder.project(nation, ExprPtrVec {
		builder.assign("x",
			builder.function("ifthenelse",
				builder.function("=",
					builder.constant("GERMANY"), builder.column("n_name")),
				builder.column("n_name"),
				builder.constant("GERMANY")
			)
			),
		});

		auto got_rows = TestUtils::run_query(cat, project);
		TestUtils::compare_results(got_rows, {
			"GERMANY", "GERMANY", "GERMANY", "GERMANY", "GERMANY",
			"GERMANY", "GERMANY", "GERMANY", "GERMANY", "GERMANY",
			"GERMANY", "GERMANY", "GERMANY", "GERMANY", "GERMANY",
			"GERMANY", "GERMANY", "GERMANY", "GERMANY", "GERMANY",
			"GERMANY", "GERMANY", "GERMANY", "GERMANY", "GERMANY",});
	}


	auto orders = builder.scan("orders", {
		"o_orderkey", "o_orderpriority"
	});

	RelOpPtr join = builder.hash_join(
		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderpriority") },

		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_shipmode") },

		true
	);

	auto project = builder.project(join, ExprPtrVec {
		builder.assign("_TRSDM_2",
			builder.function("ifthenelse",
				builder.function("&",
						builder.function("!=", builder.column("o_orderpriority"),
							builder.constant("2-HIGH")),
						builder.function("!=", builder.column("o_orderpriority"),
							builder.constant("1-URGENT"))),
				builder.constant("1"),
				builder.constant("0")
			)),
		builder.assign("_TRSDM_3",
			builder.function("ifthenelse",
				builder.function("|",
						builder.function("=", builder.column("o_orderpriority"),
							builder.constant("2-HIGH")),
						builder.function("=", builder.column("o_orderpriority"),
							builder.constant("1-URGENT"))),
				builder.constant("1"),
				builder.constant("0")
			)),
		builder.column("l_shipmode"),
	});



	SECTION("project") {
		LOG_WARN("start");
		RelOpPtr aggr = builder.hash_group_by(project,
			ExprPtrVec {},
			ExprPtrVec {
				builder.assign("count",
					builder.function("count", builder.column("l_shipmode"))),
				builder.assign("hi",
					builder.function("sum", builder.column("_TRSDM_3"))),
				builder.assign("lo",
					builder.function("sum", builder.column("_TRSDM_2"))),
			}
		);

		auto got_rows = TestUtils::run_query(cat, aggr);
		TestUtils::compare_results(got_rows, {"30988|12402|18586"});
	}


	auto aggr = builder.hash_group_by(project,
		ExprPtrVec { builder.column("l_shipmode") },
		ExprPtrVec {
			builder.assign("high_line_count",
				builder.function("sum", builder.column("_TRSDM_3"))),
			builder.assign("low_line_count",
				builder.function("sum", builder.column("_TRSDM_2"))),
		}
	);
}
#endif