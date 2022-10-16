#include "catch.hpp"


#include "system/system.hpp"
#include "system/scheduler.hpp"

#include <chrono>
#include <thread>
#include <sstream>

#include "engine/query.hpp"
#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "engine/types.hpp"

#include <memory>
#include "test_utils.hpp"

using namespace engine;


static void
test_global_aggregate(size_t card, const std::string& type, int aggrs = 0)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;

	uint64_t sum = 0;
	uint64_t count = 0;
	uint64_t min = 0;
	uint64_t max = card-1;

	for (size_t i=0; i<card; i++) {
		sum += i;
		count++;
	}

	{
		std::ostringstream s;

		s << sum << "|" << count;

		if (aggrs > 0) {
			s << "|" << min << "|" << max;
		}
		expected_rows.emplace_back(s.str());
	}

	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	table->add_column(TestUtils::mkSeqTestCol(type, card), "a");
	column_names.push_back("a");

	catalog.add_table("lineitem", std::move(table));

	plan::Builder builder;
	auto build = builder.scan("lineitem", column_names);

	std::vector<plan::ExprPtr> aggregates;

	aggregates.emplace_back(builder.assign("sum",
		builder.function("sum", builder.column("a"))));
	aggregates.emplace_back(builder.assign("count",
		builder.function("count", builder.column("a"))));
	if (aggrs > 0) {
		aggregates.emplace_back(builder.assign("min",
			builder.function("min", builder.column("a"))));
		aggregates.emplace_back(builder.assign("max",
			builder.function("max", builder.column("a"))));
	}

	auto got_rows = TestUtils::run_query(catalog,
		builder.hash_group_by(
		build,
		std::vector<plan::ExprPtr> { },
		aggregates));

	TestUtils::compare_results(got_rows, expected_rows);
	REQUIRE(got_rows.size() == 1);
}

#if 0
TEST_CASE("Global aggregate", "[engine][basic][groupby][aggregate]")
{
#if 0
	// supressing empty rows in global aggregate, also supresses this row
	SECTION("0 rows") {
		test_global_aggregate(0, "i64", 0);
	}
#endif

	SECTION("13 rows") {
		test_global_aggregate(13, "i64", 0);
	}

	SECTION("1024 rows") {
		test_global_aggregate(1024, "i64", 0);
	}

	SECTION("13 rows with min & max") {
		test_global_aggregate(13, "i64", 1);
	}
}
#endif



struct Entries {
	size_t sum = 0;
	size_t count = 0;
};

template<typename T>
static void
test_groupby(size_t card, const std::string& type, int aggregate, const T& group)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;

	std::unordered_map<int64_t, Entries> groupby_aggr;

	for (size_t i=0; i<card; i++) {
		auto& e = groupby_aggr[group(i)];
		e.sum += i;
		e.count++;
	}

	for (auto& keyval : groupby_aggr) {
		std::ostringstream s;
		auto& key = keyval.first;
		auto& val = keyval.second;

		s << key;

		switch (aggregate) {
		case 0: break;
		case 1: s << "|" << val.sum; break;
		case 2: s << "|" << val.sum << "|" << val.count; break;
		}
		expected_rows.emplace_back(s.str());
	}

	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	table->add_column(TestUtils::mkSeqTestColWithGen(type, card,
		[&] (auto res, auto num) {
			for (size_t i=0; i<num; i++) {
				res[i] = group(i);
			}
		}), "a");
	column_names.push_back("a");

	table->add_column(TestUtils::mkSeqTestCol(type, card), "b");
	column_names.push_back("b");

	catalog.add_table("lineitem", std::move(table));

	plan::Builder builder;
	auto build = builder.scan("lineitem", column_names);

	std::vector<plan::ExprPtr> aggregates;

	switch (aggregate) {
	case 0:
		break;

	case 1:
		aggregates.emplace_back(builder.assign("sum",
			builder.function("sum", builder.column("b"))));
		break;

	case 2:
		aggregates.emplace_back(builder.assign("sum",
			builder.function("sum", builder.column("b"))));
		aggregates.emplace_back(builder.assign("count",
			builder.function("count", builder.column("b"))));
		break;

	default:
		ASSERT(false);
		break;
	}

	auto got_rows = TestUtils::run_query(catalog,
		builder.hash_group_by(
		build,
		std::vector<plan::ExprPtr> { builder.column("a") },
		aggregates));

	TestUtils::compare_results(got_rows, expected_rows);
}


TEST_CASE("Simple groupby SUM", "[engine][basic][groupby]")
{
	SECTION("1 row") {
		test_groupby(1, "i64", 1, [] (auto k) { return k%32; });
	}

	SECTION("2 rows") {
		test_groupby(2, "i64", 1, [] (auto k) { return k%32; });
	}

	SECTION("64 rows") {
		test_groupby(64, "i64", 1, [] (auto k) { return k%32; });
	}

	SECTION("128 rows") {
		test_groupby(128, "i64", 1, [] (auto k) { return k%32; });
	}

	SECTION("1k rows") {
		test_groupby(1024, "i64", 1, [] (auto k) { return k%32; });
	}
}


TEST_CASE("Simple groupby SUM and COUNT", "[engine][basic][groupby]")
{
	SECTION("64 rows") {
		test_groupby(64, "i64", 2, [] (auto k) { return k%32; });
	}

	SECTION("128 rows") {
		test_groupby(128, "i64", 2, [] (auto k) { return k%32; });
	}

	SECTION("1k rows") {
		test_groupby(1024, "i64", 2, [] (auto k) { return k%32; });
	}
}

static void
test_groupby(size_t card, const std::string& type, int aggregate)
{
	test_groupby(card, type, aggregate, [] (auto k) { return k; });
}

TEST_CASE("Simple groupby distinct keys", "[engine][basic][groupby]")
{
	SECTION("4 rows") {
		test_groupby(4, "i64", 0);
	}

	SECTION("4 rows") {
		test_groupby(4, "i64", 1);
	}

	SECTION("4 rows") {
		test_groupby(4, "i64", 2);
	}

	SECTION("64k rows") {
		test_groupby(64*1024, "i64", 0);
	}

	SECTION("64k rows") {
		test_groupby(64*1024, "i64", 1);
	}

	SECTION("64k rows") {
		test_groupby(64*1024, "i64", 2);
	}
}


TEST_CASE("Empty groupby", "[engine][basic][groupby]")
{
	SECTION("0 rows") {
		test_groupby(0, "i64", 0);
	}

	SECTION("0 rows") {
		test_groupby(0, "i64", 1);
	}

	SECTION("0 rows") {
		test_groupby(0, "i64", 2);
	}
}