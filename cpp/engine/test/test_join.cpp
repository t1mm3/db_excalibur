#include "catch.hpp"

#include "excalibur_context.hpp"
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
#include <future>
#include "test_utils.hpp"

using namespace engine;

struct SelfJoinParam {
	size_t card;
	const std::string& type;
	bool fk1;
};


static void
_test_self_join(size_t card, const std::string& type, bool fk1)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;

	for (size_t i=0; i<card; i++) {
		std::ostringstream s;
		s << i << "|" << i;
		expected_rows.emplace_back(s.str());
	}

	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	auto colA = TestUtils::mkSeqTestCol(type, card);

	table->add_column(std::move(colA), "a");
	column_names.push_back("a");

	catalog.add_table("lineitem", std::move(table));

	plan::Builder builder;
	auto build = builder.scan("lineitem", column_names);
	auto probe = builder.scan("lineitem", column_names);

	auto got_rows = TestUtils::run_query(catalog, builder.hash_join(
		build,
		builder.column("a"),
		probe,
		builder.column("a"), fk1));

	CHECK(got_rows.size() == card);

	TestUtils::compare_results(got_rows, expected_rows);
}

static void
test_self_join(const std::vector<SelfJoinParam>& params)
{
	bool parallel = params.size() > 1;

	if (parallel) {
		std::vector<std::future<void>> tasks;

		for (auto& param : params) {
	    	tasks.emplace_back(std::async(std::launch::async,
	    		_test_self_join,
	    		param.card, param.type, param.fk1));
		}

		for (auto& task : tasks) {
			task.wait();
		}
	} else {
		for (auto& param : params) {
	    	_test_self_join(param.card, param.type, param.fk1);
		}
	}
}

static void
test_self_join(size_t card, const std::string& type, bool fk1)
{
	test_self_join(std::vector<SelfJoinParam> {
		SelfJoinParam { card, type, fk1 }
	});
}

TEST_CASE("Self join", "[engine][basic][join][joinN]")
{
	bool fk1 = false;

	SECTION("0 rows") {
		test_self_join(0, "i64", fk1);
	}

	SECTION("2 rows") {
		test_self_join(2, "i64", fk1);
	}

	SECTION("3 rows") {
		test_self_join(3, "i64", fk1);
	}

	SECTION("4 rows") {
		test_self_join(4, "i64", fk1);
	}

	SECTION("16 rows") {
		test_self_join(16, "i64", fk1);
	}

	SECTION("32 rows") {
		test_self_join(32, "i64", fk1);
	}

	SECTION("48 rows") {
		test_self_join(48, "i64", fk1);
	}

	SECTION("64 rows") {
		test_self_join(64, "i64", fk1);
	}

	SECTION("128 rows") {
		test_self_join(128, "i64", fk1);
	}
}

TEST_CASE("Self join fk1", "[engine][basic][join][join1]")
{
	bool fk1 = true;

	SECTION("0 rows") {
		test_self_join(0, "i64", fk1);
	}

	SECTION("2 rows") {
		test_self_join(2, "i64", fk1);
	}

	SECTION("3 rows") {
		test_self_join(3, "i64", fk1);
	}

	SECTION("4 rows") {
		test_self_join(4, "i64", fk1);
	}

	SECTION("16 rows") {
		test_self_join(16, "i64", fk1);
	}

	SECTION("32 rows") {
		test_self_join(32, "i64", fk1);
	}

	SECTION("48 rows") {
		test_self_join(48, "i64", fk1);
	}

	SECTION("64 rows") {
		test_self_join(64, "i64", fk1);
	}

	SECTION("128 rows") {
		test_self_join(128, "i64", fk1);
	}
}


TEST_CASE("Self join big 1", "[engine][basic][join][join1]")
{
	test_self_join({ {64000, "i64", true}, {128000, "i64", true} });
}

TEST_CASE("Self join big N", "[engine][basic][join][joinN]")
{
	test_self_join({ {64000, "i64", false}, {128000, "i64", false} });
}




static void
test_self_join_n(size_t card, const std::string& type, size_t group_size)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;

	for (size_t r=0; r<card; r++) {
		for (size_t i=0; i<card/group_size; i++) {
			std::ostringstream s;
			s << (i % group_size) << "|" << (i % group_size);
			expected_rows.emplace_back(s.str());
		}
	}

	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	auto colA = TestUtils::mkSeqTestColWithGen(type, card,
		[&] (auto res, auto num) {
			for (size_t i=0; i<num; i++) {
				res[i] = i % group_size;
			}
		});

	table->add_column(std::move(colA), "a");
	column_names.push_back("a");

	catalog.add_table("lineitem", std::move(table));

	plan::Builder builder;
	auto build = builder.scan("lineitem", column_names);
	auto probe = builder.scan("lineitem", column_names);

	auto got_rows = TestUtils::run_query(catalog,
		builder.hash_join(
			build,
			builder.column("a"),
			probe,
			builder.column("a"),
			false));

	CHECK(got_rows.size() == expected_rows.size());

	TestUtils::compare_results(got_rows, expected_rows);
}

TEST_CASE("Self joinN", "[engine][basic][join][joinN]")
{
	SECTION("16 rows, 1 group") {
		test_self_join_n(16, "i64", 1);
	}

	SECTION("16 rows, 4 groups") {
		test_self_join_n(16, "i64", 4);
	}

	SECTION("128 rows, 4 groups") {
		test_self_join_n(128, "i64", 4);
	}
}

#if 0

#include "engine/voila/code_cache.hpp"

template<typename T>
static size_t get_num_compiled(const T& cache)
{
	auto stats = cache.get_stats();

	return stats.num_compiled;
}

TEST_CASE("Test hash join caching", "[engine][code-cache]")
{
	auto& cache = TestUtils::get_excalibur_context().get_code_cache();
	cache.clear();

	test_self_join(4, "i64", false);

	// initial run should trigger compilation
	REQUIRE(get_num_compiled(cache) > 0);

	cache.reset_stats();

	for (int i=0; i<3; i++) {
		CAPTURE(i);

		test_self_join(4, "i64", false);

		// should require no re-compilation
		REQUIRE(get_num_compiled(cache) == 0);
	}
}


TEST_CASE("Clear code cache", "[engine][code-cache]")
{
	auto& cache = TestUtils::get_excalibur_context().get_code_cache();
	cache.clear();
}

#endif