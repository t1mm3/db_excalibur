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
run_scan(size_t card, const std::string& type, int64_t* add_select_ge = nullptr,
	int add_project = 0, int64_t* add_select_le = nullptr)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;


	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	auto colA = TestUtils::mkSeqTestCol(type, card);

	expected_rows.reserve(card);

	if (add_select_ge || add_select_le) {
		for (size_t i=0; i<card; i++) {
			auto sval = colA->get(i);
			auto ival = std::stoll(sval);
			if (add_select_ge && ival < *add_select_ge) {
				continue;
			}
			if (add_select_le && ival > *add_select_le) {
				continue;
			}
			expected_rows.emplace_back(sval);
		}
	} else {
		for (size_t i=0; i<card; i++) {
			expected_rows.emplace_back(colA->get(i));
		}
	}


	table->add_column(std::move(colA), "a");
	column_names.push_back("a");

	catalog.add_table("lineitem", std::move(table));

	plan::RelOpPtr root = std::make_shared<plan::Scan>("lineitem",
		column_names);

	if (add_select_ge) {
		root = std::make_shared<plan::Select>(root,
			std::make_shared<plan::Function>(">=", std::vector<plan::ExprPtr> {
				std::make_shared<plan::ColumnId>("a"),
				std::make_shared<plan::Constant>(std::to_string(*add_select_ge))
			}));		
	}
	if (add_select_le) {
		root = std::make_shared<plan::Select>(root,
			std::make_shared<plan::Function>("<=", std::vector<plan::ExprPtr> {
				std::make_shared<plan::ColumnId>("a"),
				std::make_shared<plan::Constant>(std::to_string(*add_select_le))
			}));
	}

	switch (add_project) {
	case 0:
		break;

	case 1:
		root = std::make_shared<plan::Project>(root,
			std::vector<plan::ExprPtr> {
				std::make_shared<plan::ColumnId>("a")
			});
		break;

	case 2:
		root = std::make_shared<plan::Project>(root,
			std::vector<plan::ExprPtr> {
				std::make_shared<plan::Assign>("a",
					std::make_shared<plan::Function>("+", std::vector<plan::ExprPtr> {
						std::make_shared<plan::ColumnId>("a"),
						std::make_shared<plan::Constant>("0")
					})
				)
			});
		break;

	case 3:
		root = std::make_shared<plan::Project>(root,
			std::vector<plan::ExprPtr> {
				std::make_shared<plan::Assign>("a",
					std::make_shared<plan::Function>("+", std::vector<plan::ExprPtr> {
						std::make_shared<plan::Function>("+", std::vector<plan::ExprPtr> {
							std::make_shared<plan::ColumnId>("a"),
							std::make_shared<plan::Constant>("0")
						}),
						std::make_shared<plan::Constant>("0")
					})
				)
			});
		break;
	}

	auto got_rows = TestUtils::run_query(catalog, root);

	TestUtils::compare_results(got_rows, expected_rows);
}

TEST_CASE("Scan one column with different types", "[engine][basic]")
{
	SECTION("i64") {
		run_scan(16, "i64");
	}

	SECTION("i32") {
		run_scan(16, "i32");
	}

	SECTION("i16") {
		run_scan(16, "i16");
	}

	SECTION("i8") {
		run_scan(16, "i8");
	}
}

TEST_CASE("Scan with dummy project", "[engine][basic][project]")
{
	SECTION("0 i64") {
		run_scan(0, "i64", nullptr, 1);
	}

	SECTION("16 i64") {
		run_scan(16, "i64", nullptr, 1);
	}
}

TEST_CASE("Scan with arithmetic", "[engine][basic][project]")
{
	SECTION("0 i64") {
		run_scan(0, "i64", nullptr, 2);
	}

	SECTION("16 i64") {
		run_scan(16, "i64", nullptr, 2);
	}

	SECTION("0 i64") {
		run_scan(0, "i64", nullptr, 3);
	}

	SECTION("16 i64") {
		run_scan(16, "i64", nullptr, 3);
	}
}

TEST_CASE("Scan with filter", "[engine][basic][select]")
{
	SECTION("non-selective, 0 i64") {
		int64_t constant = 0;
		run_scan(0, "i64", &constant);
	}

	SECTION("non-selective, 16 i64") {
		int64_t constant = 0;
		run_scan(16, "i64", &constant);
	}

	SECTION("no result, 16 i64") {
		int64_t constant = 100000000;
		run_scan(16, "i64", &constant);
	}

	SECTION("1 result, 16 i64") {
		int64_t constant = 15;
		run_scan(16, "i64", &constant);
	}

	SECTION("range, 16 i64") {
		int64_t ge = 3;
		int64_t le = 9;
		run_scan(16, "i64", &ge, 0, &le);
	}

	SECTION("empty range, 16 i64") {
		int64_t ge = 9;
		int64_t le = 3;
		run_scan(16, "i64", &ge, 0, &le);
	}
}

template<typename T>
static void
run_scan2(size_t card, const std::string& type, const T& modifer,
	size_t add_unused = 0)
{
	engine::catalog::Catalog catalog;
	std::vector<std::string> expected_rows;

	for (size_t i=0; i<card; i++) {
		auto a = i;
		auto b = modifer(i);

		std::ostringstream s;
		s << a << "|" << b;
		expected_rows.emplace_back(s.str());
	}

	auto table = std::make_unique<engine::catalog::FlatArrayTable>();
	table->size = card;

	std::vector<std::string> column_names;

	table->add_column(TestUtils::mkSeqTestCol(type, card), "a");
	column_names.push_back("a");

	for (size_t i=0; i<add_unused; i++) {
		table->add_column(TestUtils::mkSeqTestCol(type, card), "unused" + std::to_string(i));
	}

	table->add_column(TestUtils::mkSeqTestColWithGen(type, card,
		[&] (auto res, auto num) {
			for (size_t i=0; i<num; i++) {
				res[i] = modifer(i);
			}
		}), "b");
	column_names.push_back("b");

	catalog.add_table("lineitem", std::move(table));

	auto got_rows = TestUtils::run_query(catalog,
		std::make_shared<plan::Scan>("lineitem",
		column_names));

	TestUtils::compare_results(got_rows, expected_rows);
}

TEST_CASE("Scan two columns", "[engine][basic]")
{
	SECTION("16 i64") {
		run_scan2(16, "i64", [] (auto i) { return i % 32; });
	}

	SECTION("128 i64") {
		run_scan2(128, "i64", [] (auto i) { return i % 32; });
	}
}

TEST_CASE("Scan two columns with useless cols", "[engine][basic]")
{
	SECTION("16 i64") {
		run_scan2(16, "i64", [] (auto i) { return i % 32; }, 3);
	}

	SECTION("128 i64") {
		run_scan2(128, "i64", [] (auto i) { return i % 32; }, 3);
	}
}