#include "catch.hpp"

#include "system/system.hpp"
#include "engine/catalog.hpp"
#include "engine/storage/csv_loader.hpp"
#include "tpch_test_utils.hpp"
#include "test_utils.hpp"

#include <map>

// #define TEST

#define TEST_Q3

static std::map<std::string, engine::catalog::Catalog> catalogs;

using namespace plan;

typedef std::vector<ExprPtr> ExprPtrVec;

template<typename CATALOG>
static void run_sequential(CATALOG& cat, std::string sf = "1")
{
	auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
	auto expected_rows = TpchUtils::get_results(sf, 1);

	TestUtils::compare_results(got_rows, expected_rows);

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
	expected_rows = TpchUtils::get_results(sf, 6);

	TestUtils::compare_results(got_rows, expected_rows);

#ifdef TEST_Q3
	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
	expected_rows = TpchUtils::get_results(sf, 3);

	TestUtils::compare_results(got_rows, expected_rows);
#endif

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(4));
	expected_rows = TpchUtils::get_results(sf, 4);

	TestUtils::compare_results(got_rows, expected_rows);

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
	expected_rows = TpchUtils::get_results(sf, 9);

	TestUtils::compare_results(got_rows, expected_rows);

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(10));
	expected_rows = TpchUtils::get_results(sf, 10);

	TestUtils::compare_results(got_rows, expected_rows);

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(12));
	expected_rows = TpchUtils::get_results(sf, 12);

	TestUtils::compare_results(got_rows, expected_rows);

	got_rows = TestUtils::run_query(cat, TpchUtils::get_query(18));
	expected_rows = TpchUtils::get_results(sf, 18);

	TestUtils::compare_results(got_rows, expected_rows);
}


TEST_CASE("TPC-H SF0.01", "[engine][tpch][tpch0.01]")
{
	const std::string scale_factor = "0.01";
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

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Q1") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
		REQUIRE(got_rows.size() == 4);
	}

	SECTION("Q3") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
		REQUIRE(got_rows.size() >= 1);
	}

	SECTION("Q6") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
		REQUIRE(got_rows.size() == 1);
	}

	SECTION("Q9") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
		REQUIRE(got_rows.size() >= 1);
	}
}


TEST_CASE("TPC-H SF1", "[engine][tpch][tpch1]")
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

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Q1") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
		auto expected_rows = TpchUtils::get_results("1", 1);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#ifdef TEST_Q3
	SECTION("Q3") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
		auto expected_rows = TpchUtils::get_results("1", 3);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#endif
	SECTION("Q4") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(4));
		auto expected_rows = TpchUtils::get_results("1", 4);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
		auto expected_rows = TpchUtils::get_results("1", 6);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q9") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
		auto expected_rows = TpchUtils::get_results("1", 9);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q10") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(10));
		auto expected_rows = TpchUtils::get_results("1", 10);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q12") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(12));
		auto expected_rows = TpchUtils::get_results("1", 12);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q18") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(18));
		auto expected_rows = TpchUtils::get_results("1", 18);

		TestUtils::compare_results(got_rows, expected_rows);
		REQUIRE(got_rows.size() == 57);
	}

	SECTION("Sequential") {
		run_sequential(cat, "1");
	}

	SECTION("Par Stream") {
		size_t num_streams = 4;
		size_t num_runs = 2;
		std::vector<std::future<void>> futures;
		for (size_t i=0; i<num_streams; i++) {
			futures.emplace_back(scheduler::async([&] () {
				for (size_t i=0; i<num_runs; i++) {
					run_sequential(cat, "1");
				}
			}));
		}

		for (auto& future: futures) {
			future.wait();
		}
	}
}


TEST_CASE("TPC-H SF1 compact", "[engine][tpch][tpch1][compact]")
{
	const std::string scale_factor = "1";
	const auto tpch_csv_files = TpchUtils::get_files(scale_factor);
	auto& cat = catalogs[scale_factor];
	if (!cat.get_num_tables()) {
		TpchUtils::create_schema(cat, true, scale_factor);

		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;
			auto& file_name = file_mapping.second;

			auto table = cat.get_table(table_name);
			ASSERT(table);

			engine::storage::CsvLoader csv(*table);

			csv.from_file(file_name, "compact");
			csv.flush();

			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Q1") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
		auto expected_rows = TpchUtils::get_results("1", 1);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#ifdef TEST_Q3
	SECTION("Q3") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
		auto expected_rows = TpchUtils::get_results("1", 3);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#endif
	SECTION("Q4") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(4));
		auto expected_rows = TpchUtils::get_results("1", 4);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
		auto expected_rows = TpchUtils::get_results("1", 6);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q9") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
		auto expected_rows = TpchUtils::get_results("1", 9);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q10") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(10));
		auto expected_rows = TpchUtils::get_results("1", 10);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q12") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(12));
		auto expected_rows = TpchUtils::get_results("1", 12);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q18") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(18));
		auto expected_rows = TpchUtils::get_results("1", 18);

		TestUtils::compare_results(got_rows, expected_rows);
		REQUIRE(got_rows.size() == 57);
	}

	SECTION("Sequential") {
		run_sequential(cat, "1");
	}

	SECTION("Par Stream") {
		size_t num_streams = 4;
		size_t num_runs = 2;
		std::vector<std::future<void>> futures;
		for (size_t i=0; i<num_streams; i++) {
			futures.emplace_back(scheduler::async([&] () {
				for (size_t i=0; i<num_runs; i++) {
					run_sequential(cat, "1");
				}
			}));
		}

		for (auto& future: futures) {
			future.wait();
		}
	}
}



TEST_CASE("TPC-H SF10", "[engine][tpch][tpch10][.]")
{
	const std::string scale_factor = "10";
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

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Q1") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
		auto expected_rows = TpchUtils::get_results("10", 1);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
		auto expected_rows = TpchUtils::get_results("10", 6);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q9") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
		auto expected_rows = TpchUtils::get_results("10", 9);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q10") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(10));
		auto expected_rows = TpchUtils::get_results("10", 10);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q12") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(12));
		auto expected_rows = TpchUtils::get_results("10", 12);

		TestUtils::compare_results(got_rows, expected_rows);
	}

#ifdef TEST_Q3
	SECTION("Q3") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
		auto expected_rows = TpchUtils::get_results("10", 3);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#endif

	SECTION("Q4") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(4));
		auto expected_rows = TpchUtils::get_results("10", 4);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q18") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(18));
		auto expected_rows = TpchUtils::get_results("10", 18);

		TestUtils::compare_results(got_rows, expected_rows);
		REQUIRE(got_rows.size() == 624);
	}

	SECTION("Sequential") {
		run_sequential(cat, "10");
	}

	SECTION("Par Stream") {
		size_t num_streams = 4;
		size_t num_runs = 2;
		std::vector<std::future<void>> futures;
		for (size_t i=0; i<num_streams; i++) {
			futures.emplace_back(scheduler::async([&] () {
				for (size_t i=0; i<num_runs; i++) {
					run_sequential(cat, "10");
				}
			}));
		}

		for (auto& future: futures) {
			future.wait();
		}
	}
}

#if 1
TEST_CASE("TPC-H SF100", "[engine][tpch][tpch100][.]")
{
	const std::string scale_factor = "100";
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

	SECTION("Check not empty") {
		for (auto& file_mapping : tpch_csv_files) {
			auto& table_name = file_mapping.first;

			auto table = cat.get_table(table_name);
			REQUIRE(table);
			REQUIRE(table->get_num_rows() > 0);
		}
	}

	SECTION("Q1") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(1));
		auto expected_rows = TpchUtils::get_results("100", 1);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q6") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(6));
		auto expected_rows = TpchUtils::get_results("100", 6);

		TestUtils::compare_results(got_rows, expected_rows);
	}

	SECTION("Q9") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(9));
		auto expected_rows = TpchUtils::get_results("100", 9);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#ifdef TEST_Q3
	SECTION("Q3") {
		auto got_rows = TestUtils::run_query(cat, TpchUtils::get_query(3));
		auto expected_rows = TpchUtils::get_results("100", 3);

		TestUtils::compare_results(got_rows, expected_rows);
	}
#endif

	SECTION("Sequential") {
		run_sequential(cat, "100");
	}

	SECTION("Par Stream") {
		size_t num_streams = 4;
		size_t num_runs = 2;
		std::vector<std::future<void>> futures;
		for (size_t i=0; i<num_streams; i++) {
			futures.emplace_back(scheduler::async([&] () {
				for (size_t i=0; i<num_runs; i++) {
					run_sequential(cat, "100");
				}
			}));
		}

		for (auto& future: futures) {
			future.wait();
		}
	}
}
#endif