#include "catch.hpp"

#include "system/system.hpp"
#include "system/scheduler.hpp"

#include <chrono>
#include <thread>
#include <sstream>

#include "engine/query.hpp"
#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "engine/sql_types.hpp"

#include "engine/storage/csv_loader.hpp"

#include <memory>
#include "test_utils.hpp"

using namespace engine;
using namespace catalog;
using namespace storage;

TEST_CASE("Load simple CSV data", "[engine][csv]")
{
	Catalog catalog;
	std::vector<std::string> expected_rows;
	std::vector<std::string> csv_data;

	for (size_t i=0; i<13; i++) {
		std::ostringstream exp;
		std::ostringstream csv;

		exp << i << "|" << i << "|" << i*10 << "|" << (i*10000 + 3*1000);
		csv << i << "|" << i << "|" << i 	<< "|" << i<<"." <<3;

		expected_rows.emplace_back(exp.str());
		csv_data.emplace_back(csv.str());
	}


	auto table = std::make_unique<FlatArrayTable>();

	auto column = [&] (std::string type) {
		table->add_column(
			std::make_shared<AppendableFlatArrayColumn>(SqlType::from_string(type)),
		"c" + std::to_string(table->get_num_columns()));
	};

	column("bigint");
	column("decimal(2,0)");
	column("decimal(2,1)");
	column("decimal(8,4)");

	CsvLoader csv(*table);

	for (auto& line : csv_data) {
		csv.push_row(line);
	}

	csv.flush();


	auto plan = std::make_unique<plan::RelPlan>(
		std::make_shared<plan::Scan>("lineitem", table->get_column_names()));

	catalog.add_table("lineitem", std::move(table));
	auto query = std::make_shared<Query>(TestUtils::get_excalibur_context(),
		1, catalog, std::move(plan));
	g_scheduler.submit(query);
	query->wait();

	// check result
	auto got_rows = query->result.rows;

	CHECK(got_rows.size() == expected_rows.size());

	std::sort(got_rows.begin(), got_rows.end());
	std::sort(expected_rows.begin(), expected_rows.end());

	REQUIRE(got_rows == expected_rows);
}

TEST_CASE("Load simple CSV data big", "[engine][csv]")
{
	Catalog catalog;
	std::vector<std::string> expected_rows;
	std::vector<std::string> csv_data;

	for (size_t i=0; i<64*1024+13; i++) {
		std::ostringstream exp;
		std::ostringstream csv;

		exp << i << "|" << i << "|" << i*10;
		csv << i << "|" << i << "|" << i;

		expected_rows.emplace_back(exp.str());
		csv_data.emplace_back(csv.str());
	}


	auto table = std::make_unique<FlatArrayTable>();

	auto column = [&] (std::string type) {
		table->add_column(
			std::make_shared<AppendableFlatArrayColumn>(SqlType::from_string(type)),
		"c" + std::to_string(table->get_num_columns()));
	};

	column("bigint");
	column("decimal(6,0)");
	column("decimal(6,1)");

	CsvLoader csv(*table);

	for (auto& line : csv_data) {
		csv.push_row(line);
	}

	csv.flush();


	auto plan = std::make_unique<plan::RelPlan>(
		std::make_shared<plan::Scan>("lineitem", table->get_column_names()));

	catalog.add_table("lineitem", std::move(table));
	auto query = std::make_shared<Query>(TestUtils::get_excalibur_context(),
		1, catalog, std::move(plan));
	g_scheduler.submit(query);
	query->wait();

	// check result
	auto got_rows = query->result.rows;

	CHECK(got_rows.size() == expected_rows.size());

	std::sort(got_rows.begin(), got_rows.end());
	std::sort(expected_rows.begin(), expected_rows.end());

	for (size_t i=0; i<got_rows.size(); i++) {
		REQUIRE(got_rows[i] == expected_rows[i]);
	}
}
