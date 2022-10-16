#pragma once
#include "catch.hpp"


#include "system/system.hpp"
#include "system/scheduler.hpp"

#include <chrono>
#include <thread>

#include "engine/query.hpp"
#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "engine/types.hpp"
#include "engine/sql_types.hpp"

#include "system/system.hpp"

#include <memory>
#include <fstream>

using namespace engine;

struct ITestCol : engine::catalog::AppendableFlatArrayColumn {
	ITestCol(std::unique_ptr<SqlType>&& sql_type) : AppendableFlatArrayColumn(std::move(sql_type)) {

	}
	virtual std::string get(size_t i) = 0;
};

template<typename T>
struct TestCol : ITestCol {
	std::vector<T> cpp_data;

	TestCol(const std::string& type, size_t card,
		const std::function<void(T*, size_t)>& gen)
	 : ITestCol(SqlType::from_string(type))
	{
		ASSERT(data_type->num_bits() == sizeof(T)*8);

		cpp_data.resize(card);

		if (card > 0) {
			gen(&cpp_data[0], card);
			append_n((void*)&cpp_data[0], card);
		}
	}

	std::string get(size_t i) final {
		return std::to_string(cpp_data[i]);
	}
};

struct TestUtils {
	struct Config {
		std::shared_ptr<QueryConfig> query;
		std::string prefix_dir;
	};

	static Config& get_config();
	static excalibur::Context& get_excalibur_context();

	template<typename T>
	static std::unique_ptr<ITestCol>
	mkSeqTestColWithGen(const std::string& type, size_t card, const T& gen)
	{
		if (!type.compare("i64")) {
			return std::make_unique<TestCol<int64_t>>(type, card, gen);
		} else if (!type.compare("i32")) {
			return std::make_unique<TestCol<int32_t>>(type, card, gen);
		} if (!type.compare("i16")) {
			return std::make_unique<TestCol<int16_t>>(type, card, gen);
		} if (!type.compare("i8")) {
			return std::make_unique<TestCol<int8_t>>(type, card, gen);
		}

		ASSERT(false);
		return nullptr;
	}


	static std::unique_ptr<ITestCol>
	mkSeqTestCol(const std::string& type, size_t card)
	{
		return mkSeqTestColWithGen(type, card, [] (auto res, auto num) {
			for (size_t i=0; i<num; i++) {
				res[i] = i;
			}
		});
	}


	static std::vector<std::vector<std::string>>
	run_queries(engine::catalog::Catalog& catalog,
		std::vector<std::unique_ptr<plan::RelPlan>>&& plans,
		QueryProfile* out_profile = nullptr,
		const std::string& out_profile_file = "");

	static std::vector<std::vector<std::string>>
	run_queries(engine::catalog::Catalog& catalog,
			const std::vector<plan::RelOpPtr>& roots,
			QueryProfile* out_profile = nullptr,
			const std::string& out_profile_file = "")
	{
		std::vector<std::unique_ptr<plan::RelPlan>> plans;

		for (auto& root : roots) {
			plans.emplace_back(std::make_unique<plan::RelPlan>(root));
		}
		return run_queries(catalog, std::move(plans), out_profile, out_profile_file);
	}

	static std::vector<std::string> run_query(engine::catalog::Catalog& catalog,
			const plan::RelOpPtr& root, QueryProfile* out_profile = nullptr,
			const std::string& out_profile_file = "")
	{
		return run_queries(catalog, { root }, out_profile, out_profile_file)[0];
	}

	static std::vector<std::string>
	get_results_from_file(const std::string& filename)
	{
		std::vector<std::string> r;

		std::ifstream file(filename.c_str());

		if (!file.is_open()) {
			LOG_ERROR("get_results_from_file: Cannot open file '%s'",
				filename.c_str());
			ASSERT(file.is_open());
		}

		std::string line;
		while (std::getline(file, line)) {
			r.emplace_back(std::move(line));
		}

		return r;
	}

	static std::string get_prefix_directory();

	static bool
	compare_results(const std::vector<std::string>& _got_rows,
		const std::vector<std::string>& _expected_rows,
		bool keep_order = false)
	{
		auto got_rows = _got_rows;
		auto expected_rows = _expected_rows;

		if (!keep_order) {
			std::sort(got_rows.begin(), got_rows.end());
			std::sort(expected_rows.begin(), expected_rows.end());
		}


		if (got_rows == expected_rows) {
			return true;
		}

		CHECK(got_rows.size() == expected_rows.size());
		size_t n = std::min(got_rows.size(), expected_rows.size());

		if (n < 10) {
			CHECK(got_rows == expected_rows);

			return got_rows == expected_rows;
		} else {
			size_t fail = 0;

			for (size_t i=0; i<n; i++) {
				const auto& got = got_rows[i];
				const auto& expected = expected_rows[i];

				CAPTURE(i);
				CHECK(got == expected);
				if (got != expected) {
					fail++;
				}

				if (fail > 10) {
					LOG_ERROR("compare_results: Aborting comparison after 10 failures");
					return false;
				}
			}

			return !fail;
		}
	}

	static bool
	compare_results_assert(const std::vector<std::string>& _got_rows,
		const std::vector<std::string>& _expected_rows,
		bool keep_order = false)
	{
		auto got_rows = _got_rows;
		auto expected_rows = _expected_rows;

		if (!keep_order) {
			std::sort(got_rows.begin(), got_rows.end());
			std::sort(expected_rows.begin(), expected_rows.end());
		}


		if (got_rows == expected_rows) {
			return true;
		}

		if (got_rows.size() != expected_rows.size()) {
			LOG_ERROR("compare_results: got %llu rows, expected %llu rows",
				got_rows.size(), expected_rows.size());
			std::vector<std::string> m_excess;
			std::vector<std::string> m_missing;

			std::set_difference(got_rows.begin(), got_rows.end(),
				expected_rows.begin(), expected_rows.end(),
				std::back_inserter(m_excess));
			std::set_difference(expected_rows.begin(), expected_rows.end(),
				got_rows.begin(), got_rows.end(),
				std::back_inserter(m_missing));

			LOG_ERROR("compare_results: introduced %llu extra rows",
				m_excess.size());
			for (size_t i=0; i<std::min(m_excess.size(), (size_t)10); i++) {
				LOG_ERROR("excess[%llu]: %s", i, m_excess[i].c_str());
			}
			LOG_ERROR("compare_results: missing %llu rows",
				m_missing.size());
			for (size_t i=0; i<std::min(m_missing.size(), (size_t)10); i++) {
				LOG_ERROR("missing[%llu]: %s", i, m_missing[i].c_str());
			}
		}

		ASSERT(got_rows.size() == expected_rows.size());
		size_t n = std::min(got_rows.size(), expected_rows.size());

		if (n < 10) {
			ASSERT(got_rows == expected_rows);

			return got_rows == expected_rows;
		} else {
			size_t fail = 0;

			for (size_t i=0; i<n; i++) {
				const auto& got = got_rows[i];
				const auto& expected = expected_rows[i];

				if (got != expected) {
					fail++;
					LOG_ERROR("result[i]: expected '%s', got '%s'", expected.c_str(), got.c_str());
				}

				if (fail > 10) {
					LOG_ERROR("compare_results: Aborting comparison after 10 failures");
					break;
				}
			}

			ASSERT(!fail);
			return !fail;
		}
	}
};
