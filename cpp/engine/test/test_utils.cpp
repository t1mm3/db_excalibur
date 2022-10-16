#include "test_utils.hpp"
#include "tpch_test_utils.hpp"
#include "excalibur_context.hpp"
#include "catch.hpp"

TestUtils::Config g_test_config;

TestUtils::Config&
TestUtils::get_config()
{
	return g_test_config;
}

static excalibur::Context g_context;

excalibur::Context&
TestUtils::get_excalibur_context()
{
	return g_context;
}



static std::atomic<size_t> query_id = 1;

std::vector<std::vector<std::string>>
TestUtils::run_queries(engine::catalog::Catalog& catalog,
	std::vector<std::unique_ptr<plan::RelPlan>>&& plans,
	QueryProfile* out_profile, const std::string& out_profile_file)
{
	std::vector<std::vector<std::string>> results;
	std::vector<std::shared_ptr<Query>> queries;
	for (auto&& plan : plans) {
		auto query = std::make_shared<Query>(get_excalibur_context(),
			query_id.fetch_add(1),
			catalog, std::move(plan), get_config().query);
		g_scheduler.submit(query);

		queries.emplace_back(query);
	}

	for (auto& query : queries) {
		query->wait();

		results.push_back(query->result.rows);
	}

	if (out_profile) {
		out_profile->aggregate(*queries[0]->profile);

		std::fstream f(
			out_profile_file.empty() ?
			"/tmp/profile.json" : out_profile_file,
			std::ios::out | std::ios::trunc);
		ASSERT(f.is_open());

		try {
			queries[0]->profile->to_json(f);
		} catch (...) {
			f.close();
			throw;
		}
	}
	return results;
}

std::string
TestUtils::get_prefix_directory()
{
	return TpchUtils::get_prefix_directory();
}