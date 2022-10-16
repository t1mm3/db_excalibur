#include "system/system.hpp"
#include "system/scheduler.hpp"

#include <chrono>
#include <thread>
#include <iomanip>

#include "engine/query.hpp"
#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "third_party/cxxopts.hpp"

#include <memory>
#include <functional>

struct Runtime {
	double time_ms;
};

static std::vector<Runtime> g_runtimes;
void
run_kernel(const std::function<void()>& kernel, int64_t num_warmup, int64_t num_runs)
{
	int64_t run = 0;
	for (int64_t i=0; i<num_warmup; i++) {
		kernel();
		run++;
	}

	for (int64_t i=0; i<num_runs; i++) {
		auto start = std::chrono::high_resolution_clock::now();
		kernel();
		auto end = std::chrono::high_resolution_clock::now();
		run++;
		double time_ms = std::chrono::duration<double, std::milli>(end-start).count();

		g_runtimes.emplace_back(Runtime { time_ms });
	} 
}

int
main(int argc, char** argv)
{
	cxxopts::Options options("Runs a query", "");

	options.add_options()
		("data", "Data directory", cxxopts::value<std::string>()->default_value("data"))
		("warmup", "Warmup round", cxxopts::value<int64_t>()->default_value("3"))
		("r,runs", "Rounds", cxxopts::value<int64_t>()->default_value("3"))
		("h,help", "Print usage")
		;

	auto result = options.parse(argc, argv);

	if (result.count("help")) {
		std::cout << options.help() << std::endl;
		exit(0);
	}

	const size_t num_warmup = result["warmup"].as<int64_t>();
	const size_t num_runs = result["runs"].as<int64_t>();

	g_scheduler.init();

	g_runtimes.reserve(num_warmup + num_runs);

	run_kernel([&] () {
		
		// wait until all threads are done
		g_scheduler.wait_until_idle();
	}, num_warmup, num_runs);

	std::sort(g_runtimes.begin(), g_runtimes.end(), [] (const auto& a, const auto& b) {
		return a.time_ms < b.time_ms;
	});

	double time_ms_sum = 0.0;
	double num_runtimes = g_runtimes.size();
	for (size_t i=0; i<g_runtimes.size(); i++) {
		time_ms_sum += g_runtimes[i].time_ms;
	}

	std::cout
		<< "average: " << time_ms_sum/num_runtimes << "ms" << std::endl;


	
#if 0
	Catalog catalog;

	auto plan = std::make_unique<plan::RelPlan>();

	plan->root = std::make_shared<plan::Scan>("lineitem",
		std::vector<std::string> { std::string("a") });

	g_scheduler.submit(std::make_unique<Query>(1, catalog, std::move(plan)));

	g_scheduler.init();
	LOG_INFO("Running query");

	std::this_thread::sleep_for(std::chrono::seconds(10));
#endif
	return 0;
}
