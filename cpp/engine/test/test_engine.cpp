#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <iostream>
#include <thread>
#include "test_utils.hpp"
#include "system/scheduler.hpp"

static QueryConfig*
get_query_config()
{
	auto& config = TestUtils::get_config();
	if (!config.query) {
		config.query = std::make_shared<QueryConfig>();
	}
	return config.query.get();
}

int
main(int argc, char* argv[])
{
	Catch::Session session;

	std::string config;
	long threads = -0;

	using namespace Catch::clara;
	auto cli = session.cli()
		| Opt(config, "config")
			["--config"]
			("Custom configuration string")
		| Opt(threads, "threads")
			["--threads"]
			("#threads")
		;

	session.cli(cli); 
	int r = session.applyCommandLine(argc, argv);
	if (r) {
		return r;
	}

	if (!config.empty()) {
		std::cerr << "Using custom config = " << config << std::endl;
		auto qconf = get_query_config();
		qconf->overwrite(config);
	}

	if (threads > 0) {
		std::cerr << "Using custom #threads = " << threads << std::endl;
		g_scheduler.init(threads);
	}

	return session.run();
}