#include "system/system.hpp"
#include "system/scheduler.hpp"

#include "third_party/cxxopts.hpp"

#include <condition_variable>
#include <iostream>
#include <string>

#include "crow.h"

int
main(int argc, char** argv)
{
	crow::SimpleApp app;
	cxxopts::Options options("Runs a query", "");

	options.add_options()
		("port", "Port", cxxopts::value<int64_t>()->default_value("40080"))
		("h,help", "Print usage")
		;

	auto result = options.parse(argc, argv);
	if (result.count("help")) {
		std::cout << options.help() << std::endl;
		exit(0);
	}

	g_scheduler.init();

	int port = result["port"].as<int64_t>();

	// crow::mustache::set_base(".");
#if 0
    CROW_ROUTE(app, "/")([](){
        return "Hello world";
    });
    app.port(port).run();
#endif

    LOG_DEBUG("Terminated");
	return 0;
}
