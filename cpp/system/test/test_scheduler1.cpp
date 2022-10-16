#include "catch.hpp"
#include "system/scheduler.hpp"

#include <atomic>


std::atomic<uint64_t> g_some_func_count = 0;

struct Object {
	uint64_t val;
};

uint64_t some_func_int(int val) {
	return g_some_func_count.fetch_add(val);
}

uint64_t some_func_copy(Object add) {
	return g_some_func_count.fetch_add(add.val);
}

TEST_CASE("std::async", "[std]")
{
	std::vector<std::future<uint64_t>> futures;
	size_t num_futures = 32;


	SECTION("std::shared_future: Get multiple times") {
		g_some_func_count = 0;
		auto future = std::async(std::launch::async,
			some_func_copy, Object { 42 }).share();

		REQUIRE(future.valid());
		future.wait();
		REQUIRE(g_some_func_count == 42);
		REQUIRE(future.get() == 0);
		REQUIRE(g_some_func_count == 42);
		REQUIRE(future.get() == 0);
		REQUIRE(g_some_func_count == 42);
		REQUIRE(future.get() == 0);
		REQUIRE(g_some_func_count == 42);
	}

	SECTION("Direct call") {
		g_some_func_count = 0;
		futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			futures.emplace_back(std::async(std::launch::async,
				some_func_int, 42));
		}

		for (auto& future : futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(g_some_func_count == 42 * num_futures);
	}

	SECTION("Direct call copy") {
		g_some_func_count = 0;
		futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			futures.emplace_back(std::async(std::launch::async,
				some_func_copy, Object { 42 }));
		}

		for (auto& future : futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(g_some_func_count == 42 * num_futures);
	}
}


TEST_CASE("Async", "[system][scheduler]")
{
	std::vector<std::future<uint64_t>> futures;
	std::vector<std::shared_future<uint64_t>> shared_futures;
	size_t num_futures = 32;

	SECTION("Direct call") {
		g_some_func_count = 0;
		futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			futures.emplace_back(scheduler::async(some_func_int, 42));
		}

		for (auto& future : futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(scheduler::is_idle());
		REQUIRE(g_some_func_count == 42 * num_futures);
	}

	SECTION("Direct call copy") {
		g_some_func_count = 0;
		futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			futures.emplace_back(scheduler::async(some_func_copy, Object { 42 }));
		}

		for (auto& future : futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(scheduler::is_idle());
		REQUIRE(g_some_func_count == 42 * num_futures);
	}

	SECTION("Direct call move") {
		g_some_func_count = 0;
		futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			auto obj = Object { 42 };
			futures.emplace_back(scheduler::async(some_func_copy, std::move(obj)));
		}

		for (auto& future : futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(scheduler::is_idle());
		REQUIRE(g_some_func_count == 42 * num_futures);
	}

	SECTION("Direct call copy share") {
		g_some_func_count = 0;
		shared_futures.clear();

		for (size_t i=0; i<num_futures; i++) {
			shared_futures.emplace_back(
				scheduler::async(some_func_copy, Object { 42 }).share());
		}

		for (auto& future : shared_futures) {
			REQUIRE(future.valid());
			future.wait();
		}
		REQUIRE(scheduler::is_idle());
		REQUIRE(g_some_func_count == 42 * num_futures);
	}

	SECTION("Get multiple times") {
		g_some_func_count = 0;
		auto future = scheduler::async(some_func_copy, Object { 42 }).share();

		REQUIRE(future.valid());
		future.wait();
		REQUIRE(scheduler::is_idle());
		REQUIRE(future.get() == 0);
		REQUIRE(future.get() == 0);
		REQUIRE(future.get() == 0);
		REQUIRE(g_some_func_count == 42);
	}

	SECTION("Complex call") {
		auto check = [&] (auto result, auto func) {
			REQUIRE(func().get() == result);
		};
		
		check(42, [] () {
			return scheduler::async([] (std::string a) {
				REQUIRE(a == "test");
				return 42;
			}, "test");
		});

		check(42, [] () {
			return scheduler::async([] (const std::string& a) {
				REQUIRE(a == "test");
				return 42;
			}, "test");
		});

		check(42, [] () {
			return scheduler::async([] (const std::string& a) {
				REQUIRE(a == "test");
				return 42;
			}, std::move("test"));
		});

		REQUIRE(scheduler::is_idle());
	}
}


