#include "catch.hpp"

#include "engine/plan.hpp"
#include <memory>

#if 0
TEST_CASE("Parse trees", "[engine][parser]")
{
	SECTION("dense") {
		LOG_WARN("2");
		auto node = plan::parse_plan("select(scan(a, []), [])");
		LOG_WARN("3");
		node = plan::parse_plan("select(scan(a), [a, b])");
		LOG_WARN("4");
		node = plan::parse_plan("select(scan(lineitem,a,b),[==(a,a)])");
	}
}
#endif