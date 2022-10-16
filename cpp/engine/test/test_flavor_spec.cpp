#include "catch.hpp"

#include "engine/voila/flavor.hpp"

using namespace engine::voila;

static void
compare_str_serialization(const FlavorSpec& spec)
{
	std::string str(spec.to_string());

	FlavorSpec spec2 = FlavorSpec::from_string(str);
	REQUIRE(spec == spec2);
}

TEST_CASE("Test FlavorSpec string serialization", "[engine][flavor][utils]")
{
	SECTION("Back and forth") {
		compare_str_serialization(FlavorSpec(nullptr));
		compare_str_serialization(FlavorSpec::from_string("sel=42,nosel=3"));
		compare_str_serialization(FlavorSpec::from_string("sel=42,pred=0"));
	}

	SECTION("From string") {
		FlavorSpec spec(nullptr);

		spec = FlavorSpec::from_string("sel=42");
		REQUIRE(spec.unroll_sel == 42);

		spec = FlavorSpec::from_string("sEl=42");
		REQUIRE(spec.unroll_sel == 42);

		spec = FlavorSpec::from_string("sel=42,nosel=3");
		REQUIRE(spec.unroll_sel == 42);
		REQUIRE(spec.unroll_nosel == 3);

		spec = FlavorSpec::from_string("sel=42,pred=0");
		REQUIRE(spec.unroll_sel == 42);
		REQUIRE(spec.predicated == 0);

		spec = FlavorSpec::from_string("unroll_sel=42");
		REQUIRE(spec.unroll_sel == 42);

		spec = FlavorSpec::from_string("UnrollSel=42");
		REQUIRE(spec.unroll_sel == 42);
	}
}