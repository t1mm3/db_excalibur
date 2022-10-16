#include "catch.hpp"

#include "engine/query.hpp"

using namespace engine::voila;

TEST_CASE("Test with_config()", "[config][utils]")
{
	QueryConfig old_conf;

	std::string old_vsize, old_msize;
	bool exists;

	const std::string kVSize("vector_size");
	const std::string kMSize("morsel_size");

	exists = old_conf.get(old_vsize, kVSize);
	REQUIRE(exists);

	exists = old_conf.get(old_msize, kMSize);
	REQUIRE(exists);

	SECTION("set nothing") {
		QueryConfig::with_config(old_conf,
			{},
			[&] (auto& new_conf) {
				std::string vsize, msize;
				bool exists;

				exists = new_conf.get(vsize, kVSize);
				REQUIRE(exists);
				REQUIRE(old_vsize == vsize);

				exists = new_conf.get(msize, kMSize);
				REQUIRE(exists);
				REQUIRE(old_msize == msize);
			}
		);
	}

	SECTION("set some") {
		QueryConfig::with_config(old_conf,
			{QueryConfig::KeyValuePair {kVSize, "10013"}},
			[&] (auto& new_conf) {
				std::string vsize, msize;
				bool exists;

				exists = new_conf.get(vsize, kVSize);
				REQUIRE(exists);
				REQUIRE("10013" == vsize);

				exists = new_conf.get(msize, kMSize);
				REQUIRE(exists);
				REQUIRE(old_msize == msize);
			}
		);
	}


	// must reset
	std::string post_vsize, post_msize;

	exists = old_conf.get(post_vsize, kVSize);
	REQUIRE(exists);
	REQUIRE(old_vsize == post_vsize);

	exists = old_conf.get(post_msize, kMSize);
	REQUIRE(exists);
	REQUIRE(old_msize == post_msize);
}


#include "engine/voila/statement_identifier.hpp"

TEST_CASE("Test StatementRange", "[jit][utils][statement-range]")
{
	using Id = engine::voila::StatementIdentifier;
	using Range = engine::voila::StatementRange;

	Range range0_1(Id {0, {0}}, Id {0, {1}});
	Range range5_6(Id {0, {5}}, Id {0, {6}});
	Range range6_7(Id {0, {6}}, Id {0, {7}});
	Range range7_0_7_1(Id {0, {7, 0}}, Id {0, {7, 1}});
	Range range7_2_7_3(Id {0, {7, 2}}, Id {0, {7, 3}});
	Range range5_2_5_3(Id {0, {5, 2}}, Id {0, {5, 3}});
	Range range0_10(Id {0, {0}}, Id {0, {10}});

	REQUIRE(range0_10.overlaps(range0_1));
	REQUIRE(range0_1.overlaps(range0_10));

	REQUIRE(range0_10.overlaps(range7_0_7_1));
	REQUIRE(range7_0_7_1.overlaps(range0_10));

	REQUIRE(!range5_6.overlaps(range6_7));
	REQUIRE(!range6_7.overlaps(range5_6));

	REQUIRE(!range0_1.overlaps(range5_6));
	REQUIRE(!range5_6.overlaps(range0_1));

	REQUIRE(!range5_6.overlaps(range7_0_7_1));
	REQUIRE(!range7_0_7_1.overlaps(range5_6));

	REQUIRE(!range7_2_7_3.overlaps(range7_0_7_1));
	REQUIRE(!range7_0_7_1.overlaps(range7_2_7_3));

	REQUIRE(!range7_2_7_3.overlaps(range5_2_5_3));
	REQUIRE(!range5_2_5_3.overlaps(range7_2_7_3));

	REQUIRE(range0_1.overlaps(range0_1));
	REQUIRE(range7_2_7_3.overlaps(range7_2_7_3));
}


TEST_CASE("Test StatementRange 2 (#57)", "[jit][utils][statement-range]")
{
	using Id = engine::voila::StatementIdentifier;
	using Range = engine::voila::StatementRange;

	Range range1(Id {0, {2, 2}}, Id {0, {2, 5}});
	Range range2(Id {0, {2, 5}}, Id {0, {2, 6}});
	Range range3(Id {0, {2, 5, 0}}, Id {0, {2, 5, 2}});

	REQUIRE(!range1.overlaps(range2));
	REQUIRE(!range2.overlaps(range1));

	REQUIRE(range2.overlaps(range3));
	REQUIRE(range3.overlaps(range2));
}
