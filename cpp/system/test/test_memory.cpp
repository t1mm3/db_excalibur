#include "catch.hpp"
#include "system/memory.hpp"

#include <deque>

using namespace memory;


TEST_CASE("memory chunk", "[memory]")
{
	MemoryChunkFactory factory;

	auto chunk = factory.alloc_chunk(1024*1024);

	SECTION("allocate-free") {
		for (size_t r=0; r<16; r++) {
			CAPTURE(r);

			auto p = chunk->allocate(64);
			REQUIRE(p);
			chunk->free(p);
		}
	}

	SECTION("allocate-free-cycle") {
		for (size_t r=0; r<16; r++) {
			CAPTURE(r);

			std::vector<char*> ptrs;
			for (size_t i=0; i<128; i++) {
				auto p = chunk->allocate(64);
				REQUIRE(p);
				ptrs.push_back(p);
			}

			for (auto& ptr : ptrs) {
				chunk->free(ptr);
			}
		}
	}

	SECTION("reverse allocate-free-cycle") {
		for (size_t r=0; r<16; r++) {
			CAPTURE(r);

			std::deque<char*> ptrs;
			for (size_t i=0; i<128; i++) {
				auto p = chunk->allocate(64);
				REQUIRE(p);
				ptrs.push_front(p);
			}

			for (auto& ptr : ptrs) {
				chunk->free(ptr);
			}
		}
	}

	SECTION("allocate-free-free-cycle") {
		for (size_t r=0; r<16; r++) {
			CAPTURE(r);

			std::vector<char*> ptrs;
			for (size_t i=0; i<64; i++) {
				auto p = chunk->allocate(64);
				REQUIRE(p);
				ptrs.push_back(p);
			}

			for (size_t i=0; i<32; i++) {
				chunk->free(ptrs[i]);
			}

			for (size_t i=0; i<32; i++) {
				chunk->free(ptrs[32+i]);
			}
		}
	}


	factory.free_chunk(chunk);
}

