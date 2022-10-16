#include "catch.hpp"
#include "engine/lolepops/sort_helper.hpp"

using namespace engine;
using namespace engine::lolepop;

template<typename T>
void sort_scalar_values(std::string type, size_t card, bool ascending)
{
	std::vector<T> data;
	data.reserve(card);

	std::vector<T> data2;
	data2.reserve(card);	

	for (size_t i=0; i<card; i++) {
		T v = v % 2 ? -i : i;
		data.push_back(v);
		data2.push_back(v);
	}

	SortHelper sorter;

	if (ascending) {
		sorter.ascending(TypeSystem::from_string(type)); 
		std::sort(data2.begin(), data2.end(), std::less<int>());
	} else {
		sorter.descending(TypeSystem::from_string(type)); 
		std::sort(data2.begin(), data2.end(), std::greater<int>());
	}

	sorter.sort((char*)&data[0], data.size());


	REQUIRE(data == data2);
}

TEST_CASE("Sort scalar values", "[engine][sort][sort-helper]")
{
	sort_scalar_values<int32_t>("i32", 16, true);
	sort_scalar_values<int32_t>("i32", 16, false);

	sort_scalar_values<int8_t>("i8", 16, true);
	sort_scalar_values<int8_t>("i8", 16, false);
}
