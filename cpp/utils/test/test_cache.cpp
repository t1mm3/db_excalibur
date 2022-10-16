#include "catch.hpp"

#include "utils/cache.hpp"
#include <unordered_set>

using namespace utils;

template<typename CacheT, typename KeySetT>
static void
_check_against_keyset(CacheT& cache, const KeySetT& key_set)
{
	cache.for_each([&] (auto& value) {
		CAPTURE(value.first);
		REQUIRE(key_set.find(value.first) != key_set.end());
	});
}

template<typename CacheT, typename KeySetT>
static void
check_against_keyset(CacheT& cache, const KeySetT& key_set)
{
	_check_against_keyset(cache, key_set);

	for (auto& key : key_set) {
		auto p = cache.lookup(key, false);
		REQUIRE(p);
		REQUIRE(*p == key);
	}
}

TEST_CASE("Basic cache test", "[cache][utils]")
{
	Cache<int, int> cache(3);

	size_t num = 0;
	cache.for_each([&] (auto& value) {
		REQUIRE(!num);
		num++;
	});

	std::unordered_set<int> key_set;

	for (int i=0; i<3; i++) {
		cache.insert({i, i});
		key_set.insert(i);
	}

	REQUIRE(cache.size() == 3);
	check_against_keyset(cache, key_set);

	key_set.erase(0);
	key_set.insert(42);
	cache.insert({42, 42});

	REQUIRE(cache.size() == 3);
	check_against_keyset(cache, key_set);

	key_set.erase(1);
	key_set.insert(43);
	cache.insert({43, 43});

	REQUIRE(cache.size() == 3);
	check_against_keyset(cache, key_set);

	key_set.erase(2);
	key_set.insert(44);
	cache.insert({44, 44});

	REQUIRE(cache.size() == 3);
	check_against_keyset(cache, key_set);

	// now [42, 43, 44]

	// erase some
	cache.erase(43);
	key_set.erase(43);

	REQUIRE(cache.size() == 2);
	check_against_keyset(cache, key_set);


	cache.erase(42);
	key_set.erase(42);

	REQUIRE(cache.size() == 1);
	check_against_keyset(cache, key_set);


	cache.erase(44);
	key_set.erase(44);

	REQUIRE(cache.size() == 0);
	check_against_keyset(cache, key_set);
}

TEST_CASE("Update cache test", "[cache][utils]")
{
	Cache<int, int> cache(3);

	cache.lookup_or_create(2, [] () {
		return 3;
	});

	auto val_ptr = cache.lookup(2);
	REQUIRE(val_ptr);
	REQUIRE(*val_ptr == 3);

	cache.lookup_or_create(2, [] () {
		REQUIRE(false);
		return 3;
	});

	val_ptr = cache.lookup(2);
	REQUIRE(val_ptr);
	REQUIRE(*val_ptr == 3);


	auto ins_ptr = cache.insert({2, 4});
	val_ptr = cache.lookup(2);
	REQUIRE(val_ptr);
	REQUIRE(*val_ptr == 4);
	REQUIRE(ins_ptr == val_ptr);


	cache.clear();
	cache.lookup_or_create(2, [] () {
		return 2;
	});
	cache.lookup_or_create(4, [] () {
		return 4;
	});
	cache.lookup_or_create(100, [] () {
		return 100;
	});

	check_against_keyset(cache, std::unordered_set<int> {2, 4, 100});
}

template<typename K, typename V>
static std::vector<K>
cache_to_keys(const Cache<K, V>& cache)
{
	std::vector<K> r;
	cache.for_each([&] (auto& value) {
		r.push_back(value.first);
	});
	return r;
}

template<typename K, typename V>
static void
check_keys(const Cache<K, V>& cache,
	const std::vector<K>& expected_keys)
{
	auto cache_keys = cache_to_keys(cache);

	REQUIRE(cache_keys == expected_keys);
}

TEST_CASE("Update placement test", "[cache][utils]")
{
	Cache<int, std::unique_ptr<int>> cache(5);

	auto check_ptr = [] (auto ptr, auto val) {
		REQUIRE(ptr);
		REQUIRE(*ptr);
		int* p = ptr->get();
		REQUIRE(*p == val);
	};

	std::unordered_set<int> key_set;

	for (size_t i=0; i<5; i++) {
		cache.insert({i, std::make_unique<int>(3*i)});
		key_set.insert(i);
	}

	_check_against_keyset(cache, key_set);
	check_keys(cache, {0, 1, 2, 3, 4});

	auto p = cache.lookup(0);
	check_ptr(p, 0);

	_check_against_keyset(cache, key_set);
	check_keys(cache, {1, 2, 3, 4, 0});

	p = cache.lookup(4);
	check_ptr(p, 3*4);

	_check_against_keyset(cache, key_set);
	check_keys(cache, {1, 2, 3, 0, 4});	

	p = cache.lookup(1);
	check_ptr(p, 3*1);

	_check_against_keyset(cache, key_set);
	check_keys(cache, {2, 3, 0, 4, 1});
}
