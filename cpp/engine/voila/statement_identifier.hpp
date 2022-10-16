#pragma once

#include <ostream>
#include <vector>
#include <memory>

#include "system/system.hpp"

namespace engine {
namespace voila {

struct StatementIdentifier {
	size_t operator_id;
	std::vector<size_t> line_numbers;

	StatementIdentifier(size_t operator_id, const std::vector<size_t>& line_numbers)
	 : operator_id(operator_id), line_numbers(line_numbers) {

	}

	~StatementIdentifier() {
	}

	StatementIdentifier& operator=(const StatementIdentifier& o) {
		operator_id = o.operator_id;
		line_numbers = o.line_numbers;
		return *this;
	}

	bool operator==(const StatementIdentifier& o) const {
		if (o.operator_id != operator_id) {
			return false;
		}

		if (o.depth() != depth()) {
			return false;
		}

		for (size_t i=0; i<line_numbers.size(); i++) {
			if (o.line_numbers[i] != line_numbers[i]) {
				return false;
			}
		}

		return true;
	}

	bool operator!=(const StatementIdentifier& o) const {
		return !(*this == o);
	}

	size_t depth() const { return line_numbers.size(); }

	void to_string(std::ostream& o) const;
	std::string to_string() const;

	size_t get_last_line() const {
		return line_numbers[line_numbers.size()-1];
	}
};

struct StatementRange {
	using StatementIdentifier = engine::voila::StatementIdentifier;

	StatementIdentifier begin;
	StatementIdentifier end;


	StatementRange(const StatementIdentifier begin, const StatementIdentifier end)
	 : begin(begin), end(end)
	{
		debug_assert_valid();
	}

	StatementRange(const StatementRange& o) : begin(o.begin), end(o.end) {
		debug_assert_valid();
	}


	StatementRange& operator=(const StatementRange& o) {
		begin = o.begin;
		end = o.end;
		return *this;
	}

	bool operator==(const StatementRange& o) const {
		return o.begin == begin && o.end == end;
	}

	bool operator!=(const StatementRange& o) const { return !(*this == o); }

	bool overlaps_depth(const StatementRange& o, size_t curr_depth) const {
		if (curr_depth >= depth() || curr_depth >= o.depth()) {
			return true;
		}

		auto d = curr_depth;

		auto a_start = begin.line_numbers[d];
		auto a_end = end.line_numbers[d];

		auto b_start = o.begin.line_numbers[d];
		auto b_end = o.end.line_numbers[d];

		LOG_TRACE("d=%llu", d);
		LOG_TRACE("a [%lld, %lld)", a_start, a_end);
		LOG_TRACE("b [%lld, %lld)", b_start, b_end);

		// same prefix, continue
		if (a_start == b_start && a_end == b_end) {
			LOG_TRACE("cont");
			return overlaps_depth(o, curr_depth+1);
		}

		if (a_start >= b_start && a_start < b_end) {
			// a is in b
			return true;
		}

		if (b_start >= a_start && b_start < a_end) {
			// a is in b
			return true;
		}

		return false;
		bool r = a_start < b_end && b_start < a_end;
		LOG_TRACE("r=%d", r);
		return r;
	}

	bool overlaps(const StatementRange& o) const {
		LOG_TRACE("%s overlaps with %s?",
			to_string().c_str(), o.to_string().c_str());
		if (begin.operator_id != o.begin.operator_id) {
			return false;
		}

		return overlaps_depth(o, 0);
#if 0
		size_t min_depth = std::min(depth(), o.depth());

		for (size_t d=0; d<min_depth; d++) {
			auto x1 = begin.line_numbers[d];
			auto x2 = end.line_numbers[d]-1;

			auto y1 = o.begin.line_numbers[d];
			auto y2 = o.end.line_numbers[d]-1;

			LOG_WARN("d=%llu", d);
			LOG_WARN("x1 %lld, x2 %lld", x1, x2);
			LOG_WARN("y1 %lld, y2 %lld", y1, y2);

			if (x1 == y1 && y2 == x2) {
				LOG_WARN("cont");
				continue;
			}

			bool r = x1 <= y2 && y1 <= x2;
			LOG_WARN("r=%d", r);
			return r;
		}

		return true;
#endif
	}

	void assert_valid() const;

	void debug_assert_valid() const {
#ifdef IS_DEBUG
		assert_valid();
#endif
	}

	size_t depth() const { return begin.depth(); }

	int64_t num_statements() const {
		debug_assert_valid();

		size_t last = begin.line_numbers.size()-1;
		return (int64_t)end.line_numbers[last] - (int64_t)begin.line_numbers[last];
	}

	void to_string(std::ostream& o) const;
	std::string to_string() const;

	static size_t gowers_distance_count() {
		return 2;
	}

	double gowers_distance_diff(const StatementRange& o) const {
		double sum = 0;

		if (begin != o.begin) {
			sum += 1.0;
		}
		if (end != o.end) {
			sum += 1.0;
		}

		return sum;
	}
};

struct VoilaStatementTracker {
	VoilaStatementTracker(size_t operator_id)
	 : operator_id(operator_id)
	{
	}

	void enter() {
		stack.push_back(0);
	}

	void update(size_t line) {
		ASSERT(!stack.empty());
		stack[stack.size()-1] = line;

		curr_range.reset();
	}

	void leave() {
		stack.pop_back();
	}

	size_t depth() const { return stack.size(); }

	StatementIdentifier get_identifier() const {
		return StatementIdentifier(operator_id, stack);
	}

	std::shared_ptr<StatementRange> get_curr_range() {
		if (!curr_range) {
			auto id = get_identifier();
			curr_range = std::make_shared<StatementRange>(id, id);
		}
		return curr_range;
	}

private:
	const size_t operator_id;
	std::vector<size_t> stack;

	std::shared_ptr<StatementRange> curr_range;
};

} /* voila */
} /* engine */

namespace std {
template <>
struct hash<engine::voila::StatementIdentifier>
{
	std::size_t operator()(const engine::voila::StatementIdentifier& k) const
	{
		using std::size_t;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<uint64_t>()(k.operator_id));
		for (auto l : k.line_numbers) {
			combine(hash<uint64_t>()(l));
		}
		return seed;
	}
};

template <>
struct hash<engine::voila::StatementRange>
{
	std::size_t operator()(const engine::voila::StatementRange& k) const
	{
		using std::size_t;
		using engine::voila::StatementIdentifier;

		size_t seed = 13;

		auto combine = [&] (size_t val) {
			seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
		};

		combine(hash<StatementIdentifier>()(k.begin));
		combine(hash<StatementIdentifier>()(k.end));
		return seed;
	}
};
} /* std */
