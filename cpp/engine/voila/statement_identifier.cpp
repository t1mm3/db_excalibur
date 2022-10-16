#include "statement_identifier.hpp"

#include "system/system.hpp"

#include <sstream>

using namespace engine::voila;

void
StatementIdentifier::to_string(std::ostream& o) const
{
	o << "op=" << operator_id << ", [";
	bool first = true;
	for (auto& l : line_numbers) {
		if (!first) {
			o << ", ";
		}
		o << l;
		first = false;
	}
	o << "]";
}

std::string
StatementIdentifier::to_string() const
{
	std::stringstream ss;
	to_string(ss);
	return ss.str();
}



void
StatementRange::assert_valid() const
{
	ASSERT(begin.operator_id == end.operator_id && "same operator");
	ASSERT(begin.depth() == end.depth() && "same depth");

	size_t n = begin.line_numbers.size();
	if (n > 1) {
		for (size_t i=0; i<n-1; i++) {
			ASSERT(begin.line_numbers[i] == end.line_numbers[i] && "same prefix/path");
		}
	}
}

void
StatementRange::to_string(std::ostream& o) const
{
	o << "begin=(";
	begin.to_string(o);
	o << "), end=(";
	end.to_string(o);
	o << ")";
}

std::string
StatementRange::to_string() const
{
	std::stringstream ss;
	to_string(ss);
	return ss.str();
}