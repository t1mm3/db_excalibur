#include "metrics.hpp"

#include <sstream>

using namespace engine;
using namespace adaptive;

void
Metrics::to_string(std::ostream& o) const
{
	o << "cyc/tup=" << cyc_per_tup;
}

std::string
Metrics::to_string() const
{
	std::ostringstream ss;
	to_string(ss);
	return ss.str();
}