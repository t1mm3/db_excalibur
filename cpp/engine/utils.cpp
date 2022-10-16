#include "utils.hpp"

#include <algorithm>
#include <sstream>

std::vector<std::string>
StringUtils::split(const std::string& s, char delim) {
	// https://stackoverflow.com/a/20755262
	std::vector<std::string> elems;
	std::stringstream ss(s);
	std::string number;
	while (std::getline(ss, number, delim)) {
		elems.push_back(number);
	}
	return elems;
}

std::string
StringUtils::to_lower_case(std::string s)
{
	std::transform(s.begin(), s.end(), s.begin(),
    	[](unsigned char c){ return std::tolower(c); });

	return s;
}
