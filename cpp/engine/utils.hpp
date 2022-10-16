#pragma once

#include <vector>
#include <string>

struct StringUtils {
	static std::vector<std::string> split(const std::string& s, char delim);

	static std::string to_lower_case(std::string s);
};
