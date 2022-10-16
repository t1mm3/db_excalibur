#include "flavor.hpp"
#include "engine/query.hpp"

#include <sstream>

using namespace engine::voila;

static int
spec_parse_bool(const std::string& t)
{
	if (t == "1" || !strcasecmp("true", t.c_str()) ||
			!strcasecmp("yes", t.c_str())) {
		return true;
	} else if (t == "0" || !strcasecmp("false", t.c_str()) ||
			!strcasecmp("no", t.c_str())) {
		return false;
	} else {
		ASSERT(false);
		return false;
	}
}

static int
spec_parse_int(const std::string& t)
{
	return std::stoi(t);
}

static std::vector<std::string>
split_strings(const std::string& str, char delimeter)
{
	std::stringstream ss(str);
	std::string item;
	std::vector<std::string> result;
	while (std::getline(ss, item, delimeter)) {
		result.push_back(item);
	}
	return result;
}

FlavorSpec::FlavorSpec(const QueryConfig* config)
{
	if (config) {
		*this = from_string(config->default_flavor());
	}
}

FlavorSpec::~FlavorSpec()
{
}

void
FlavorSpec::to_string(std::ostream& o, bool short_name) const
{
	bool first = true;

#define DECL(_1, NAME, CNAME, _2, _3, SHORT_NAME, _4, _5, _6) \
	if (!first) { \
		o << ","; \
	} \
	if (short_name) { \
		o << #SHORT_NAME; \
	} else { \
		o << #NAME; \
	} \
	o << "=" << CNAME; \
	first = false;
FLAVOR_EXPAND(DECL, error)
#undef DECL
}

void
FlavorSpec::generate_signature(std::ostream& o) const
{
	to_string(o, true);
}

std::string
FlavorSpec::generate_signature() const
{
	std::stringstream ss;
	generate_signature(ss);
	return ss.str();;
}

std::string
FlavorSpec::to_string() const
{
	std::stringstream ss;
	to_string(ss, false);
	return ss.str();
}

FlavorSpec
FlavorSpec::from_string(const std::string& spec)
{
	FlavorSpec result(nullptr);
	if (spec.empty()) {
		return result;
	}

	auto kv_pairs = split_strings(spec, ',');
	for (auto& pair : kv_pairs) {
		auto kv = split_strings(pair, '=');

		ASSERT(kv.size() == 2);
		const auto& key = kv[0];
		const auto& val = kv[1];

		#define DECL(_1, NAME, CNAME, TYPE, _2, SHORT_NAME, _3, _4, _5) \
		if (!strcasecmp(#NAME, key.c_str()) || \
				!strcasecmp(#CNAME, key.c_str()) || \
				!strcasecmp(#SHORT_NAME, key.c_str())) {\
			result.CNAME = spec_parse_##TYPE(val); \
			continue; \
		}
		FLAVOR_EXPAND(DECL, error)
		#undef DECL

		LOG_ERROR("Invalid key '%s'", key.c_str());
		ASSERT(false && "Invald key");
	}
	return result;
}