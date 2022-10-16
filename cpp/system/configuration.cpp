#include "configuration.hpp"
#include "system.hpp"
#include <mutex>

struct ConfigurationException : Exception {
	ConfigurationException(const std::string& msg) : Exception(msg) {}
};

struct InvalidConfigValue : ConfigurationException {
	InvalidConfigValue(const std::string& msg) : ConfigurationException(msg) {}
};

static bool
parse_bool(bool& out, const std::string& value)
{
	bool is_true = !value.compare("1") || !value.compare("on") || !value.compare("true");
	bool is_false = !value.compare("0")|| !value.compare("off")|| !value.compare("false");

	out = is_true;

	return is_true != is_false;
}

bool
Configuration::valid_long(const std::string& value)
{
	try {
		std::stoll(value);
		return true;
	} catch (...) {
		return false;
	}
}

bool
Configuration::valid_double(const std::string& value)
{
	try {
		std::stod(value);
		return true;
	} catch (...) {
		return false;
	}
}

bool
Configuration::valid_bool(const std::string& value)
{
	bool b;
	return parse_bool(b, value);
}

static bool
valid_string(const std::string& value)
{
	return true;
}

void
Configuration::set(const std::string& key, const std::string& value)
{
	std::unique_lock lock(mutex);

	auto it = opts.find(key);
	if (it == opts.end()) {
		THROW(InvalidConfigValue, "Invalid key");
	}

	auto& item = it->second;

	bool valid = item.validate(value);
	if (!valid) {
		THROW(InvalidConfigValue, "Invalid value");
	}

	item.value = value;

	try {
		switch (item.type) {
		case Type::kLong:
			item.cache.ival = std::stoll(value);
			break;
		case Type::kDouble:
			item.cache.dval = std::stod(value);
			break;
		case Type::kBool:
			if (!parse_bool(item.cache.bval, value)) {
				throw InvalidConfigValue("");
			}
			break;
		default:
			break;
		} 
	} catch (...) {
		THROW(InvalidConfigValue, "Couldn't cache value");
	}
}

bool
Configuration::get_nolock(std::string& out, const std::string& key) const
{
	out.clear();
	auto it = opts.find(key);
	if (it == opts.end()) {
		return false;
	}
	out = it->second.value;
	return true;
}

bool
Configuration::get_bool_nolock(const std::string& key) const
{
	auto it = opts.find(key);
	if (it == opts.end()) {
		THROW(InvalidConfigValue, "Invalid key");
	}

	if (it->second.type != Type::kBool) {
		THROW(InvalidConfigValue, "Types do not match");
	}

	return it->second.cache.bval;
}

int64_t
Configuration::get_long_nolock(const std::string& key) const
{
	auto it = opts.find(key);
	if (it == opts.end()) {
		if (parent) {
			parent->get_long(key);
		} else {
			THROW(InvalidConfigValue, "Invalid key");
		}
	}

	if (it->second.type != Type::kLong) {
		THROW(InvalidConfigValue, "Types do not match");
	}

	return it->second.cache.ival;
}

double
Configuration::get_double_nolock(const std::string& key) const
{
	auto it = opts.find(key);
	if (it == opts.end()) {
		if (parent) {
			parent->get_double(key);
		} else {
			THROW(InvalidConfigValue, "Invalid key");
		}
	}

	if (it->second.type != Type::kDouble) {
		THROW(InvalidConfigValue, "Types do not match");
	}

	return it->second.cache.dval;
}

std::string
Configuration::get_string_nolock(const std::string& key) const
{
	auto it = opts.find(key);
	if (it == opts.end()) {
		if (parent) {
			parent->get_string(key);
		} else {
			THROW(InvalidConfigValue, "Invalid key");
		}
	}

	if (it->second.type != Type::kString) {
		THROW(InvalidConfigValue, "Types do not match");
	}

	return it->second.value;
}

void
Configuration::add(const std::string& key, const Configuration::Type& t,
	const std::string& default_value,
	const Configuration::ValidateFunction& validate)
{
	Configuration::ValidateFunction validator(validate);

	if (!validator) {
		switch (t) {
		case kLong: validator = valid_long; break;
		case kDouble: validator = valid_double; break;
		case kBool: validator = valid_bool; break;
		case kString: validator = valid_string; break;
		default:
			ASSERT(false);
			break;
		}
	}

	auto it = opts.find(key);
	ASSERT(it == opts.end());

	Item item;
	item.validate = std::move(validator);
	item.type = t;

	opts.insert({ key, std::move(item) });

	set(key, default_value);
}

Configuration::Configuration(Configuration* parent)
 : parent(parent)
{

}

Configuration::~Configuration()
{

}
