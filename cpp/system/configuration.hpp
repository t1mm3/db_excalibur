#pragma once

#include <string>
#include <unordered_map>
#include <functional>
#include <shared_mutex>

struct Configuration {
	typedef std::function<bool(const std::string&)> ValidateFunction;

	enum Type {
		kString, kLong, kDouble, kBool
	};
private:
	mutable std::shared_mutex mutex;


	struct Item {
		ValidateFunction validate;

		std::string value;

		Type type;

		union {
			int64_t ival;
			double dval;
			bool bval;
		} cache;
	};
	std::unordered_map<std::string, Item> opts;

	Configuration* parent;

public:
	static bool valid_long(const std::string& value);
	static bool valid_double(const std::string& value);
	static bool valid_bool(const std::string& value);

	void add(const std::string& key, const Type& t,
		const std::string& default_value,
		const ValidateFunction& validate = nullptr);

	void set(const std::string& key, const std::string& value);

	bool get(std::string& out, const std::string& key) const {
		std::shared_lock lock(mutex);
		return get_nolock(out, key);
	}
	bool get_nolock(std::string& out, const std::string& key) const;


	bool get_bool_nolock(const std::string& key) const;
	int64_t get_long_nolock(const std::string& key) const;
	double get_double_nolock(const std::string& key) const;
	std::string get_string_nolock(const std::string& key) const;

	bool get_bool(const std::string& key) const {
		std::shared_lock lock(mutex);
		return get_bool_nolock(key);
	}

	int64_t get_long(const std::string& key) const {
		std::shared_lock lock(mutex);
		return get_long_nolock(key);
	}

	double get_double(const std::string& key) const {
		std::shared_lock lock(mutex);
		return get_double_nolock(key);
	}

	std::string get_string(const std::string& key) const {
		std::shared_lock lock(mutex);
		return get_string_nolock(key);
	}

	Configuration(Configuration* parent = nullptr);
	~Configuration();

	Configuration(const Configuration&) = delete;
	Configuration& operator=(const Configuration&) = delete;
};
