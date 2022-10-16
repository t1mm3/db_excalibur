#include "third_party/cxxopts.hpp"
#include <iostream>
#include <fstream>
#include <string.h>
#include <memory>
#include <random>

using namespace std;

static string g_newline("\n");
static string g_seperator("|");
static string g_mode("");
static unique_ptr<mt19937> g_gen;

static vector<string>
split(string s, string delimiter)
{
	// from https://stackoverflow.com/a/46931770
	size_t pos_start = 0, pos_end, delim_len = delimiter.length();
	string token;
	vector<string> res;

	while ((pos_end = s.find (delimiter, pos_start)) != string::npos) {
		token = s.substr (pos_start, pos_end - pos_start);
		pos_start = pos_end + delim_len;
		res.push_back (token);
	}

	res.push_back (s.substr (pos_start));
	return res;
}

void
join_experiment_gen_probe(string path,
	const vector<pair<size_t, size_t>>& vec_tres_group)
{
	ofstream f(path.c_str(), ios::out | ios::trunc);
	if (!f) {
		cerr << "join_experiment_gen_probe: Cannot open file '" << path << "'" << endl;
		return;
	}

	cerr << "join_experiment_gen_probe: path='" << path << "'" << endl;

	size_t i = 0;

	for (auto& p : vec_tres_group) {
		auto threshold = p.first;
		auto group_size = p.second;

		std::vector<std::string> v;

		if (threshold > i) {
			v.reserve(threshold - i);
		}

		if (group_size > 0) {
			for (; i<threshold; i++) {
				std::ostringstream ss;
				ss
					<< (i % group_size) << g_seperator
					<< i << g_seperator;

				v.emplace_back(ss.str());
			}
		} else {
			for (; i<threshold; i++) {
				std::ostringstream ss;
				ss
					<< i << g_seperator
					<< i << g_seperator;

				v.emplace_back(ss.str());
			}
		}

		std::shuffle(v.begin(), v.end(), *g_gen);
		for (auto& line : v) {
			f << line << g_newline;
		}
	}

	f.close();
}

void
join_experiment_gen_build(string path, size_t num_rows, size_t num_cols,
	long long offset)
{
	ofstream f(path.c_str(), ios::out | ios::trunc);
	if (!f) {
		cerr << "join_experiment_gen_build: Cannot open file '" << path << "'" << endl;
		return;
	}

	cerr << "join_experiment_gen_build: path='" << path << "' num_rows=" << num_rows
		<< " num_cols=" << num_cols << " offset=" << offset << endl;

	std::vector<std::string> v;
	v.reserve(num_rows);

	for (size_t i=0; i<num_rows; i++) {
		std::ostringstream ss;
		for (size_t k=0; k<num_cols; k++) {
			if (k) {
				ss << g_seperator;
			}

			ss << (i+offset);
		}

		ss << g_seperator;

		v.emplace_back(ss.str());
	}

	std::shuffle(v.begin(), v.end(), *g_gen);

	for (auto& line : v) {
		f << line << g_newline;
	}

	f.close();
}

int
main(int argc, char** argv)
{
	cxxopts::Options options("Generates join experiment data", "");

	options.add_options()
		("mode", "Mode. One of 'probe', 'build'",
			cxxopts::value<string>()->default_value(""))
		("config", "Configuration string", cxxopts::value<string>()->default_value(""))
		("o", "Output path", cxxopts::value<string>()->default_value(""))
		("seed", "Random seed", cxxopts::value<long long>()->default_value("13"))
		("h,help", "Print usage")
		;

	auto args = options.parse(argc, argv);

	auto help_and_exit = [&] () {
		cout << options.help() << endl;
		exit(0);
	};

	if (args.count("help")) {
		help_and_exit();
	}

	seed_seq seed { args["seed"].as<long long>() };
	g_gen = make_unique<mt19937>(seed);

	g_mode = args["mode"].as<string>();
	auto config = args["config"].as<string>();
	auto path = args["o"].as<string>();

	try {
		if (!strcasecmp(g_mode.c_str(), "probe")) {
			std::vector<pair<size_t, size_t>> arguments;
				for (auto& tuple : split(config, ";")) {
					auto values = split(tuple, ",");
					if (values.size() != 2) {
						throw invalid_argument("invalid format");
					}

					auto v1 = stoll(values[0]);
					auto v2 = stoll(values[1]);

					if (v1 <= 0 || v2 <= 0) {
						throw invalid_argument("values must be positive integers");
					}

					arguments.push_back(make_pair<size_t, size_t>(v1, v2));
				}
			join_experiment_gen_probe(path, arguments);
		} else if (!strcasecmp(g_mode.c_str(), "build")) {
			auto values = split(config, ";");
			if (values.size() != 2 && values.size() != 3) {
				throw invalid_argument("invalid format");
			}

			auto rows = stoll(values[0]);
			auto cols = stoll(values[1]);
			long long offset = 0;
			if (values.size() == 3) {
				offset = stoll(values[2]);
			}

			if (rows <= 0 || cols <= 0 || offset < 0) {
				throw invalid_argument("values must be positive integers");
			}

			join_experiment_gen_build(path, rows, cols, offset);
		} else {
			cerr << "Invalid mode '" << g_mode << "'" << endl;
			help_and_exit();
		}
	} catch (invalid_argument& arg) {
		cerr << "Invalid config '" << config << "'. " << arg.what() << endl;
		help_and_exit();
	};

	return 0;
}
