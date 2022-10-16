#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>

#include <ostream>
#include <chrono>

#include "system/profiling.hpp"

namespace engine {
struct StreamProf;
struct LolepopProf;

} /* enigne */

namespace engine::voila {
struct StatementRange;
} /* engine::voila */

namespace engine {
struct IProfData {
	virtual void to_json(std::ostream& o) {

	}
};

struct QueryStageProf : IProfData {
	QueryStageProf(size_t id, size_t max_parallel)
	 : id(id), max_parallel(max_parallel) {
		duration_us = 0;
	}

	void register_stream(const std::shared_ptr<StreamProf>& stream) {
		std::unique_lock<std::mutex> lock(mutex);

		streams.push_back(stream);
	}

	size_t get_id() const { return id; }

	void to_json(std::ostream& o) final;

	std::chrono::time_point<std::chrono::high_resolution_clock> clock_start;
	uint64_t duration_us;

private:
	const size_t id;
	const size_t max_parallel;

	std::vector<std::shared_ptr<StreamProf>> streams;
	std::mutex mutex;

	friend struct StreamProf;
};

struct StreamProf : IProfData {
	StreamProf(size_t parallel_id);

	size_t get_parallel_id() const { return parallel_id; }

	void to_json(std::ostream& o) final;

	enum Phase {
		kGenerateCode = 0,
		kPrepare = 1,
		kExecute = 2,
		kReoptimize = 3,
		kProfile = 4,

		kMoveResult = 5,
		kTriggerNextStage = 6,
		kDealloc = 7,
		kAlloc = 8,

		kResultRows = 9,
		kDynamicProfile = 10,

		kEND = 11
	};

	uint64_t sum_time_phase[Phase::kEND];

	std::shared_ptr<LolepopProf> root_lolepop;
private:
	const size_t parallel_id;
};

struct PrimitiveProfile : profiling::ProfileData {
#define EXPAND(F, ARG) \
	F(uint64_t, num_calls, 0, kNumCalls, ARG) \
	F(uint64_t, num_input_tuples, 0, kNumInputTuples, ARG) \
	F(uint64_t, num_output_tuples, 0, kNumOutputTuples, ARG) \
	F(uint64_t, sum_time, 0, kSumTime, ARG) \
	F(uint64_t, compilation_time_us, 0, kCompilationTimeUs, ARG) \
	F(uint64_t, binary_size_bytes, 0, kBinarySizeBytes, ARG)

#define DEF(TYPE, NAME, DEFAULT, IDENT, ARG) TYPE NAME = DEFAULT;

	EXPAND(DEF, _)

#undef DEF

	std::shared_ptr<engine::voila::StatementRange> statement_range;
	int64_t voila_func_tag = -1;

	PrimitiveProfile(const std::string& op_name);
	~PrimitiveProfile();

	void update(uint64_t calls, uint64_t num_in,
			uint64_t num_out, uint64_t time) {
		num_calls += calls;
		num_input_tuples += num_in;
		num_output_tuples += num_out;
		sum_time += time;
	}

private:
	void add_items() {
#define ADD(TYPE, NAME, DEFAULT, IDENT, ARG) \
	 	add_prof_item(\
	 		profiling::ProfileData::ProfIdent::IDENT, \
	 		profiling::ProfileData::ProfType::kNative_##TYPE, \
	 		&NAME);

		EXPAND(ADD, _)
#undef ADD
	}

#undef EXPAND
};

struct LolepopProf : profiling::ProfileData {
#define EXPAND(F, ARG) \
	F(uint64_t, num_calls, 0, kNumCalls, ARG) \
	F(uint64_t, num_input_tuples, 0, kNumInputTuples, ARG) \
	F(uint64_t, num_output_tuples, 0, kNumOutputTuples, ARG) \
	F(uint64_t, sum_time, 0, kSumTime, ARG) \

#define DEF(TYPE, NAME, DEFAULT, IDENT, ARG) TYPE NAME = DEFAULT;

	EXPAND(DEF, _)

#undef DEF

	LolepopProf(const std::string& op_name, const std::shared_ptr<LolepopProf>& child)
	 : profiling::ProfileData("Lolepop(" + op_name + ")"), child(child), op_name(op_name) {

#define ADD(TYPE, NAME, DEFAULT, IDENT, ARG) \
	 	add_prof_item(\
	 		profiling::ProfileData::ProfIdent::IDENT, \
	 		profiling::ProfileData::ProfType::kNative_##TYPE, \
	 		&NAME);

		EXPAND(ADD, _)
#undef ADD

	}

#undef EXPAND

	void to_json(std::ostream& s) override {
		profiling::ProfileData::to_json(s);

		if (!byte_code.empty()) {
			s << "\"ByteCode\" : \"" << byte_code << "\",\n";
		}
	}

	void update_on_next(uint64_t calls, uint64_t num_in,
			uint64_t num_out, uint64_t time) {
		num_calls += calls;
		num_input_tuples += num_in;
		num_output_tuples += num_out;
		sum_time += time;
	}

	std::string byte_code;
	std::vector<std::shared_ptr<PrimitiveProfile>> primitives;
	const std::shared_ptr<LolepopProf> child;
	const std::string op_name;
};


} /* enigne */