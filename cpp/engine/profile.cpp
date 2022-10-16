#include "profile.hpp"

#include "engine/voila/statement_identifier.hpp"

#include <cstring>

using namespace engine;
using namespace voila;

void
QueryStageProf::to_json(std::ostream& o)
{
	o
		<< "\"Id\": " << id <<",\n"
		<< "\"MaxParallel\": " << max_parallel <<",\n"
		<< "\"RuntimeMs\": " << ((double)duration_us / 1000.0) <<",\n"
		<< "\"Streams\": {\n";
	for (auto& stream : streams) {
		o << "\"" << stream->get_parallel_id() << "\": {\n";
		stream->to_json(o);
		o << "},\n";
	}
	o << "\"__Done\" : \"\"\n";
	o << "}\n";
}

StreamProf::StreamProf(size_t parallel_id)
 : parallel_id(parallel_id)
{
	memset(&sum_time_phase[0], 0, sizeof(sum_time_phase));
}

void
StreamProf::to_json(std::ostream& o)
{
	o
		<< "\"Phases\": {\n"
		<< "\"Alloc\": " << sum_time_phase[Phase::kAlloc] << ",\n"
		<< "\"GenerateCode\": " << sum_time_phase[Phase::kGenerateCode] << ",\n"
		<< "\"Prepare\": " << sum_time_phase[Phase::kPrepare] << ",\n"
		<< "\"Execute\": "
			<< (sum_time_phase[Phase::kExecute] - sum_time_phase[Phase::kDynamicProfile])
			<< ",\n"
		<< "\"DynamicProfile\": " << sum_time_phase[Phase::kDynamicProfile] << ",\n"
		<< "\"Reoptimize\": " << sum_time_phase[Phase::kReoptimize] << ",\n"
		<< "\"StaticProfile\": " << sum_time_phase[Phase::kProfile] << ",\n"
		<< "\"MoveResult\": " << sum_time_phase[Phase::kMoveResult] << ",\n"
		<< "\"TriggerNextStage\": " << sum_time_phase[Phase::kTriggerNextStage] << ",\n"
		<< "\"Dealloc\": " << sum_time_phase[Phase::kDealloc] << "\n"
		<< "},\n";

	o << "\"Lolepops\": {\n";

	size_t i=0;
	std::shared_ptr<LolepopProf> curr_op = root_lolepop;
	while (curr_op) {
		o << "\"" << i << "\": {\n";
		o << "\"Name\": " << "\"" << curr_op->op_name << "\",\n";
		curr_op->to_json(o);

		o << "\"NumPrims\": " << curr_op->primitives.size() << ",\n";
		if (curr_op->primitives.size() > 0) {
			o << "\"Prims\": {\n";
			for (auto& prim : curr_op->primitives) {
				o << "\"" << prim->get_name() << "\": {\n";
				prim->to_json(o);
				o << "\"__Done\" : \"\"\n";
				o << "},\n";
			}
			o << "\"__Done\" : \"\"\n";
			o << "},\n";
		}

		o << "\"__Done\" : \"\"\n";
		o << "},\n";

		curr_op = curr_op->child;
		i++;
	}
	o << "\"__Done\" : \"\"\n";
	o << "}\n";
}


PrimitiveProfile::PrimitiveProfile(const std::string& op_name)
 : profiling::ProfileData(op_name)
{
	add_items();
}

PrimitiveProfile::~PrimitiveProfile()
{

}