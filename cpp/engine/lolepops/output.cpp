#include "output.hpp"
#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/xvalue.hpp"
#include "engine/stream.hpp"

#include "system/build.hpp"
#include "system/system.hpp"
#include "system/scheduler.hpp"

#include <sstream>

using namespace engine;
using namespace lolepop;

NextResult
Output::next(NextContext& context)
{
	ASSERT(child);
	bool done;
	size_t num;


	if (!fetch_from_child(context)) {
		output = child->output;
		return NextResult::kEndOfStream;
	}
	output = child->output;

	if (!m_enabled) {
		return NextResult::kYield;
	}

	SCHEDULER_SCOPE(Output, context.current_task);

	ASSERT(output);

	ASSERT(!output->col_data.empty()
		&& "Doesn't make sense to run without columns");


	output->get_flow_meta(num, done);
	auto& predicate = output->predicate;
	selvector_t* selection_vector = nullptr;
	if (predicate) {
		ASSERT(predicate->tag == XValue::Tag::kPredicate);
		selection_vector = output->predicate->get_first_as<selvector_t>();
	}
	LOG_TRACE("collect num %llu done %d predicate %p predicate num %llu",
		num, done, predicate, predicate ? predicate->num : 0);

	ASSERT(num <= 1024);

	size_t num_cols = output->col_data.size();
	size_t num_rows = num;

	if (!m_widths.size() && num_cols > 0) {
		m_widths.reserve(num_cols);
		for (size_t col_id=0; col_id<num_cols; col_id++) {
			auto col = output->col_data[col_id];

			ASSERT(col);
			ASSERT(col->tag == XValue::Tag::kVector);
#if 0
			LOG_DEBUG("num %lld, done %d, type %s, first %p",
				col->num, col->done, col->get_data_type()->to_cstring(),
				col->first);
#endif
			m_widths.push_back(col->get_width());
		}

		m_first_ptrs.resize(num_cols);
		m_types.resize(num_cols);
	}

	ASSERT(num_cols == m_widths.size());
	ASSERT(num_cols == m_first_ptrs.size());

	for (size_t i=0; i<num_cols; i++) {
		auto type = output->col_data[i]->get_data_type();
		ASSERT(type && type->is_supported_output_type());

		LOG_TRACE("collect col_data %p first %p, type %s",
			output->col_data[i],
			output->col_data[i]->get_first(),
			type->to_cstring());
	}

	auto& result = stream.result;

	result->prepare_more(num_rows);

	for (size_t col_id=0; col_id<num_cols; col_id++) {
		auto col = output->col_data[col_id];
		auto type = col->get_data_type();
		m_first_ptrs[col_id] = static_cast<char*>(col->get_first());
		m_types[col_id] = type;
	}

	for (size_t row=0; row<num_rows; row++) {
		std::ostringstream out;

		size_t row_id = selection_vector ? selection_vector[row] : row;

		for (size_t col_id=0; col_id<num_cols; col_id++) {
			char* pdata = m_first_ptrs[col_id] + m_widths[col_id] * row_id;

			if (col_id > 0) {
				out << "|";
			}
			m_types[col_id]->print_to_stream(out, pdata);
		}

		LOG_TRACE("Output: %s", out.str().c_str());

		result->rows.emplace_back(out.str());
	}

#if 0
	if (done) {
		ASSERT(!num);
	} else {
		ASSERT(num);
	}
#endif
	return NextResult::kYield;
}

Output::Output(Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
	const std::shared_ptr<Lolepop>& child)
 : Lolepop("Output", nullptr, stream, mem_context, child, Lolepop::kNoResult),
	m_enabled(stream.query.config ? stream.query.config->enable_output() : true)
{
}

Output::~Output()
{

}