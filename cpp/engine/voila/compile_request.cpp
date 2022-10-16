#include "compile_request.hpp"

#include "system/system.hpp"
#include "engine/query.hpp"
#include "engine/types.hpp"
#include "engine/voila/voila.hpp"
#include "engine/voila/flavor.hpp"
#include "engine/voila/jit_prepare_pass.hpp"

#include "signature.hpp"

using namespace engine::voila;

#define ENABLE_SIGNATURE

CompileRequest::CompileRequest(excalibur::Context& sys_context,
	QueryConfig* config, voila::Context& voila_context)
 : sys_context(sys_context)
{
	if (config) {
		asynchronous_compilation = config->asynchronous_compilation();
		can_be_cached = config->enable_cache();
		enable_optimizations = config->enable_optimizations();
		verify = config->verify_code_fragments();
		paranoid = config->paranoid_mode();
	}

	// flavor = std::make_shared<FlavorSpec>(config);
	context = std::make_unique<voila::Context>(voila_context);
}

CompileRequest::~CompileRequest()
{

}


void
CompileRequest::canonicalize()
{
#ifdef ENABLE_SIGNATURE
	LOG_DEBUG("CompileRequest::canonicalize");
	debug_validate();

	// populate cache
	_get_operation_signature();


	// reorder
	auto reorder = [&] (auto& vec) {
		auto& cache = *signature_cache;
		std::sort(vec.begin(), vec.end(), [&] (auto& a, auto& b) {
			auto sign_a = cache.get(a.get());
			auto sign_b = cache.get(b.get());

			return sign_a < sign_b;
		});
	};

	ASSERT(signature_cache);

	reorder(sources);
	reorder(sinks);
	// ASSERT(structs.size() <= 1);
	LOG_DEBUG("TODO: canonicalize: structs?");
#else
	LOG_WARN("Todo: Fix Signature");
#endif
}

void
CompileRequest::update()
{
	cached_op_sign.clear();
	cached_full_sign.clear();
	signature_cache.reset();
}


bool
CompileRequest::add_value(const NodePtr& ptr, Type t)
{
	if (!ptr.get()) {
		ptr->dump();
		ASSERT(ptr.get());
	}

	int64_t tmp_index;
	if (contains_value(tmp_index, ptr, t)) {
		return false;
	}

	std::unordered_map<NodePtr, int64_t>* set = nullptr;
	std::vector<NodePtr>* vec = nullptr;

	switch (t) {
	case kSource:
		set = &source_set;
		vec = &sources;
		break;
	case kSink:
		set = &sink_set;
		vec = &sinks;
		break;
	case kStruct:
		set = &struct_set;
		vec = &structs;
		break;
	case kStatement:
		set = &statement_set;
		vec = &statements;
		break;
	case kExpression:
		set = &expression_set;
		vec = &expressions;
		break;
	default:
		ASSERT(false);
		break;
	}
	ASSERT(set && vec);

	set->insert({ ptr, (int64_t)vec->size() });
	vec->push_back(ptr);

	update();
	return true;
}

bool
CompileRequest::contains_value(int64_t& out_index, const NodePtr& ptr, Type t) const
{
	out_index = -1;

	const std::unordered_map<NodePtr, int64_t>* set = nullptr;

	switch (t) {
	case kSource:		set = &source_set; break;
	case kSink:			set = &sink_set; break;
	case kStruct:		set = &struct_set; break;
	case kStatement:	set = &statement_set; break;
	case kExpression:	set = &expression_set; break;
	default:
		ASSERT(false);
		break;
	}
	ASSERT(set);

	auto it = set->find(ptr);
	bool found = it != set->end();
	if (found) {
		ASSERT(it->second >= 0 && it->second <= (int64_t)set->size());
		out_index = it->second;
	}
	return found;
}

template<typename CONTEXT_T, typename SET_T>
static bool
__can_evaluate_speculatively(const std::shared_ptr<Expression>& p,
	const CONTEXT_T& context, const SET_T& source_set)
{
	auto it = source_set.find(p);
	if (it != source_set.end()) {
		return true;
	}

	if (p->predicate) {
		if (!__can_evaluate_speculatively<CONTEXT_T, SET_T>(p->predicate,
				context, source_set)) {
			return false;
		}
	}

	auto func = std::dynamic_pointer_cast<Function>(p);
	if (func) {
		for (auto arg : func->args) {
			if (!__can_evaluate_speculatively<CONTEXT_T, SET_T>(arg,
					context, source_set)) {
				return false;
			}
		}
	}
	return p->can_evaluate_speculatively(context);
}

template<typename CONTEXT_T, typename SET_T>
static bool
_can_evaluate_speculatively(const NodePtr& p,
	const CONTEXT_T& context, const SET_T& source_set)
{
	auto expr = std::dynamic_pointer_cast<Expression>(p);

	if (!expr) {
		LOG_DEBUG("_can_evaluate_speculatively: Not an expression i.e. no");
		return false;
	}

	return __can_evaluate_speculatively<CONTEXT_T, SET_T>(
		expr, context, source_set);
}

bool
CompileRequest::can_evaluate_speculatively() const
{
	bool can_eval = true;

	// It's not fully clear whether we can fully eval statements
	// For the VOILA code, we generate typically not
	// TODO: but it might be possible for special Assigns when it just assigns a value once
	LOG_DEBUG("TODO: Add case for Assign, that assigns value only once");
	if (!statement_set.empty()) {
		return false;
	}

	for (auto& keyval : sink_set) {
		auto& sink = keyval.first;
		can_eval &= _can_evaluate_speculatively(sink, *context, source_set);

		if (!can_eval) {
			return false;
		}
	}

	return can_eval;
}

struct SignatureProfCollector {
	SignatureProfCollector(Query* query) {
		prof = query ? query->profile.get() : nullptr;
		if (prof) {
			start_cycles = profiling::rdtsc();
		}
	}

	~SignatureProfCollector() {
		if (prof) {
			prof->total_code_cache_gen_signature_cy += profiling::rdtsc() - start_cycles;
		}
	}

	uint64_t start_cycles = 0;
	QueryProfile* prof;
};

#ifndef ENABLE_SIGNATURE
static std::atomic<int64_t> g_counter;
#endif

std::string
CompileRequest::_get_operation_signature()
{
	if (!signature_cache) {
		signature_cache = std::make_unique<SignatureCache>();
	}
#ifdef ENABLE_SIGNATURE
	SignatureGen gen(*this);
	gen();
	return gen.get();
#else
	LOG_WARN("Todo: Fix Signature");
	int64_t id = g_counter.fetch_add(1);
	return std::to_string(id);
#endif
}

const std::string&
CompileRequest::get_full_signature(Query* query)
{
	if (cached_full_sign.empty()) {
		SignatureProfCollector profile_collector(query);

		std::ostringstream s;
		s << _get_operation_signature();

		if (flavor) {
			s << "_flavor_";
			flavor->generate_signature(s);
		}

		cached_full_sign = s.str();
	}

	ASSERT(!cached_full_sign.empty());
	return cached_full_sign;
}

const std::string&
CompileRequest::get_operation_signature(Query* query)
{
	if (cached_op_sign.empty()) {
		SignatureProfCollector profile_collector(query);
		cached_op_sign = _get_operation_signature();
	}

	ASSERT(!cached_op_sign.empty());
	return cached_op_sign;
}

void
CompileRequest::dump() const
{
	if (LOG_WILL_LOG(DEBUG)) {
		LOG_DEBUG("<Request>");
		LOG_DEBUG("== Sources == ");
		for (auto& n : sources) {
			n->dump();

			auto it = context->infos.find(n.get());
			ASSERT(it != context->infos.end());
			ASSERT(it->second.size() == 1 && it->second[0].type);

			LOG_DEBUG("Source %p, TypeTag = %ld",
				n.get(), it->second[0].type->get_tag());
		}


		LOG_DEBUG("== Sinks == ");
		for (auto& n : sinks) {
			n->dump();

			auto it = context->infos.find(n.get());
			ASSERT(it != context->infos.end());
			ASSERT(it->second.size() == 1 && it->second[0].type);

			LOG_DEBUG("Sink %p, TypeTag = %ld",
				n.get(), it->second[0].type->get_tag());
		}

		LOG_DEBUG("== Statements == ");
		for (auto& n : statements) {
			n->dump();

			LOG_DEBUG("Statement %p", n.get());
		}

		LOG_DEBUG("== Expressions == ");
		for (auto& n : expressions) {
			n->dump();

			LOG_DEBUG("Expression %p", n.get());
		}
		LOG_DEBUG("</Request>");
	}
}

#define VALIDATE(x) \
	if (use_assert) { \
		ASSERT(x); \
	} else { \
		if (!(x)) { \
			LOG_DEBUG("CompileRequest: validation failed at '%s'@%d", \
				#x, (int)__LINE__); \
			return false; \
		} \
	}

template<typename T>
static bool
must_have_type(const T& context, Node* ptr, bool use_assert)
{
	auto type_it = context->infos.find(ptr);
	VALIDATE(type_it != context->infos.end());
	VALIDATE(type_it->second.size() == 1);
	VALIDATE(type_it->second[0].type);

	return true;
}

bool
CompileRequest::validate_check(bool use_assert) const
{
	VALIDATE(source_set.size() == sources.size());
	VALIDATE(sink_set.size() == sinks.size());
	VALIDATE(struct_set.size() == structs.size());

	VALIDATE(sink_set.size() > 0 || statement_set.size() > 0);

	for (auto& keyval : sink_set) {
		auto& sink = keyval.first;
		auto constant = std::dynamic_pointer_cast<Constant>(sink);
		VALIDATE(!constant);

		VALIDATE(source_set.find(sink) == source_set.end() &&
			"Source cannot be sink");

		if (!must_have_type(context, sink.get(), use_assert)) {
			return false;
		}
	}

	for (auto& keyval : source_set) {
		auto& source = keyval.first;

		VALIDATE(sink_set.find(source) == sink_set.end() &&
			"Sink cannot be source");

		if (!must_have_type(context, source.get(), use_assert)) {
			return false;
		}
	}
	return true;
}

#undef VALIDATE

void
CompileRequest::set_analysis_from_fuse_group(const FuseGroup& group)
{
	ASSERT(this == group.request.get());

	complexity = group.nodes.size();
	sum_bits = 0;

	const auto& voila_context = *context;


	for (auto& n : group.nodes) {
		auto it_info = voila_context.infos.find(n);

		engine::Type* type;
		if (it_info != voila_context.infos.end() && it_info->second.size() == 1) {
			type = it_info->second[0].type;
		} else {
			type = nullptr;
		}

		if (type) {
			sum_bits += type->num_bits();
		} else {
			sum_bits += engine::TypeSystem::kMaxTypeWidth*8;
		}
	}
}