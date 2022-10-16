#include "type_stream.hpp"
#include "stream.hpp"
#include "query.hpp"
#include "system/system.hpp"
#include "voila/voila.hpp"
#include "lolepops/voila_lolepop.hpp"
#include "engine/types.hpp"
#include "engine/catalog.hpp"
#include "engine/voila/jit.hpp"

using namespace engine;

using NodeInfo = TypeStream::NodeInfo;
using NodeInfos = TypeStream::NodeInfos;
using NodeInfoMap = TypeStream::NodeInfoMap;

static NodeInfo info_predicate()
{
	return NodeInfo(TypeSystem::new_predicate());
}

static NodeInfo info_integer(double min, double max)
{
	return NodeInfo(TypeSystem::new_integer(min, max), min, max);
}

static NodeInfo info_hash()
{
	return NodeInfo(TypeSystem::new_hash());
}

static NodeInfo info_bucket()
{
	return NodeInfo(TypeSystem::new_bucket());
}

static NodeInfo info_position()
{
	return NodeInfo(TypeSystem::new_position());
}

static NodeInfo info_string()
{
	return NodeInfo(TypeSystem::new_string());
}

static NodeInfo info_empty()
{
	return NodeInfo(TypeSystem::new_empty());
}

#define TRACE(...) LOG_TRACE(__VA_ARGS__)

struct TypeVoila : voila::IExpressionVisitor, voila::IStatementVisitor {
	NodeInfos result;
	NodeInfoMap& infos;
	TypeStream& type_stream;
	const double max_tuples;
	const bool enable_reaggregation;

	template<typename R, typename T>
	static NodeInfo
	get_single_info(const R& infos, T* ptr)
	{
		auto it = infos.find(ptr);
		ASSERT(ptr && it != infos.end());
		const auto& vec = it->second;
		ASSERT(vec.size() == 1);

		ASSERT(vec[0].type);

		return vec[0];
	}

	template<typename R, typename T>
	static NodeInfo
	get_single_info(const R& infos, const T& ptr)
	{
		ASSERT(ptr);
		return get_single_info(infos, ptr.get());
	}


	TypeVoila(TypeStream& type_stream, double max_tuples, bool enable_reaggregation)
	 : infos(type_stream.infos), type_stream(type_stream), max_tuples(max_tuples),
		enable_reaggregation(enable_reaggregation)
	{

	}

	void on_statement(const std::shared_ptr<voila::Assign>& b) final {
		TRACE("on_statement(assign, var '%s', global %d)",
			b->var->dbg_name.c_str(), b->var->global);
		auto& variables = type_stream.variables;

		auto it = infos.find(b->value.get());
		ASSERT(it != infos.end() && "Input must be typed");

		variables[b->var.get()] = it->second;
		infos[b->var.get()] = it->second;

		result = it->second;
	}

	void on_statement(const std::shared_ptr<voila::Block>& b) final {
		// does not have type
	}

	void on_statement(const std::shared_ptr<voila::Emit>& b) final {
		TRACE("EMIT:  %p", b->value.get());
		auto it = infos.find(b->value.get());
		ASSERT(it != infos.end() && "Must be typed");

		for (auto& pres : it->second) {
			ASSERT(pres.type);
		}
		type_stream.lolepops[type_stream.current_op.get()] = it->second;

		// does not have type
	}

	void on_statement(const std::shared_ptr<voila::Effect>& b) final {}
	void on_statement(const std::shared_ptr<voila::Loop>& b) final {}
	void on_statement(const std::shared_ptr<voila::EndOfFlow>& b) final {}
	void on_statement(const std::shared_ptr<voila::Comment>& b) final {}
	void on_statement(const std::shared_ptr<voila::MetaStatement>& b) final {}

	void on_expression(const std::shared_ptr<voila::Input>& e) final {
		auto prev = type_stream.previous_op.get();
		ASSERT(prev && "Must have previous operator");

		auto it = type_stream.lolepops.find(prev);
		ASSERT(it != type_stream.lolepops.end() && "Previous operator's output must be typed");
		result = it->second;

		for (auto& pres : result) {
			ASSERT(pres.type);
		}
	}

	void on_expression(const std::shared_ptr<voila::InputPredicate>& e) final {
		result.push_back(info_predicate());
	}

	void on_expression(const std::shared_ptr<voila::ReadVariable>& e) final {
		TRACE("on_expression(var '%s', global %d)",
			e->var->dbg_name.c_str(), e->var->global);
		const auto& variables = type_stream.variables;

		const auto it = variables.find(e->var.get());
		ASSERT(it != variables.end() && "Variable must be typed");

		result = it->second;
	}

	void on_expression(const std::shared_ptr<voila::Function>& e) final {
		using Tag = voila::Function::Tag;

		TRACE("on_expression(fun %s)", e->tag2cstring(e->tag));

		auto type_struct_col_with_info = [&] (auto& col, const auto& info) {
			auto new_info = std::make_unique<NodeInfo>(info);

			auto info2str = [] (auto& info) -> std::string{
				std::ostringstream ss;

				if (info) {
					if (info->has_minmax) {
						ss << "[" << info->min << ", " << info->max <<"]";
					} else {
						ss << "no MinMax";
					}

					ss << " -> type=" << info->type->to_string();
				} else {
					ss << "None";
				}

				return ss.str();
			};

			LOG_DEBUG("update column %p(%s) from old=%p (%s) to new=%p (%s)",
				col.get(), col->name.c_str(),
				col->node_info.get(), info2str(col->node_info).c_str(),
				new_info.get(), info2str(new_info).c_str());

			col->node_info = std::move(new_info);
		};

		auto type_struct_col = [&] (auto& col, auto& arg) {
			auto info = get_single_info(infos, arg);

			return type_struct_col_with_info(col, info);
		};

		switch (e->tag) {
		case Tag::kSelTrue:
		case Tag::kSelFalse:
		case Tag::kSelNum:
		case Tag::kSelUnion:
			result.emplace_back(info_predicate());
			break;

		case Tag::kBucketLookup:
		case Tag::kBucketNext:
		case Tag::kBucketInsert:
			result.emplace_back(info_bucket());
			break;

		case Tag::kBucketGather:
			{
				auto& col = e->data_structure_column;
				std::unique_lock lock(col->mutex);

				ASSERT(e->args.size() == 1);
				ASSERT(e->data_structure && col && col->node_info);
				ASSERT(col->node_info->type->is_supported_output_type());

				result.push_back(*col->node_info);
			}
			break;

		case Tag::kBucketScatter:
			{
				auto& col = e->data_structure_column;
				std::unique_lock lock(col->mutex);

				ASSERT(e->data_structure && col);
				ASSERT(e->args.size() == 2);
				type_struct_col(col, e->args[1]);
				result.emplace_back(info_empty());
			}
			break;

		case Tag::kWritePos:
		case Tag::kReadPos:
			result.emplace_back(info_position());
			break;

		case Tag::kWriteCol:
			{
				auto& col = e->data_structure_column;
				std::unique_lock lock(col->mutex);

				ASSERT(e->data_structure && col);
				ASSERT(e->args.size() == 2);
				type_struct_col(col, e->args[1]);
				result.emplace_back(info_empty());
			}
			break;

		case Tag::kLogAnd:
		case Tag::kLogOr:
		case Tag::kLogNot:
		case Tag::kCmpEq:
		case Tag::kCmpLt:
		case Tag::kCmpGt:
		case Tag::kCmpNe:
		case Tag::kCmpLe:
		case Tag::kCmpGe:
		case Tag::kContains:
		case Tag::kBucketCheck:
		case Tag::kBloomFilterLookup1:
		case Tag::kBloomFilterLookup2:
		case Tag::kBetweenBothIncl:
		case Tag::kBetweenUpperExcl:
		case Tag::kBetweenLowerExcl:
		case Tag::kBetweenBothExcl:
			result.emplace_back(info_integer(0, 1));
			break;

		case Tag::kHash:
		case Tag::kRehash:
			result.emplace_back(info_hash());
			break;

		case Tag::kExtractYear:
			// should fit 16-bit
			result.emplace_back(info_integer(0, 3000));
			break;

		case Tag::kAritAdd:
		case Tag::kAritSub:
		case Tag::kAritMul:
			{
				ASSERT(e->args.size() == 2);
				auto a = get_single_info(infos, e->args[0]);
				auto b = get_single_info(infos, e->args[1]);
				ASSERT(a.type && b.type);
				ASSERT(a.type->is_integer() && b.type->is_integer());
				ASSERT(a.has_minmax && b.has_minmax);

				ASSERT(a.min <= a.max);
				ASSERT(b.min <= b.max);

				double min;
				double max;

				switch (e->tag) {
				case Tag::kAritAdd:
					min = a.min + b.min;
					max = a.max + b.max;
					break;
				case Tag::kAritSub:
					min = a.min - b.max;
					max = a.max - b.min;

					if (min > max) {
						std::swap(min, max);
					}
					break;
				case Tag::kAritMul:
					min = a.min * b.min;
					max = a.max * b.max;
					break;
				default:
					min = .0;
					max = .0;
					ASSERT(false);
					break;
				}

				ASSERT(min <= max);
				result.emplace_back(info_integer(min, max));
			}
			break;

		case Tag::kReadCol:
			{
				auto& col = e->data_structure_column;
				std::unique_lock lock(col->mutex);

				ASSERT(col && col->node_info);

				result.push_back({ *col->node_info });
				break;
			}

		case Tag::kBucketAggrSum:
		case Tag::kBucketAggrCount:
		case Tag::kBucketAggrMin:
		case Tag::kBucketAggrMax:

		case Tag::kGlobalAggrSum:
		case Tag::kGlobalAggrCount:
		case Tag::kGlobalAggrMin:
		case Tag::kGlobalAggrMax:
			{
				NodeInfo info;

				bool is_count = e->tag == Tag::kBucketAggrCount || e->tag == Tag::kGlobalAggrCount;
				bool is_sum = e->tag == Tag::kBucketAggrSum || e->tag == Tag::kGlobalAggrSum;
				bool is_reaggregation = enable_reaggregation && e->is_reaggregation;

				if (is_count) {
					ASSERT(e->args.size() == 1);
					info = info_integer(0, max_tuples);
				} else {
					ASSERT(e->args.size() == 2);
					auto b = get_single_info(infos, e->args[1]);
					ASSERT(b.type && b.type->is_integer() && b.has_minmax);

					double mul = 1.0;
					if (is_sum && !is_reaggregation) {
						mul = max_tuples;
					}

					double lo = b.min * mul;
					double hi = b.max * mul;
					info = info_integer(std::min(lo, b.min), std::max(hi, b.max));
				}

				auto& col = e->data_structure_column;
				std::unique_lock lock(col->mutex);

				ASSERT(e->data_structure && col);
				
				type_struct_col_with_info(col, info);

				result.emplace_back(info_empty());
			}
			break;

		case Tag::kTuple:
			result.reserve(e->args.size());
			for (auto& arg : e->args) {
				auto r = get_single_info(infos, arg);
				ASSERT(r.type);
				result.emplace_back(r);
			}
			TRACE("Tuple: %p", e.get());
			break;

		case Tag::kTGet:
			{
				const auto& args = e->args;
				ASSERT(args.size() == 2);
				const auto& expr = args[0];
				const auto& constant = std::dynamic_pointer_cast<voila::Constant>(args[1]);
				ASSERT(constant && "Needs constant");
				auto index = std::stoll(constant->value);

				auto it = infos.find(expr.get());
				ASSERT(it != infos.end() && "Must be typed");

				const auto& vec = it->second;
				ASSERT((size_t)index < vec.size() && index >= 0);

				ASSERT(vec[index].type);

				result.emplace_back(vec[index]);
			}
			break;

		case Tag::kIfThenElse:
			{
				ASSERT(e->args.size() == 3);
				auto a = get_single_info(infos, e->args[0]);
				auto b = get_single_info(infos, e->args[1]);
				auto c = get_single_info(infos, e->args[2]);
				ASSERT(a.type && b.type && c.type);
				ASSERT(b.type == c.type);
				result.emplace_back(b);
			}
			break;


		default:
			LOG_ERROR("unhandled function tag %s",
				voila::Function::tag2cstring(e->tag));
			ASSERT(false && "todo");
		}
	}

	static bool
	is_int_precheck(const std::string& s) {
		for (auto& c : s) {
			if (c != '-' && !std::isdigit(c)) {
				return false;
			}
		}
		return true;
	}

	void on_expression(const std::shared_ptr<voila::Constant>& e) final {
		if (e->type) {
			result.emplace_back(e->type);
			return;
		}

		// is integer
		try {
			// apparently stoll() parses '1-URGENT' as 1, work around this isses
			if (!is_int_precheck(e->value)) {
				throw std::invalid_argument("cannot be an integer");
			}

			auto integer = std::stoll(e->value);
			result.emplace_back(info_integer(integer, integer));
			return;

		} catch(std::invalid_argument& except) {
			// must be string
			result.emplace_back(info_string());
		}
	}

	void on_expression(const std::shared_ptr<voila::GetScanPos>& e) final {
		result.push_back(info_position());
	}

	void on_expression(const std::shared_ptr<voila::Scan>& e) final {
		auto cat_col = e->col;

		double min, max;
		bool has_minmax = cat_col->get_min_max(min, max);
		if (has_minmax) {
			result.push_back({ NodeInfo(cat_col->data_type, min, max) });
		} else {
			result.push_back({ NodeInfo(cat_col->data_type) });
		}
	}

	void on_expression(const std::shared_ptr<voila::MetaExpression>& e) final {
		result.push_back(get_single_info(infos, e->value));
	}
};




TypeStream::TypeStream(Stream& s)
 : StreamPass(s, "Stream::TypingPass: "),
   infos(s.execution_context.voila_context->infos)
{

}


static int
update_if_needed(NodeInfoMap& infos, voila::Node* p, NodeInfos&& new_infos)
{
	const auto it = infos.find(p);
	if (it == infos.end() || it->second != new_infos) {
		// update
		infos[p] = std::move(new_infos);
		return 1;
	}

	return 0;
}

static int
update_if_needed(NodeInfoMap& infos, voila::Node* p, const NodeInfos& new_infos)
{
	const auto it = infos.find(p);
	if (it == infos.end() || it->second != new_infos) {
		// update
		infos[p] = new_infos;
		return 1;
	}

	return 0;
}

void
TypeStream::lolepop(std::shared_ptr<lolepop::Lolepop>& op) {
	current_op = op;

	auto voila_op = std::dynamic_pointer_cast<lolepop::VoilaLolepop>(op);
	
	LOG_TRACE("%s%sLolepop %s", log_prefix(), voila_op ? "Voila" : "Other",
		op->name.c_str());

	ASSERT(op->rel_op);

	const auto& qconf = *op->stream.query.config;
	const bool dynamic_minmax = qconf.dynamic_minmax();

	double max_tuples = qconf.max_tuples();
	if (dynamic_minmax) {
		double dyn = op->rel_op->child ?
			op->rel_op->child->get_max_tuples() :
			op->rel_op->get_max_tuples();

		max_tuples = std::min(max_tuples, dyn);
	}

	LOG_TRACE("%s: input max_tuples %f", op->name.c_str(), max_tuples);

	if (voila_op) {
		// voila_op->voila_block.dump();
		ASSERT(voila_op->voila_block);

		for (auto& stmt : voila_op->voila_block->statements) {
			// VOILA code
			voila::traverse(
				[&] (const voila::Stmt& stmt) {
					// stmt->dump();

					TypeVoila type(*this, max_tuples, dynamic_minmax);
					stmt->visit(stmt, type);

					num_applied += update_if_needed(infos,
						stmt.get(), std::move(type.result));
				},
				[&] (const voila::Expr& expr) {
					// expr->dump();

					TypeVoila type(*this, max_tuples, dynamic_minmax);
					expr->visit(expr, type);
					num_applied += update_if_needed(infos,
						expr.get(), std::move(type.result));
				},
				stmt);
		}

		for (const auto& pair : variables) {
			auto& var_ptr = pair.first;
			auto& var_infos = pair.second;

			ASSERT(var_infos.size() == 1);

			TRACE("type_stream: variable %p", var_ptr);
			update_if_needed(infos, var_ptr, var_infos);
		}
	} else {
		ASSERT(false);
	}

	previous_op = current_op;
	current_op = nullptr;
}
