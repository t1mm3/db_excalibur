#include "signature.hpp"
#include "system/system.hpp"

#include "engine/voila/voila.hpp"
#include "engine/voila/compile_request.hpp"

#include "engine/types.hpp"
#include "engine/table.hpp"
#include "jit_prepare_pass.hpp"

#include <atomic>
#include <stack>

using namespace engine;
using namespace voila;

#if 0
#define TRACE_DEBUG(...) LOG_DEBUG(__VA_ARGS__)
#define TRACE_WARN(...) LOG_WARN(__VA_ARGS__)
#define TRACE_ERROR(...) LOG_ERROR(__VA_ARGS__)
#define DUMP_VOILA(x) x->dump()
#else
#define TRACE_DEBUG(...) LOG_TRACE(__VA_ARGS__)
#define TRACE_WARN(...) LOG_TRACE(__VA_ARGS__)
#define TRACE_ERROR(...) LOG_TRACE(__VA_ARGS__)
#define DUMP_VOILA(x)
#endif


void
SignatureCache::put(Node* ptr, const Signature& signature)
{
	TRACE_DEBUG("SignatureCache: put(%p, %s)", ptr, signature.c_str());
	DUMP_VOILA(ptr);

	ASSERT(signature.size() > 0);
	auto it = signatures.find(ptr);
	ASSERT(it == signatures.end() && "Must not yet exist");
	signatures.insert({ptr, signature});
}

bool
SignatureCache::get(Signature& output, Node* ptr) const
{
	auto it = signatures.find(ptr);
	if (it == signatures.end()) {
		output.clear();
		return false;
	}

	output = it->second;
	ASSERT(output.size() > 0);
	return true;
}

Signature
SignatureCache::get(Node* ptr) const
{
	TRACE_DEBUG("SignatureCache: get(%p)", ptr);
	DUMP_VOILA(ptr);

	Signature result;
	bool exists = get(result, ptr);
	ASSERT(exists && "Must exist");
	return result;
}

void
SignatureCache::try_put_check_result(bool result)
{
	ASSERT(result);
}

struct AssertExprVisitor : voila::IExpressionVisitor {
	void on_expression(const std::shared_ptr<Function>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<Input>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<InputPredicate>& e) override {
		e->dump();
		ASSERT(false && "InputPredicate");
	}
	void on_expression(const std::shared_ptr<GetScanPos>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<Scan>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<ReadVariable>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<Constant>& e) override {
		ASSERT(false);
	}
	void on_expression(const std::shared_ptr<MetaExpression>& e) override {
		ASSERT(false);
	}
};

struct AssertStmtVisitor : voila::IStatementVisitor {
	void on_statement(const std::shared_ptr<Assign>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<Block>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<Emit>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<Effect>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<Loop>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<EndOfFlow>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<Comment>& b) override {
		ASSERT(false);
	}
	void on_statement(const std::shared_ptr<MetaStatement>& e) override {
		ASSERT(false);
	}
};

struct GenSign : AssertExprVisitor, AssertStmtVisitor {
	GenSign(CompileRequest& req)
	 : cache(req.signature_cache.get()), req(req),
		infos(req.context->infos)
	{
	}

	Signature
	operator()(const NodePtr& sink)
	{
		if (auto expr_ptr = std::dynamic_pointer_cast<voila::Expression>(sink)) {
			return expr(expr_ptr);
		} else if (auto stmt_ptr = std::dynamic_pointer_cast<voila::Statement>(sink)) {
			return stmt(stmt_ptr);
		} else if (auto var_ptr = std::dynamic_pointer_cast<voila::Variable>(sink)) {
			return var(var_ptr);
		} else {
			ASSERT(false);
			return "";
		}
	}

private:
	SignatureCache* cache = nullptr;
	CompileRequest& req;
	const std::unordered_map<voila::Node*, std::vector<voila::Context::NodeInfo>>& infos;

	std::stack<std::ostringstream*> ostream_stack;

	bool descriptive = true;


	template<typename S, typename T>
	void tag2string(std::ostringstream& s, S tag, const T& name_resolver) const {
		size_t itag = (size_t)tag;

		if (descriptive) {
			name_resolver(s, tag);
		} else {
			s << std::hex << itag;
		}
	}

	void struct_col(std::ostringstream& s, const Col& column, bool key, bool this_col) {
		if (key) {
			s << "k";
		} else {
			s << "v";
		}
		if (this_col) {
			s << "XX";
		}

		s << "t";
		tag2string(s, column->type->get_tag(), [&] (auto& s, auto& tag) {
			s << column->type->to_string();
		});
	}

	void produce_output_type(std::ostringstream& s, const NodePtr& e) {
		auto it = infos.find(e.get());
		ASSERT(it != infos.end());

		s << "{";
		bool first = true;
		for(auto& info : it->second) {
			if (!first) {
				s << ",";
			}
			first = false;
			s << "t";
			tag2string(s, info.type->get_tag(), [&] (auto& s, auto& tag) {
				s << info.type->to_string();
			});
		}
		s << "}";
	};

	Signature expr(const voila::Expr& e) {
		return SignatureCache::try_put(cache, e.get(), [&] (auto& result) {
			std::ostringstream s;
			s << "(";

			int64_t source_index = req.get_source_index(e);
			if (source_index >= 0) {
				produce_output_type(s, e);

				// the problem is that the JitPrepare pass might produce sources
				// in arbitrary order
				s << "\\" << source_index;
				s << ")";
				result = s.str();
				return true;
			}

			ostream_stack.push(&s);
			e->visit(e, *this);
			ASSERT(!ostream_stack.empty());
			ostream_stack.pop();

			bool is_read_var = e->flags & Node::kExprReadVar;
			// TODO: remove hack 'is_read_var && e->predicate', was just e->predicate before

			if (!is_read_var && e->predicate) {
				TRACE_ERROR("predicate");
				DUMP_VOILA(e->predicate);
				s << "|" << expr(e->predicate);
			}

			s << "?"; // TODO: '?' should be redundant
			produce_output_type(s, e);

			if (e->data_structure_column) {
				ASSERT(e->data_structure);
			}

			if (e->data_structure) {
				auto ht = std::dynamic_pointer_cast<HashTable>(e->data_structure);
				ASSERT(ht);

				s << "[";
				bool first = true;

				for (auto& key : ht->keys) {
					const bool this_col = e->data_structure_column ?
						key->data_structure_index == e->data_structure_column->data_structure_index :
						false;

					if (!first) {
						s << ",";
					}
					first = false;
					struct_col(s, key, true, this_col);
				}
				for (auto& val : ht->values) {
					const bool this_col = e->data_structure_column ?
						val->data_structure_index == e->data_structure_column->data_structure_index :
						false;
					if (!first) {
						s << ",";
					}
					first = false;
					struct_col(s, val, false, this_col);
				}
#if 0
				auto physical = e->data_structure->physical_table.get();
				ASSERT(physical);
				auto layout = physical->get_layout();
				auto padding_at_end = layout.get_row_padding_at_end();
				bool has_hash = false;
				bool has_next = false;
				int64_t hash_col = layout.get_hash_column(has_hash);
				int64_t next_col = layout.get_next_column(has_next);
				const int64_t kNoCol = -1;

				layout.foreach_column([&] (auto type, auto offset, auto stride) {
					s << ";t";
					tag2string(s, type->get_tag(), [&] (auto& s, auto& tag) {
						s << type->to_string();
					});
				});

				s << "," << (has_hash ? hash_col : kNoCol)
					<< "," << (has_next ? next_col : kNoCol)
					<< "," << padding_at_end;
#endif
				s << "]";
			}

			s << ")";

			result = s.str();
			return true;
		});
	}

	void on_expression(const std::shared_ptr<Function>& e) final {
		TRACE_WARN("Function %p", e.get());

		auto& s = *ostream_stack.top();
		s << "f";

		DUMP_VOILA(e);

		tag2string(s, e->tag, [&] (auto& s, auto tag) {
			s << e->tag2string(e->tag);
		});
		s << "(";
		bool first = true;
		for (auto& a : e->args) {
			if (!first) {
				s << ",";
			}
			first = false;
			s << expr(a);
		}
		s << ")";
	}

	void on_expression(const std::shared_ptr<Constant>& e) final {
		TRACE_WARN("Constant %p", e.get());

		auto& s = *ostream_stack.top();
		s << "c\"" << e->value << "\"";
	}

	void on_statement(const std::shared_ptr<Assign>& b) final {
		TRACE_WARN("Assign %p", b.get());

		auto& s = *ostream_stack.top();
		produce_output_type(s, b->var);
		// ASSERT(req.contains_sink(b->var));
		s << "=" << expr(b->value);
	}

	void on_statement(const std::shared_ptr<Effect>& b) final {
		TRACE_WARN("Effect %p", b.get());

		*ostream_stack.top() << expr(b->value);
	}

	void on_statement(const std::shared_ptr<Comment>& b) final {
		TRACE_WARN("Comment %p", b.get());

		*ostream_stack.top() << "comment'" << b->message << "'";
	}

	void on_statement(const std::shared_ptr<Loop>& b) final {
		TRACE_WARN("Loop %p", b.get());

		auto& s = *ostream_stack.top();
		s << "loop(";
		for (auto& t : b->statements) {
			s << stmt(t) << ";";
		}
		s << ")";
	}

	Signature stmt(const voila::Stmt& e) {
		TRACE_WARN("Stmt %p", e.get());
		DUMP_VOILA(e);
		return SignatureCache::try_put(cache, e.get(), [&] (auto& result) {
			std::ostringstream s;
			TRACE_ERROR("new stmt %p", e.get());
			ostream_stack.push(&s);
			e->visit(e, *this);
			ASSERT(!ostream_stack.empty());
			ostream_stack.pop();
			if (e->predicate) {
				TRACE_ERROR("predicate");
				DUMP_VOILA(e->predicate);
				s << "|" << expr(e->predicate);
			}

			result = s.str();
			return true;
		});
	}

	Signature var(const std::shared_ptr<voila::Variable>& e) {
		TRACE_WARN("Var %p", e.get());
		DUMP_VOILA(e);
		return SignatureCache::try_put(cache, e.get(), [&] (auto& result) {
			std::ostringstream s;
			produce_output_type(s, e);
			result = s.str();
			return true;
		});
	}

	void on_expression(const std::shared_ptr<ReadVariable>& e) final {
		TRACE_WARN("ReadVariable %p", e.get());

		auto& s = *ostream_stack.top();
		s << var(e->var);
	}
};

void
SignatureGen::operator()()
{
	GenSign gen(req);

	TRACE_ERROR("SignatureGen: begin");

	if (req.statements.empty()) {
		for (auto& sink : req.sinks) {
			s << "/" << gen(sink);
		}
	} else {
		if (LOG_WILL_LOG(DEBUG)) {
			for (auto& stmt : req.statements) {
				DUMP_VOILA(stmt);
			}
		}
		for (auto& stmt : req.statements) {
			s << ";" << gen(stmt);
		}
		for (auto& sink : req.sinks) {
			s << "/" << gen(sink);
		}
#if 0
		// complex statement might also involve outside sources
		// therefore, we have to make the part of the signature
		#error but might also include e.g. InputPredicate
		for (auto& source : req.sources) {
			s << "\\" << gen(source);
		}
#endif
	}

	TRACE_ERROR("SignatureGen: end");
}
