#include "voila.hpp"
#include "system/system.hpp"
#include "jit.hpp"
#include "engine/types.hpp"
#include "code_cache.hpp"

#include <sstream>

using namespace engine;
using namespace voila;

static const size_t kIndentInc = 1;
static const std::string kNewLine = "\n";
static const std::string kEndStmt = ";\n";
static const std::string kIndentStep = "  ";

static void
gen_indent(std::ostream& out, size_t indent)
{
	for (size_t i=0; i<indent; i++) {
		out << kIndentStep;
	}
}


void
DataStructure::deallocate()
{
	physical_table.reset();
}

DataStructure::~DataStructure()
{
}



bool
HashTable::add_column(bool key, const Col& col, bool init)
{
	ASSERT(col);

	auto it = col_names.find(col->name);
	if (it != col_names.end()) {
		col->data_structure_index = it->second.phy_idx;
		return false;
	}

	if (key) {
		ASSERT(!init);
		keys.emplace_back(col);
	} else {
		values.emplace_back(col);
	}

	if (init) {
		col_init.insert(col);
	}
	col_names.insert({ col->name, ColInfo {key, col, num_cols}});
	col->data_structure_index = num_cols;


	num_cols++;
	return true;
}

size_t
HashTable::get_phy_column_index(const std::string& name) const
{
	auto it = col_names.find(name);
	ASSERT(it != col_names.end());

	return it->second.phy_idx;
}

size_t
HashTable::get_phy_next_column_index() const
{
	return col_names.size();
}

bool
HashTable::has_column(const std::string& name) const
{
	auto it = col_names.find(name);
	return it != col_names.end();
}

void
HashTable::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "HashTable {" << kNewLine;

	gen_indent(out, indent+1);
	out << "Keys {" << kNewLine;
	for (auto& col : keys) {
		col->to_string(out, indent+2);
		out << kNewLine;
	}
	gen_indent(out, indent+1);
	out << "}" << kNewLine;

	gen_indent(out, indent+1);
	out << "Values {" << kNewLine;
	for (auto& col : values) {
		col->to_string(out, indent+2);
	}
	gen_indent(out, indent+1);
	out << "}" << kNewLine;
	gen_indent(out, indent);
	out << "{" << kNewLine;
}

void
Column::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << name << " : " << type->to_string();
}


void
DebugUtils::_assert_expr_is_pred(const Expression& pred)
{

}

void
Node::dump()
{
	LOG_DEBUG("%s", to_string(0).c_str());
}

std::string
Node::to_string(size_t indent)
{
	std::ostringstream s;
	to_string(s, 0);
	return s.str();
}

void
Assign::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Assign>(this_));
}

void
Assign::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	if (var->global) {
		out << "global ";
	}
	out << "$" << var->dbg_name << " := ";
	value->to_string(out, 0);
	if (predicate) {
		out << " |";
		predicate->to_string(out, 0);
	}
	out << kEndStmt;
}

void
Block::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Block>(this_));
}

void
Block::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Block {" << kNewLine;
	for (auto& s : statements) {
		s->to_string(out, indent+kIndentInc);
	}

	gen_indent(out, indent);
	out << "}" << kEndStmt;
}

void
Emit::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Emit>(this_));
}

void
Emit::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Emit(";
	value->to_string(out, 0);
	out << ")";
	if (predicate) {
		out << " |";
		predicate->to_string(out, 0);
	}
	out << kEndStmt;
}

void
Effect::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Effect>(this_));
}

void
Effect::to_string(std::ostream& out, size_t indent)
{
	value->to_string(out, indent);
	if (predicate) {
		out << " |";
		predicate->to_string(out, 0);
	}
	out << kEndStmt;
}

void
Loop::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Loop>(this_));
}

void
Loop::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Loop |";
	predicate->to_string(out, 0);
	out << " {" << kNewLine;
	for (auto& s : statements) {
		s->to_string(out, indent+kIndentInc);
	}

	gen_indent(out, indent);
	out << "}" << kEndStmt;
}

void
EndOfFlow::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<EndOfFlow>(this_));
}

void
EndOfFlow::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "EndOfFlow" << kEndStmt;
}

void
Comment::visit(const Stmt& this_, IStatementVisitor& visitor)
{
	visitor.on_statement(std::dynamic_pointer_cast<Comment>(this_));
}

void
Comment::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "// " << message <<kEndStmt;
}

void
Variable::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "'" << dbg_name << "'";
}

// Expressions

void
Input::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<Input>(this_));
}

void
Input::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Input";
}

void
InputPredicate::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<InputPredicate>(this_));
}

void
InputPredicate::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "InputPredicate";
}

void
ReadVariable::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<ReadVariable>(this_));
}

void
ReadVariable::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "$" << var->dbg_name;
}

void
Function::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<Function>(this_));
}

void
Function::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << tag2string(tag) << "(";

	bool first = true;
	for (auto& arg : args) {
		if (!first) {
			out << ", ";
		}

		if (arg) {
			arg->to_string(out, 0);
		} else {
			out << "NULL";
		}


		first = false;
	}

	if (predicate) {
		out << "|";
		predicate->to_string(out, 0);
	}
	out << ")";
}

bool
Function::can_evaluate_speculatively(const Context& context) const
{
	auto has_no_string_argument = [&] () {
		for (auto& arg : args) {
			auto it = context.infos.find(arg.get());

			ASSERT(it != context.infos.end());
			if (it->second.size() != 1) {
				return false;
			}

			const auto& info = it->second[0];
			ASSERT(info.type);

			if (info.type->is_string()) {
				return false;
			}
		}
		return true;
	};

	switch (tag) {
	case kLogAnd:
	case kLogOr:
	case kLogNot:
	case kAritAdd:
	case kAritSub:
	case kAritMul:
		return true;

	case kHash:
	case kRehash:
		return has_no_string_argument();

	case kCmpEq:
	case kCmpLt:
	case kCmpGt:
	case kCmpNe:
	case kCmpLe:
	case kCmpGe:
	case kBetweenBothIncl:
	case kBetweenUpperExcl:
	case kBetweenLowerExcl:
	case kBetweenBothExcl:
		return has_no_string_argument();

	default:
		return false;
	}
}

void
Constant::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<Constant>(this_));
}

void
Constant::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "\"" << value << "\"";
}

void
GetScanPos::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<GetScanPos>(this_));
}

void
GetScanPos::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "GetScanPos";
}

void
Scan::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<Scan>(this_));
}

void
Scan::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Scan(";
	pos->to_string(out, 0);
	if (predicate) {
		out << "|";
		predicate->to_string(out, 0);
	}
	out << ")";
}

void
MetaExpression::visit(const Expr& this_, IExpressionVisitor& visitor)
{
	visitor.on_expression(std::dynamic_pointer_cast<MetaExpression>(this_));
}

void
MetaExpression::to_string(std::ostream& out, size_t indent)
{
	gen_indent(out, indent);
	out << "Meta(";
	value->to_string(out, 0);
	out << ")";
}

Function::Function(Tag tag, const Expr& pred, const std::vector<Expr>& args)
 : Expression(Node::kExprFunc, pred), tag(tag), args(args)
{
	ASSERT(tag != Tag::kUnknown)
}


void
Builder::in_loop(const Expr& pred, const std::function<void(Builder&)>& f)
{
	auto block = loop(pred);
	Builder b(*block);
	f(b);
}

void
Builder::while_loop(const std::function<Expr(Builder&)>& pred_gen,
	const std::function<void(Builder&, const Expr&)> loop_gen,
	const std::string& dbg_name_prefix)
{
	auto cond = new_var(pred_gen(*this), dbg_name_prefix + "_cond");
	auto block = loop(ref(cond, nullptr));

	auto in_loop_cond = ref(cond, nullptr);
	Builder b(*block);
	loop_gen(b, in_loop_cond);

	b.assign(in_loop_cond, cond, pred_gen(b));
}

Expr
ExprBuilder::scan_columnar(const Expr& pred, const Expr& pos,
	engine::catalog::Column* col, size_t index)
{
	return std::make_shared<Scan>(pred, pos, col, index);
}

void
BaseBuilder::bucket_aggr(const Function::Tag tag, const Expr& pred,
	const DataStruct& ds, const Expr& bucket, const Col& col,
	const Expr& vals, bool reaggr)
{
	if (tag == Function::Tag::kBucketAggrCount || tag == Function::Tag::kGlobalAggrCount) {
		ASSERT(!vals);
	} else {
		ASSERT(vals);
	}

	_bucket_scatter_like(tag, pred, ds,
		bucket, col, vals, reaggr);
}

void
BaseBuilder::write(const Expr& pred, const DataStruct& ds,
	const Col& column, const Expr& pos, const Expr& val)
{
	ASSERT(ds && column);

#if 0
	auto f = std::dynamic_pointer_cast<Function>(pos);
	ASSERT(f && f->tag == Function::Tag::kWritePos);
	ASSERT(ds.get() && ds.get() == f->data_structure.get());
#else
	ASSERT(ds.get());
#endif
	ASSERT(ds->has_column(column->name));

	auto expr = func(Function::Tag::kWriteCol, pred, {pos, val});
	expr->data_structure = ds;
	expr->data_structure_column = column;

	effect(std::move(expr));
}

Expr
ExprBuilder::bucket_gather(const Expr& pred, const DataStruct& ds, const Expr& bucket,
	const Col& col)
{
	ASSERT(pred && ds && bucket && col);

	auto expr = func(Function::Tag::kBucketGather, pred, { bucket });
	expr->data_structure = ds;
	expr->data_structure_column = col;
	return expr;
}

Expr
ExprBuilder::bucket_check(const Expr& pred, const DataStruct& ds, const Expr& bucket,
	const Col& col, const Expr& val)
{
#if 0
	return eq(pred,
		bucket_gather(pred, ds, bucket, col),
		val);
#else
	ASSERT(pred && ds && bucket && col);

	auto expr = func(Function::Tag::kBucketCheck, pred, { bucket, val });
	expr->data_structure = ds;
	expr->data_structure_column = col;
	return expr;
#endif
}