#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <iostream>
#include <mutex>

namespace engine {

struct Type;

namespace table {
struct ITable;
}

namespace catalog {
struct Column;
}


namespace voila {

struct Statement;
struct Expression;
struct Variable;
struct DataStructure;
struct Column;
struct Context;
struct StatementIdentifier;
struct StatementRange;

typedef std::shared_ptr<Expression> Expr;
typedef std::shared_ptr<Statement> Stmt;
typedef std::shared_ptr<Variable> Var;
typedef std::shared_ptr<Column> Col;
typedef std::shared_ptr<DataStructure> DataStruct;


struct DebugUtils {
	static void _assert_expr_is_pred(const Expression& pred);
	inline static void _assert_expr_is_pred(const Expr& pred) {
		if (pred) {
			_assert_expr_is_pred(*pred);
		}	
	}
};

struct Node;
typedef std::shared_ptr<Node> NodePtr;

struct Node {
	typedef uint64_t Flags;

	static constexpr Flags kStmtAssign = 1ll << 0;
	static constexpr Flags kStmtBlock  = 1ll << 1;
	static constexpr Flags kStmtEmit   = 1ll << 2;
	static constexpr Flags kStmtEffect = 1ll << 3;
	static constexpr Flags kStmtLoop   = 1ll << 4;
	static constexpr Flags kStmtEndOfFlow = 1ll << 13;
	static constexpr Flags kStmtComment = 1ll << 14;
	static constexpr Flags kStmtMeta   = 1ll << 17;

	static constexpr Flags kAllStmts  = kStmtAssign | kStmtBlock | kStmtEmit |
										kStmtEffect | kStmtLoop | kStmtEndOfFlow |
										kStmtComment| kStmtMeta;

	static constexpr Flags kExprInput  = 1ll << 5;
	static constexpr Flags kExprInputPred = 1ll << 6;
	static constexpr Flags kExprReadVar   = 1ll << 7;
	static constexpr Flags kExprFunc   = 1ll << 8;
	static constexpr Flags kExprConst  = 1ll << 9;
	static constexpr Flags kExprReadDs = 1ll << 10;
	static constexpr Flags kExprGetScanPos = 1ll << 11;
	static constexpr Flags kExprScan   = 1ll << 12;
	static constexpr Flags kExprMeta   = 1ll << 18;

	static constexpr Flags kAllExprs  = kExprInput | kExprInputPred | kExprReadVar |
										kExprFunc | kExprConst | kExprReadDs |
										kExprGetScanPos | kExprScan | kExprMeta;

	static constexpr Flags kStructHashTable   = 1ll << 15;
	static constexpr Flags kAllStructs   = kStructHashTable;
	static constexpr Flags kColumn   = 1ll << 16;
	static constexpr Flags kVariable   = 1ll << 19;

	const Flags flags;
	Node(Flags flags) : flags(flags) {}
	virtual ~Node() = default;

	virtual void to_string(std::ostream& out, size_t indent) = 0;

	virtual std::string to_string(size_t indent);
	virtual void dump();


	// bool can_inline = true;

};

struct Context {
	struct NodeInfo {
		Type* type;

		double min;
		double max;
		bool has_minmax;

		NodeInfo() {
			type = nullptr;
			min = 0;
			max = 0;
			has_minmax = false;
		}

		NodeInfo(Type* type, double min, double max)
		 : type(type), min(min), max(max) {
		 	has_minmax = true;
		}

		NodeInfo(Type* type)
		 : type(type), min(0), max(0) {
		 	has_minmax = false;
		}

		NodeInfo(const NodeInfo& i) {
			type = i.type;
			min = i.min;
			max = i.max;
			has_minmax = i.has_minmax;
		}

		bool operator==(const NodeInfo& i) const {
			bool eq = true;
			// pointer equality is enough
			eq &= type == i.type;
			eq &= has_minmax == i.has_minmax;

			if (has_minmax) {
				eq &= min == i.min && max == i.max;
			}

			return eq;
		}


	};

	struct LocationInfo {
		std::shared_ptr<StatementIdentifier> location;
		std::shared_ptr<StatementRange> range;
	};

	std::unordered_map<Node*, std::vector<NodeInfo>> infos;
	std::unordered_map<Node*, LocationInfo> locations;
};


struct Column : Node {
	const std::string name;
	Type* type;

	// more or less copy of NodeInfo:
	std::unique_ptr<Context::NodeInfo> node_info;

	std::mutex mutex;

	int64_t data_structure_index = -1;

	Column(const std::string& name, Type* type)
	 : Node(Node::kColumn), name(name), type(type) {}

	virtual void to_string(std::ostream& out, size_t indent) override;
};

struct DataStructure : Node {
	std::shared_ptr<engine::table::ITable> physical_table;

	virtual bool has_column(const std::string& name) const {
		return false;
	}

	virtual size_t get_phy_column_index(const std::string& name) const = 0;
	virtual size_t get_phy_next_column_index() const = 0;

	virtual void deallocate();

	DataStructure(Node::Flags flags) : Node(flags) {}
	~DataStructure();
};

struct HashTable : DataStructure {
	std::vector<Col> keys;
	std::vector<Col> values;

private:
	struct ColInfo {
		bool key;
		Col col;
		size_t phy_idx;
	};
	std::unordered_map<std::string, ColInfo> col_names;
	std::unordered_set<Col> col_init;
	size_t num_cols = 0;

public:
	HashTable() : DataStructure(Node::kStructHashTable) {}

	size_t get_phy_column_index(const std::string& name) const override;
	size_t get_phy_next_column_index() const override;


	bool add_column(bool key, const Col& col, bool init = false);
	bool has_column(const std::string& name) const override;

	Col get_column(const std::string& name) const {
		auto it = col_names.find(name);
		if (it == col_names.end()) {
			return nullptr;
		}

		return it->second.col;
	}

	virtual void to_string(std::ostream& out, size_t indent) override;

	bool column_must_init(const Col& col) const {
		return col_init.find(col) != col_init.end();
	}
};


struct IStatementVisitor;

struct Statement : Node {
	Expr predicate;

	Statement(const Node::Flags& flags, const Expr& pred)
	 : Node(flags), predicate(pred) {
		DebugUtils::_assert_expr_is_pred(pred);
	}

	virtual void visit(const Stmt& this_, IStatementVisitor& visitor) = 0;

	virtual ~Statement() = default;

	std::unordered_set<Var> var_dead_after;
};


struct Assign : Statement {
	Var var;
	Expr value;

	Assign(const Var& var, const Expr& value, const Expr& pred)
	 : Statement(Node::kStmtAssign, pred), var(var), value(value) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};

struct Block : Statement {
	std::vector<Stmt> statements;

	Block(const Expr& pred, const Node::Flags& flags = Node::kStmtBlock)
	 : Statement(flags | Node::kStmtBlock, pred) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};

struct Emit : Statement {
	Expr value;

	Emit(const Expr& value, const Expr& pred)
	 : Statement(Node::kStmtEmit, pred), value(value) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};


struct Effect : Statement {
	Expr value;

	Effect(const Expr& value, const Expr& pred)
	 : Statement(Node::kStmtEffect, pred), value(value) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};

struct Loop : Block {
	Loop(const Expr& pred)
	 : Block(pred, Node::kStmtLoop) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};

struct EndOfFlow : Statement {
	EndOfFlow()
	 : Statement(Node::kStmtEndOfFlow, nullptr) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
};

struct Comment : Statement {
	const std::string message;
	int log_level;

	Comment(const std::string& message, const Expr& predicate, int log_level)
	 : Statement(Node::kStmtComment, predicate), message(message), log_level(log_level) {}

	void visit(const Stmt& this_, IStatementVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;	
};

struct MetaStatement : Statement {
	MetaStatement() : Statement(Node::kStmtMeta, nullptr) {}
};

struct MetaVarDead : MetaStatement {
	std::vector<Var> vars;

	MetaVarDead(const std::vector<Var>& vars) : MetaStatement(), vars(vars) {}
};

struct IStatementVisitor {
	virtual void on_statement(const std::shared_ptr<Assign>& b) = 0;
	virtual void on_statement(const std::shared_ptr<Block>& b) = 0;
	virtual void on_statement(const std::shared_ptr<Emit>& b) = 0;
	virtual void on_statement(const std::shared_ptr<Effect>& b) = 0;
	virtual void on_statement(const std::shared_ptr<Loop>& b) = 0;
	virtual void on_statement(const std::shared_ptr<EndOfFlow>& b) = 0;
	virtual void on_statement(const std::shared_ptr<Comment>& b) = 0;
	virtual void on_statement(const std::shared_ptr<MetaStatement>& b) = 0;
};



struct Variable : Node {
	const std::string dbg_name;
	bool global;

	// To track struct usage across variables, otherwise read_pos/read_col won't compile
	DataStruct data_structure;

	Variable(const std::string& dbg_name, bool global)
	 : Node(Node::kVariable), dbg_name(dbg_name), global(global)
	{

	}

	void to_string(std::ostream& out, size_t indent) override;
};

// Expressions

struct IExpressionVisitor;

struct Expression : Node {
	Expr predicate;

	DataStruct data_structure;
	Col data_structure_column;
	bool data_structure_can_change = true;

	Expression(const Node::Flags& flags, const Expr& pred)
	 : Node(flags), predicate(pred) {
		DebugUtils::_assert_expr_is_pred(pred);
	}

	virtual void visit(const Expr& this_, IExpressionVisitor& visitor) = 0;

	virtual bool can_evaluate_speculatively(const Context& context) const {
		return false;
	}

	// Creates a copy of this - and only this - exoression
	virtual Expr clone() = 0;

protected:
	void copy_base_properties(Expression& out) {
		out.predicate = predicate;
		out.data_structure = data_structure;
		out.data_structure_column = data_structure_column;
	}

	Expr clone_wrapper(Expr&& r)
	{
		copy_base_properties(*r);
		return std::move(r);
	}
};

struct Input : Expression {
	Input() : Expression(Node::kExprInput, nullptr) {}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;

	Expr clone() override {
		return clone_wrapper(std::make_shared<Input>());
	}
};

struct InputPredicate : Expression {
	InputPredicate() : Expression(Node::kExprInputPred, nullptr) {}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;

	Expr clone() override {
		return clone_wrapper(std::make_shared<InputPredicate>());
	}
};

struct ReadVariable : Expression {
	Var var;

	ReadVariable(const Expr& pred, const Var& var)
	 : Expression(Node::kExprReadVar, pred), var(var) {
	 }

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;

	Expr clone() override {
		return clone_wrapper(std::make_shared<ReadVariable>(predicate, var));
	}
};

struct Function : Expression {
	enum Tag {
		kUnknown = 0,

#define EXPAND_TAGS(F, ARGS) \
		F(TGet, ARGS) \
		F(Tuple, ARGS) \
		\
		F(SelTrue, ARGS) \
		F(SelFalse, ARGS) \
		F(SelNum, ARGS) \
		F(SelUnion, ARGS) \
		\
		F(Hash, ARGS) \
		F(Rehash, ARGS) \
		\
		F(BucketLookup, ARGS) \
		F(BloomFilterLookup1, ARGS) \
		F(BloomFilterLookup2, ARGS) \
		F(BucketGather, ARGS) \
		F(BucketCheck, ARGS) \
		F(BucketScatter, ARGS) \
		F(BucketInsert, ARGS) \
		F(BucketAggrSum, ARGS) \
		F(BucketAggrCount, ARGS) \
		F(BucketAggrMin, ARGS) \
		F(BucketAggrMax, ARGS) \
		F(BucketNext, ARGS) \
		F(ReadPos, ARGS) \
		F(ReadCol, ARGS) \
		F(WritePos, ARGS) \
		F(WriteCol, ARGS) \
		\
		F(GlobalAggrSum, ARGS) \
		F(GlobalAggrCount, ARGS) \
		F(GlobalAggrMin, ARGS) \
		F(GlobalAggrMax, ARGS) \
		\
		F(CmpEq, ARGS) \
		F(CmpLt, ARGS) \
		F(CmpGt, ARGS) \
		F(CmpNe, ARGS) \
		F(CmpLe, ARGS) \
		F(CmpGe, ARGS) \
		F(BetweenBothIncl, ARGS) \
		F(BetweenUpperExcl, ARGS) \
		F(BetweenLowerExcl, ARGS) \
		F(BetweenBothExcl, ARGS) \
		F(Contains, ARGS) \
		\
		F(LogAnd, ARGS) \
		F(LogOr, ARGS) \
		F(LogNot, ARGS) \
		\
		F(AritAdd, ARGS) \
		F(AritSub, ARGS) \
		F(AritMul, ARGS) \
		F(ExtractYear, ARGS) \
		F(IfThenElse, ARGS)

		#define T(N, _) k##N,
		EXPAND_TAGS(T, 0)
		#undef T
	};
	const Tag tag;
	std::vector<Expr> args;

	Function(Tag tag, const Expr& pred, const std::vector<Expr>& args);

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;

	Expr clone() override {
		return clone_wrapper(std::make_shared<Function>(tag, predicate, args));
	}

	static std::string tag2string(Tag t) {
		switch (t) {
		#define CASE(N, _) case Tag::k##N: return #N;
		EXPAND_TAGS(CASE, 0)
		#undef CASE

		default: return "Unknown";
		}
	}

	static const char* tag2cstring(Tag t) {
		switch (t) {
		#define CASE(N, _) case Tag::k##N: return #N;
		EXPAND_TAGS(CASE, 0)
		#undef CASE

		default: return "Unknown";
		}
	}
#undef EXPAND_TAGS

	bool can_evaluate_speculatively(const Context& context) const final;
	bool creates_predicate() const { return creates_predicate(tag); }

	static const uint64_t kReadsFromMemory	= 1ull << 1ull;
	static const uint64_t kWritesToMemory	= 1ull << 2ull;
	static const uint64_t kCreatesPredicate	= 1ull << 3ull;
	static const uint64_t kComplexOperation = 1ull << 4ull;
	static const uint64_t kColocatableAccess = 1ull << 5ull;
	static const uint64_t kRandomAccess = 1ull << 6ull;

	static const uint64_t kALL = kReadsFromMemory | kWritesToMemory |
		kCreatesPredicate | kComplexOperation |
		kColocatableAccess | kRandomAccess;

	static uint64_t get_flags(Function::Tag tag) {
		switch (tag) {
		case kBloomFilterLookup1:
		case kBloomFilterLookup2:
		case kBucketLookup:
			return kReadsFromMemory | kRandomAccess;
		case kBucketGather:
		case kBucketCheck:
			return kReadsFromMemory | kColocatableAccess | kRandomAccess;
		case kBucketScatter:
			return kWritesToMemory | kColocatableAccess | kRandomAccess;

		case kBucketInsert:
			return kComplexOperation | kReadsFromMemory | kWritesToMemory;
		case kBucketAggrSum:
		case kBucketAggrCount:
		case kBucketAggrMin:
		case kBucketAggrMax:
			return kReadsFromMemory | kWritesToMemory | kColocatableAccess | kRandomAccess;

		case kBucketNext:
			return kReadsFromMemory | kRandomAccess;
		case kReadCol:
			return kReadsFromMemory | kColocatableAccess;
		case kWriteCol:
			return kWritesToMemory | kColocatableAccess;

		case kSelFalse:
		case kSelTrue:
		case kSelNum:
			return kCreatesPredicate;
		case kSelUnion:
			return kCreatesPredicate | kComplexOperation;

		case kWritePos:
			return kComplexOperation;

		default:
			return 0;
		}
	}

	uint64_t get_flags() const { return get_flags(tag); }

	bool accesses_memory() const {
		auto flags = get_flags();
		return (flags & kReadsFromMemory) || (flags & kWritesToMemory);
	}

	bool is_reaggregation = false;

private:
	static bool creates_predicate(voila::Function::Tag tag) {
		return get_flags(tag) & kCreatesPredicate;
	}
};

struct Constant : Expression {
	std::string value;
	Type* type;

	Constant(const std::string& value)
	 : Expression(Node::kExprConst, nullptr), value(value), type(nullptr) {}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
	Expr clone() override {
		return clone_wrapper(std::make_shared<Constant>(value));
	}
};

namespace string {

template<typename T>
inline std::string
stringify(const T& x) {
	return std::to_string(x);
}

template<>
inline std::string
stringify<std::string>(const std::string& x) {
	return x;
}

} /* string */

struct ReadPosition {
	// set by application
	char** column_data = nullptr;

	size_t num_columns = 0;
	size_t num_tuples = 0;
	bool done = true;

	virtual void next(size_t num) = 0;
	virtual double get_progress() = 0;

	// set from VOILA
	size_t num_consumed = 0;
	size_t num_flow = 0;

	size_t num_calls = 0;

	virtual ~ReadPosition() = default;
};

struct GetScanPos : Expression {
	ReadPosition& pos;

	GetScanPos(ReadPosition& pos) : Expression(Node::kExprGetScanPos, nullptr), pos(pos) {
	}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
	Expr clone() override {
		return clone_wrapper(std::make_shared<GetScanPos>(pos));
	}
};

struct Scan : Expression {
	Expr pos;
	engine::catalog::Column* col;
	size_t index;

	Scan(const Expr& pred, const Expr& pos, engine::catalog::Column* col, size_t index)
	 : Expression(Node::kExprScan, pred), pos(pos), col(col), index(index) {}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;
	Expr clone() override {
		return clone_wrapper(std::make_shared<Scan>(predicate, pos, col, index));
	}
};

struct MetaExpression : Expression {
	Expr value;

	MetaExpression(const Expr& value)
	 : Expression(Node::kExprMeta, nullptr), value(value) {}

	void visit(const Expr& this_, IExpressionVisitor& visitor) override;
	void to_string(std::ostream& out, size_t indent) override;

	Expr clone() override {
		return clone_wrapper(std::make_shared<MetaExpression>(value));
	}
};

struct IExpressionVisitor {
	virtual void on_expression(const std::shared_ptr<Input>& e) = 0;
	virtual void on_expression(const std::shared_ptr<InputPredicate>& e) = 0;
	virtual void on_expression(const std::shared_ptr<ReadVariable>& e) = 0;
	virtual void on_expression(const std::shared_ptr<Function>& e) = 0;
	virtual void on_expression(const std::shared_ptr<Constant>& e) = 0;
	virtual void on_expression(const std::shared_ptr<GetScanPos>& e) = 0;
	virtual void on_expression(const std::shared_ptr<Scan>& e) = 0;
	virtual void on_expression(const std::shared_ptr<MetaExpression>& e) = 0;
};

struct ExprBuilder {
public:
	template<typename T>
	Expr func(const T& name, const Expr& pred,
			const std::vector<Expr>& args) {
		return std::make_shared<Function>(name, pred, args);
	}

	template<typename T>
	Expr select(const T& name, const Expr& pred,
			const std::vector<Expr>& args) {
		return std::make_shared<Function>(name, pred, args);
	}

	Expr input() {
		return std::make_shared<Input>(); 
	}

	Expr input_pred() {
		return std::make_shared<InputPredicate>();
	}

	template<typename T>
	Expr constant(const T& val) {
		return std::make_shared<Constant>(string::stringify<T>(val));
	}

	Expr get(const Expr& pred, const Expr& expr, size_t i) {
		return func(Function::Tag::kTGet, pred, {expr, constant(i)});
	}

	Expr tuple(const Expr& pred, const std::vector<Expr>& vals) {
		return func(Function::Tag::kTuple, pred, vals);
	}

	Expr seltrue(const Expr& pred, const Expr& expr) {
		return select(Function::Tag::kSelTrue, pred, {expr});
	}

	Expr selfalse(const Expr& pred, const Expr& expr) {
		return select(Function::Tag::kSelFalse, pred, {expr});
	}

	Expr selnum(const Expr& pred, const Expr& expr) {
		return select(Function::Tag::kSelNum, pred, {expr});
	}

	Expr selunion(const Expr& pred, const Expr& expr) {
		return select(Function::Tag::kSelUnion, pred, {expr});
	}

	Expr scan_pos(ReadPosition& callback) {
		return std::make_shared<GetScanPos>(callback);
	}

	Expr scan_columnar(const Expr& pred, const Expr& pos,
		engine::catalog::Column* col, size_t index);

	Expr read_col(const Expr& pred, const Expr& pos, const DataStruct& ds, const Col& col) {
		auto r = func(Function::Tag::kReadCol, pred, { pos });
		r->data_structure_column = col;
		r->data_structure = ds;
		return r;
	}

	Expr hash(const Expr& pred, const Expr& val) {
		return func(Function::Tag::kHash, pred, { val });
	}

	Expr rehash(const Expr& pred, const Expr& h, const Expr& val) {
		return func(Function::Tag::kRehash, pred, { h, val });
	}

	Expr bucket_lookup(const Expr& pred, const DataStruct& ds, const Expr& hash,
			bool can_change = true) {
		auto expr = func(Function::Tag::kBucketLookup, pred, { hash });
		expr->data_structure = ds;
		expr->data_structure_can_change = can_change;
		return expr;
	}

	Expr bloomfilter_lookup(const Expr& pred, const DataStruct& ds, const Expr& hash,
			size_t bits) {
		auto expr = func(
			bits == 1 ? Function::Tag::kBloomFilterLookup1 : Function::Tag::kBloomFilterLookup2,
			pred, { hash });
		expr->data_structure = ds;
		return expr;
	}

	Expr bucket_check(const Expr& pred, const DataStruct& ds, const Expr& bucket,
			const Col& col, const Expr& val);

	Expr bucket_gather(const Expr& pred, const DataStruct& ds, const Expr& bucket,
		const Col& col);

	Expr eq(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kCmpEq, pred, { a, b });
	}

	Expr lt(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kCmpLt, pred, { a, b });
	}

	Expr gt(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kCmpGt, pred, { a, b });
	}

	Expr add(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kAritAdd, pred, { a, b });
	}

	Expr sub(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kAritSub, pred, { a, b });
	}

	Expr mul(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kAritMul, pred, { a, b });
	}

	Expr extract_year(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kExtractYear, pred, { a, b });
	}
	Expr l_and(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kLogAnd, pred, { a, b });
	}

	Expr dbg_name(const Expr& expr, const std::string& name) {
		return expr;
	}

	Expr contains(const Expr& pred, const Expr& a, const Expr& b) {
		return func(Function::Tag::kContains, pred, { a, b });
	}

	Expr ref(const Var& var, const Expr& pred) {
		auto r = std::make_shared<ReadVariable>(pred, var);
		r->data_structure = var->data_structure;
		return r;
	}

};

struct BaseBuilder : ExprBuilder {
protected:
	std::vector<Stmt>& statements;

	std::shared_ptr<Block> loop(const Expr& pred) {
		auto ptr = std::make_shared<Loop>(pred);
		statements.emplace_back(ptr);
		return ptr;
	}

	BaseBuilder(std::vector<Stmt>& statements) : statements(statements) {
	}
public:
	BaseBuilder(Block& block) : statements(block.statements) {
	}

	void emit(const Expr& pred, const Expr& values) {
		statements.emplace_back(std::make_shared<Emit>(values, pred));
	}

	std::shared_ptr<Assign> new_assign(const Expr& pred, const Var& var, const Expr& val,
			bool can_inline = true, bool force_copy = false) {
		var->data_structure = val->data_structure;
		std::shared_ptr<Assign> a(std::make_shared<Assign>(var, val, pred));
		// a->can_inline = can_inline;
		statements.emplace_back(a);
		return a;
	}

	void assign(const Expr& pred, const Var& var, const Expr& val, bool can_inline = true) {
		new_assign(pred, var, val, can_inline);
	}

	void effect(const Expr& pred, const Expr& expr) {
		statements.emplace_back(std::make_shared<Effect>(expr, pred));
	}

	void effect(const Expr& expr) {
		return effect(expr->predicate, expr);
	}

	void end_of_flow() {
		statements.emplace_back(std::make_shared<EndOfFlow>());
	}

	void comment(const Expr& pred, const std::string& m, int log_level = 2) {
		statements.emplace_back(std::make_shared<Comment>(m, pred, log_level));
	}

	void comment(const std::string& m, int log_level = 2) {
		comment(nullptr, m, log_level);
	}

	void write(const Expr& pred, const DataStruct& ds, const Col& column,
		const Expr& pos, const Expr& val);

private:
	void _bucket_scatter_like(const Function::Tag& tag, const Expr& pred,
			const DataStruct& ds, const Expr& bucket, const Col& col,
			const Expr& vals, bool is_reaggregation = false) {
		auto expr = std::make_shared<Function>(tag, pred, vals ?
			std::vector<Expr> { bucket, vals } : std::vector<Expr> { bucket });
		expr->data_structure = ds;
		expr->data_structure_column = col;
		expr->is_reaggregation = is_reaggregation;
		effect(std::move(expr));
	}

public:
	void bucket_scatter(const Expr& pred, const DataStruct& ds, const Expr& bucket,
			const Col& col, const Expr& vals) {
		_bucket_scatter_like(Function::Tag::kBucketScatter, pred, ds,
			bucket, col, vals);
	}

	Expr bucket_insert(const Expr& pred, const DataStruct& ds, const Expr& hash) {
		auto expr = func(Function::Tag::kBucketInsert, pred, { hash });
		expr->data_structure = ds;
		return expr;
	}

	void bucket_aggr(const Function::Tag tag, const Expr& pred,
		const DataStruct& ds, const Expr& bucket, const Col& col,
		const Expr& vals, bool reaggr = false);

	Expr bucket_next(const Expr& pred, const DataStruct& ds, const Expr& bucket) {
		auto expr = func(Function::Tag::kBucketNext, pred, { bucket });
		expr->data_structure = ds;
		return expr;
	}

	Expr write_pos(const Expr& pred, const DataStruct& ds) {
		auto expr = func(Function::Tag::kWritePos, pred, { });
		expr->data_structure = ds;
		return expr;
	}

	Expr read_pos(ReadPosition& callback, const DataStruct& ds) {
		auto expr = scan_pos(callback);
		expr->data_structure = ds;
		return expr;
	}

	Var new_var(const std::string& dbg_name, bool global = false) {
		return std::make_shared<Variable>(dbg_name, global);
	}

	Var new_var(const Expr& initial_val, const std::string& dbg_name,
			bool global = false, bool can_inline = true) {
		auto var = std::make_shared<Variable>(dbg_name, global);
		assign(nullptr, var, initial_val, can_inline);
		return var;
	}
};

struct DirectStatementBuilder : BaseBuilder {
	DirectStatementBuilder(std::vector<Stmt>& stmts) : BaseBuilder(stmts) {}
};

struct Builder : BaseBuilder {
	Builder(Block& block) : BaseBuilder(block.statements) {}

	void in_loop(const Expr& pred, const std::function<void(Builder&)>& f);

	void while_loop(const std::function<Expr(Builder&)>& pred_gen,
		const std::function<void(Builder&, const Expr&)> loop_gen,
		const std::string& dbg_name_prefix);

};

struct VoilaTreePass : IStatementVisitor, IExpressionVisitor {
	virtual void operator()(const std::vector<Stmt>& stmts) {
		handle(stmts);
		finalize();
	}

protected:
	void on_statement(const std::shared_ptr<Assign>& b) override {
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Block>& b) override {
		handle(b->predicate);
		handle(b->statements);
	}
	void on_statement(const std::shared_ptr<Emit>& b) override {
		handle(b->predicate);
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Effect>& b) override {
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Loop>& b) override {
		handle(b->predicate);
		handle(b->statements);
	}
	void on_statement(const std::shared_ptr<EndOfFlow>& b) override {}
	void on_statement(const std::shared_ptr<Comment>& b) override {}
	void on_statement(const std::shared_ptr<MetaStatement>& b) override {}


	void on_expression(const std::shared_ptr<Input>& e) override {}
	void on_expression(const std::shared_ptr<InputPredicate>& e) override {}
	void on_expression(const std::shared_ptr<ReadVariable>& e) override {}
	void on_expression(const std::shared_ptr<Function>& e) override {
		handle(e->predicate);
		for (auto& arg : e->args) {
			handle(arg);
		}
	}
	void on_expression(const std::shared_ptr<Constant>& e) override {}
	void on_expression(const std::shared_ptr<GetScanPos>& e) override {}
	void on_expression(const std::shared_ptr<Scan>& e) override {
		handle(e->pos);
	}
	void on_expression(const std::shared_ptr<MetaExpression>& e) override {
		handle(e->value);
	}

	virtual void handle(const std::vector<Stmt>& stmts) {
		for (auto& stmt : stmts) {
			handle(stmt);
		}
	}

	virtual void handle(const Stmt& s) {
		if (s) {
			s->visit(s, *this);
		}
	}

	virtual void handle(const Expr& s) {
		if (s) {
			s->visit(s, *this);
		}
	}

	virtual void finalize() {}
};

// same as VoilaTreePass but we can modify Statements and Expressions
struct VoilaTreeRewritePass : IStatementVisitor, IExpressionVisitor {
	virtual void operator()(std::vector<Stmt>& stmts) {
		handle(stmts);

		finalize();
	}

protected:
	void on_statement(const std::shared_ptr<Assign>& b) override {
		handle(b->predicate);
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Block>& b) override {
		handle(b->predicate);
		handle(b->statements);
	}
	void on_statement(const std::shared_ptr<Emit>& b) override {
		handle(b->predicate);
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Effect>& b) override {
		handle(b->predicate);
		handle(b->value);
	}
	void on_statement(const std::shared_ptr<Loop>& b) override {
		handle(b->predicate);
		handle(b->statements);
	}
	void on_statement(const std::shared_ptr<EndOfFlow>& b) override {}
	void on_statement(const std::shared_ptr<Comment>& b) override {}
	void on_statement(const std::shared_ptr<MetaStatement>& b) override {}


	void on_expression(const std::shared_ptr<Input>& e) override {}
	void on_expression(const std::shared_ptr<InputPredicate>& e) override {}
	void on_expression(const std::shared_ptr<ReadVariable>& e) override {}
	void on_expression(const std::shared_ptr<Function>& e) override {
		handle(e->predicate);
		for (auto& arg : e->args) {
			handle(arg);
		}
	}
	void on_expression(const std::shared_ptr<Constant>& e) override {}
	void on_expression(const std::shared_ptr<GetScanPos>& e) override {}
	void on_expression(const std::shared_ptr<Scan>& e) override {
		handle(e->pos);
	}
	void on_expression(const std::shared_ptr<MetaExpression>& e) override {
		handle(e->value);
	}

	virtual void handle(std::vector<Stmt>& stmts) {
		for (auto& s : stmts) {
			handle(s);
		}
	}

	virtual void handle(Stmt& s) {
		if (s) {
			s->visit(s, *this);
		}
	}

	virtual void handle(Expr& s) {
		if (s) {
			s->visit(s, *this);
		}
	}

	virtual void finalize() {

	}
};

struct VoilaGraphTreePass : VoilaTreePass {
protected:
	void on_statement(const std::shared_ptr<Assign>& b) override {
		add_stmt_expr_edge(false, b, b->value);
		VoilaTreePass::on_statement(b);
	}
	void on_statement(const std::shared_ptr<Block>& b) override {
		add_stmt_expr_edge(true, b, b->predicate);
		VoilaTreePass::on_statement(b);
	}
	void on_statement(const std::shared_ptr<Emit>& b) override {
		add_stmt_expr_edge(true, b, b->predicate);
		add_stmt_expr_edge(false, b, b->value);
		VoilaTreePass::on_statement(b);
	}
	void on_statement(const std::shared_ptr<Effect>& b) override {
		add_stmt_expr_edge(false, b, b->value);
		VoilaTreePass::on_statement(b);
	}
	void on_statement(const std::shared_ptr<Loop>& b) override {
		add_stmt_expr_edge(true, b, b->predicate);
		VoilaTreePass::on_statement(b);
	}



	void on_expression(const std::shared_ptr<Function>& e) override {
		add_expr_expr_edge(true, e, e->predicate);
		for (auto& arg : e->args) {
			add_expr_expr_edge(false, e, arg);
		}
		VoilaTreePass::on_expression(e);
	}
	void on_expression(const std::shared_ptr<Scan>& e) override {
		add_expr_expr_edge(false, e, e->pos);
		VoilaTreePass::on_expression(e);
	}

	virtual void add_expr_expr_edge(bool predicate_edge, const Expr& from,
		const Expr& to) {}

	virtual void add_stmt_expr_edge(bool predicate_edge, const Stmt& from,
		const Expr& to) {}
};

// TODO: Rewrite using VoilaTreePass
template<typename STMT_CALL_T, typename EXPR_CALL_T>
struct TraverseProgram : IStatementVisitor, IExpressionVisitor {
private:
	void on_statement(const std::shared_ptr<Assign>& b) final {
		expr(b->value);
	}

	void on_statement(const std::shared_ptr<Block>& b) final {
		for (auto& i : b->statements) {
			stmt(i);
		}
	}

	void on_statement(const std::shared_ptr<Emit>& b) final {
		expr(b->value);
	}

	void on_statement(const std::shared_ptr<Effect>& b) final {
		expr(b->value);	
	}

	void on_statement(const std::shared_ptr<Loop>& b) final {
		for (auto& i : b->statements) {
			stmt(i);
		}
	}

	void on_statement(const std::shared_ptr<EndOfFlow>& b) final {}
	void on_statement(const std::shared_ptr<Comment>& b) final {}
	void on_statement(const std::shared_ptr<MetaStatement>& b) final {}

	void on_expression(const std::shared_ptr<Input>& e) final {}
	void on_expression(const std::shared_ptr<InputPredicate>& e) final {}
	void on_expression(const std::shared_ptr<ReadVariable>& e) final {}
	void on_expression(const std::shared_ptr<Function>& e) final {
		for (auto arg : e->args) {
			expr(arg);
		}
	}
	void on_expression(const std::shared_ptr<Constant>& e) final {}
	void on_expression(const std::shared_ptr<GetScanPos>& e) final {}
	void on_expression(const std::shared_ptr<Scan>& e) final {
		expr(e->pos);
	}
	void on_expression(const std::shared_ptr<MetaExpression>& e) final {
		expr(e->value);
	}

	void stmt(const Stmt& s) {
		auto p = s.get();
		if (p) {
			// common deps
			if (s->predicate) {
				expr(s->predicate);
			}

			// specific deps
			s->visit(s, *this);			
		}

		// visit self
		call_stmt(s);
	}

	void expr(const Expr& e) {
		auto p = e.get();
		if (p) {
			// common deps
			if (e->predicate) {
				expr(e->predicate);
			}

			// specific deps
			e->visit(e, *this);			
		}

		// visit self
		call_expr(e);
	}

public:
	// public interface
	TraverseProgram(const STMT_CALL_T& call_stmt,
		const EXPR_CALL_T& call_expr, const Stmt& s)
	 : call_stmt(call_stmt), call_expr(call_expr)
	{
		stmt(s);
	}

	TraverseProgram(const STMT_CALL_T& call_stmt,
		const EXPR_CALL_T& call_expr, const Expr& e)
	 : call_stmt(call_stmt), call_expr(call_expr)
	{
		expr(e);
	}

private:
	const STMT_CALL_T& call_stmt;
	const EXPR_CALL_T& call_expr;
};

template<typename STMT_CALL_T, typename EXPR_CALL_T, typename T>
static void traverse(const STMT_CALL_T& call_stmt,
	const EXPR_CALL_T& call_expr, const T& e)
{
	auto p(TraverseProgram<STMT_CALL_T, EXPR_CALL_T>(
		call_stmt, call_expr, e));
}

} /* voila */
} /* engine */
