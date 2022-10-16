#include "plan.hpp"
#include "system/system.hpp"
#include <unordered_map>
#include <functional>

#if 0
#include <boost/algorithm/string.hpp>

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/integer.hpp>

// Include the analyze function that checks
// a grammar for possible infinite cycles.

#include <tao/pegtl/contrib/analyze.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace plan_parser {

typedef std::shared_ptr<plan::Node> NodePtr;
typedef std::vector<NodePtr> NodePtrList;

struct State {
	NodePtrList nodes;
};

static std::unordered_map<std::string,
	std::function<void(State& state)>> g_function_actions;

// clang-format off

struct comma : pegtl::one<','> {};
struct lbrace : pegtl::one<'('> {};
struct rbrace : pegtl::one<')'> {};

struct llistbrace : pegtl::one<'['> {};
struct rlistbrace : pegtl::one<']'> {};


struct ident : pegtl::identifier {};
struct number : pegtl::plus<tao::pegtl::digit> {};
struct string : pegtl::seq<pegtl::one<'"'>, pegtl::any, pegtl::one<'"'>> {};

struct expression_list;

// pegtl::star<pegtl::blank>, 
struct function :
	pegtl::seq<ident, lbrace, expression_list, rbrace> {};
struct expression : pegtl::sor<ident, number, string, function> {};
struct expression_list : pegtl::list<expression, pegtl::one<','>> {};

struct relop;

struct opexpression_list : pegtl::seq<llistbrace,
	expression_list, rlistbrace> {};

struct scan : pegtl::seq<pegtl::istring<'s','c','a','n'>,
	lbrace, ident, comma, opexpression_list, rbrace> {};
struct select : pegtl::seq<pegtl::istring<'s','e','l','e','c','t'>,
	lbrace, relop, comma, opexpression_list, rbrace> {};
struct relop : pegtl::sor<scan, select> {};

struct grammar : pegtl::until< pegtl::eof, pegtl::must<relop> > {};

template< typename Rule >
struct action
{};

template<>
struct action< number >
{
	template< typename ActionInput >
	static void apply( const ActionInput& in, State& state )
	{
		std::stoll(in.string());
		state.nodes.emplace_back(std::make_shared<plan::Constant>(in.string()));
	}
};

template<>
struct action< ident >
{
	template< typename ActionInput >
	static void apply( const ActionInput& in, State& state )
	{
		state.nodes.emplace_back(std::make_shared<plan::ColumnId>(in.string()));
		LOG_DEBUG("parse ident: %s", in.string().c_str());
	}
};

template<>
struct action< string >
{
	template< typename ActionInput >
	static void apply( const ActionInput& in, State& state )
	{
		LOG_DEBUG("parse string: %s", in.string().c_str());
		state.nodes.emplace_back(std::make_shared<plan::Constant>(in.string()));
	}
};

template<>
struct action< function >
{
	template< typename ActionInput >
	static void apply( const ActionInput& in, State& st )
	{
		ASSERT(st.nodes.size() > 0);
		auto name = std::dynamic_pointer_cast<plan::ColumnId>(st.nodes[0]);
		ASSERT(name);
		LOG_DEBUG("parse: function %s", name->name.c_str());

		auto fname = boost::algorithm::to_lower_copy(name->name);

#if 0
		auto it = g_function_actions.find(fname);
		if (it == g_function_actions.end()) {
			throw pegtl::parse_error("invalid function or operator " + name->name,
				in);
		}

		it->second(st);
#else
#endif
	}
};

template<typename T, typename S, typename F>
std::vector<std::shared_ptr<T>>
try_cast(bool& fail, const std::vector<S>& s, const F& fun)
{
	fail = false;
	std::vector<std::shared_ptr<T>> result;
	result.reserve(s.size());

	for (auto& i : s) {
		auto val = fun(i);
		if (!val) {
			fail = true;
		}
		result.emplace_back(val);
	}
	return result;
}

template<typename T, typename S>
std::vector<std::shared_ptr<T>>
try_cast(bool& fail, const std::vector<S>& s)
{
	return try_cast(fail, s, [&] (auto i) {
		return std::dynamic_pointer_cast<T>(i);
	});
}


void install()
{
	if (g_function_actions.empty()) {
		g_function_actions.insert({"scan", [] (auto& state) {
			std::vector<std::string> columns;
			columns.reserve(state.nodes.size());

			bool first = true;
			std::string table;

			for (auto& node : state.nodes) {
				auto constant = std::dynamic_pointer_cast<plan::Constant>(node);
				auto column_id = std::dynamic_pointer_cast<plan::ColumnId>(node);
				ASSERT((column_id || constant.get()) && "Must be Constant or ColumnId");

				auto& str_val = constant ? constant->name : column_id->name;
				if (first) {
					table = str_val;
					first = false;
				} else {
					columns.push_back(str_val);
				}

			}

			state.nodes = NodePtrList {
				std::make_shared<plan::Scan>(table, std::move(columns))
			};
		}});

		g_function_actions.insert({"select", [] (auto& state) {
			ASSERT(state.nodes.size() >= 2);

			auto child = std::dynamic_pointer_cast<plan::RelOp>(state.nodes[0]);
			auto cond = std::dynamic_pointer_cast<plan::Expr>(state.nodes[1]);

			ASSERT(child && cond && "Must have valid shape");

			state.nodes = NodePtrList {
				std::make_shared<plan::Select>(child, std::move(cond))
			};
		}});
	}
}

} /* plan_parser */

namespace plan {

std::shared_ptr<plan::Node>
parse_plan(const std::string& text)
{
	plan_parser::install();	

	ASSERT(pegtl::analyze<plan_parser::grammar>() == 0);
	pegtl::string_input input(text, "test");

	plan_parser::State state;
	pegtl::parse<plan_parser::grammar, plan_parser::action>(input, state);

	return nullptr;
	ASSERT(state.nodes.size() == 1 && state.nodes[0].get());
	return state.nodes[0];
}

} /* plan */
#endif