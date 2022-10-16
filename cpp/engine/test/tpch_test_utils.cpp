#include "tpch_test_utils.hpp"
#include "system/system.hpp"

#include <sys/stat.h>
#include <cstdlib>
#include <map>

static bool
does_path_exist(const std::string &s)
{
	struct stat buffer;
	return (stat(s.c_str(), &buffer) == 0);
}

TpchUtils::TableFileMap
TpchUtils::get_files(const std::string& sf)
{
	TableFileMap r;

	TableFileMap files = _files();
	std::ostringstream prefix;
	prefix << get_prefix_directory() << "/tpch_sf_" << sf << "/";

	if (!does_path_exist(prefix.str())) {
		std::ostringstream command;
		command << " DIRECTORY=" << prefix.str() << " SF=" << sf << " " << get_generator_script();

		LOG_INFO("Generating TPC-H in %s", prefix.str().c_str());
		LOG_DEBUG("Run command '%s'", command.str().c_str());
		int r = std::system(command.str().c_str());
		ASSERT(!r);
	}

	for (auto& key : files) {
		r.insert({key.first, prefix.str() + key.second});
	}

	return r;
}


#include "engine/plan.hpp"
#include "engine/catalog.hpp"
#include "engine/sql_types.hpp"

#include "system/system.hpp"
#include "engine/utils.hpp"
#include "test_utils.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <sstream>

using namespace engine;
using namespace catalog;

struct SchemaEntry {
	std::string col;
	std::string tpe;

	SchemaEntry(const std::string& sql) {
		auto s = StringUtils::split(sql, ' ');
		ASSERT(s.size() == 2);
		col = s[0];
		tpe = s[1];
	}
};

static void
create_table(Catalog& catalog, std::string name,
	const std::vector<SchemaEntry>& schema)
{
	auto table = std::make_unique<FlatArrayTable>();

	for (auto& entry : schema) {
		table->add_column(
			std::make_shared<AppendableFlatArrayColumn>(
				engine::SqlType::from_string(entry.tpe)),
			entry.col);
	}

	catalog.add_table(name, std::move(table));
}

static void
create_table(Catalog& cat, std::string name,
	const std::vector<std::string>& str_schema)
{
	std::vector<SchemaEntry> schema;
	for (auto& s : str_schema) {
		schema.emplace_back(SchemaEntry(s));
	}

	return create_table(cat, name, std::move(schema));
}


void
TpchUtils::create_schema(Catalog& cat, bool compact, const std::string& sf)
{
	(void)sf;

	bool compact_small_scale = false;

	if (compact) {

		double f_sf = std::stod(sf);
		ASSERT(f_sf >= 0.0);

		compact_small_scale = f_sf < 200.0;
		LOG_ERROR("compact data types with sf='%s' (%f), compact_small_scale=%d",
			sf.c_str(), f_sf, (int)compact_small_scale);
		ASSERT(!sf.empty() && "must have scale factor");
	}


	create_table(cat, "NATION", {
		"N_NATIONKEY INTEGER",
		"N_NAME CHAR(25)",
		"N_REGIONKEY INTEGER",
		"N_COMMENT VARCHAR(152)"
	});

	create_table(cat, "REGION", {
		"R_REGIONKEY INTEGER",
		"R_NAME CHAR(25)",
		"R_COMMENT VARCHAR(152)"
	});

	create_table(cat, "PART", {
		"P_PARTKEY INTEGER",
		"P_NAME VARCHAR(55)",
		"P_MFGR CHAR(25)",
		"P_BRAND CHAR(10)",
		"P_TYPE VARCHAR(25)",
		"P_SIZE INTEGER",
		"P_CONTAINER CHAR(10)",
		"P_RETAILPRICE DECIMAL(15,2)",
		"P_COMMENT VARCHAR(23)"
	});

	create_table(cat, "SUPPLIER", {
		"S_SUPPKEY INTEGER",
		"S_NAME CHAR(25)",
		"S_ADDRESS VARCHAR(40)",
		"S_NATIONKEY INTEGER",
		"S_PHONE CHAR(15)",
		"S_ACCTBAL DECIMAL(15,2)",
		"S_COMMENT VARCHAR(101)"
	});

	create_table(cat, "PARTSUPP", {
		"PS_PARTKEY INTEGER",
		"PS_SUPPKEY INTEGER",
		"PS_AVAILQTY INTEGER",
		"PS_SUPPLYCOST DECIMAL(15,2)",
		"PS_COMMENT VARCHAR(199)"
	});

	create_table(cat, "CUSTOMER", {
		"C_CUSTKEY INTEGER",
		"C_NAME VARCHAR(25)",
		"C_ADDRESS VARCHAR(40)",
		"C_NATIONKEY INTEGER",
		"C_PHONE CHAR(15)",
		"C_ACCTBAL DECIMAL(15,2)",
		"C_MKTSEGMENT CHAR(10)",
		"C_COMMENT VARCHAR(117)"
	});

	create_table(cat, "ORDERS", {
		compact_small_scale ? "O_ORDERKEY INTEGER" : "O_ORDERKEY BIGINT",
		"O_CUSTKEY INTEGER",
		"O_ORDERSTATUS CHAR(1)",
		"O_TOTALPRICE DECIMAL(15,2)",
		"O_ORDERDATE DATE",
		"O_ORDERPRIORITY CHAR(15)",
		"O_CLERK CHAR(15)",
		"O_SHIPPRIORITY INTEGER",
		"O_COMMENT VARCHAR(79)"
	});

	create_table(cat, "LINEITEM", {
		compact_small_scale ? "L_ORDERKEY INTEGER" : "L_ORDERKEY BIGINT",
		"L_PARTKEY INTEGER",
		"L_SUPPKEY INTEGER",
		"L_LINENUMBER INTEGER",
		compact ? "L_QUANTITY DECIMAL(4,2)" : "L_QUANTITY DECIMAL(15,2)",
		compact ? "L_EXTENDEDPRICE DECIMAL(8,2)" : "L_EXTENDEDPRICE DECIMAL(15,2)",
		compact ? "L_DISCOUNT DECIMAL(2,2)" : "L_DISCOUNT DECIMAL(15,2)",
		compact ? "L_TAX DECIMAL(2,2)" : "L_TAX DECIMAL(15,2)",
		"L_RETURNFLAG CHAR(1)",
		"L_LINESTATUS CHAR(1)",
		"L_SHIPDATE DATE",
		"L_COMMITDATE DATE",
		"L_RECEIPTDATE DATE",
		"L_SHIPINSTRUCT CHAR(25)",
		"L_SHIPMODE CHAR(10)",
		"L_COMMENT VARCHAR(44)"
	});

}

using namespace plan;

typedef std::vector<ExprPtr> ExprPtrVec;

RelOpPtr
TpchUtils::get_q1(const std::string& shipdate_str)
{
	Builder builder;

	auto scan = builder.scan("lineitem", {
		"l_shipdate", "l_returnflag", "l_linestatus",
		"l_extendedprice", "l_quantity", "l_discount",
		"l_tax"
	});

	auto sql_date = SqlType::from_string("date");
	auto shipdate = sql_date->internal_parse_into_string(shipdate_str);
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	auto select = builder.select(scan,
		builder.function("<=", builder.column("l_shipdate"),
			builder.constant(shipdate)));

	auto project = builder.project(select, ExprPtrVec {
		builder.assign("_TRSDM_6",
			builder.function("-", builder.constant(one), builder.column("l_discount"))),
		builder.assign("_TRSDM_7",
			builder.function("*", builder.column("_TRSDM_6"), builder.column("l_extendedprice"))),
		builder.assign("_TRSDM_8",
			builder.function("*",
				builder.function("*",
					builder.function("+",
						builder.constant(one),
						builder.column("l_tax")
					),
					builder.column("_TRSDM_6")
				),
				builder.column("l_extendedprice")
		)),
		builder.column("l_quantity"),
		builder.column("l_discount"),
		builder.column("l_extendedprice"),
		builder.column("l_returnflag"),
		builder.column("l_linestatus")
	});

	auto aggr = builder.hash_group_by(project,
		std::vector<ExprPtr> {builder.column("l_returnflag"), builder.column("l_linestatus")},
		std::vector<ExprPtr> {
			builder.assign("count", builder.function("count", builder.column("l_quantity"))),
			builder.assign("sum_1", builder.function("sum", builder.column("l_quantity"))),
			builder.assign("sum_2", builder.function("sum", builder.column("l_extendedprice"))),
			builder.assign("sum_3", builder.function("sum", builder.column("_TRSDM_7"))),
			builder.assign("sum_4", builder.function("sum", builder.column("_TRSDM_8"))),
			builder.assign("sum_5", builder.function("sum", builder.column("l_discount")))
		}
	);

	return aggr;
}

static RelOpPtr
get_q3()
{
	Builder builder;

	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1995-03-15");
	auto c2 = sql_date->internal_parse_into_string("1995-03-15");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	RelOpPtr customer = builder.scan("customer", {
		"c_mktsegment", "c_custkey"
	});

	customer = builder.select(customer,
		builder.function("=",
			builder.column("c_mktsegment"),
			builder.constant("BUILDING")
	));

	RelOpPtr orders = builder.scan("orders", {
		"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"
	});

	orders = builder.select(orders,
		builder.function("<",
			builder.column("o_orderdate"),
			builder.constant(c2)
	));

	RelOpPtr customerorders = builder.hash_join(
		customer,
		ExprPtrVec { builder.column("c_custkey") },
		ExprPtrVec { },

		orders,
		ExprPtrVec { builder.column("o_custkey") },
		ExprPtrVec { builder.column("o_orderdate"),
			builder.column("o_shippriority"), builder.column("o_orderkey") },

		true
	);

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"
	});

	lineitem = builder.select(lineitem,
		builder.function(">",
			builder.column("l_shipdate"),
			builder.constant(c2)
	));

	RelOpPtr lineitemcustomerorders = builder.hash_join(
		customerorders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderdate"), builder.column("o_shippriority") },

		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_discount") },

		true
	);

	RelOpPtr project = builder.project(lineitemcustomerorders, ExprPtrVec {
		builder.assign("revenue",
			builder.function("*",
				builder.column("l_extendedprice"),
				(builder.function("-", builder.constant(one), builder.column("l_discount")))
			)
		),
		builder.column("l_orderkey"),
		builder.column("o_orderdate"),
		builder.column("o_shippriority"),
	});

	RelOpPtr aggr = builder.hash_group_by(project,
		ExprPtrVec {
			builder.column("l_orderkey"),
			builder.column("o_orderdate"),
			builder.column("o_shippriority")
		},
		ExprPtrVec {
			builder.assign("sum", builder.function("sum", builder.column("revenue"))
		),
		});

	return aggr;
}


static RelOpPtr
get_q4()
{
	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_commitdate", "l_receiptdate"
	});

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_commitdate"), builder.column("l_receiptdate")));


	RelOpPtr orders = builder.scan("orders", {
		"o_orderkey", "o_orderdate", "o_orderpriority"
	});

	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1993-10-01");
	auto c2 = sql_date->internal_parse_into_string("1993-07-01");

	orders = builder.select(orders, builder.function("<",
		builder.column("o_orderdate"), builder.constant(c1)));

	orders = builder.select(orders, builder.function(">=",
		builder.column("o_orderdate"), builder.constant(c2)));


	RelOpPtr join = builder.hash_join(
		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec {  },

		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderpriority") },

		true
	);

	RelOpPtr aggr = builder.hash_group_by(join,
		ExprPtrVec {
			builder.column("o_orderpriority")
		},
		ExprPtrVec {
			builder.assign("count", builder.function("count", builder.column("o_orderkey"))),
		});

	return aggr;
}


RelOpPtr
TpchUtils::get_q6(int variant, int year, int discount, const std::string& _quantity)
{
	Builder builder;

	int discount_c = 1;

	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string(
		std::to_string(year) + std::string("-01-01"));
	auto c2 = sql_date->internal_parse_into_string(
		std::to_string(year+1) + std::string("-01-01"));


	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto c3 = sql_decimal->internal_parse_into_string(std::string("0.0") + std::to_string(discount - discount_c));

	ASSERT(discount > 0 && discount <= 9);
	auto c4 = discount == 9 ?
		sql_decimal->internal_parse_into_string(std::string("0.10")) :
		sql_decimal->internal_parse_into_string(std::string("0.0") + std::to_string(discount + discount_c));
	auto c5 = sql_decimal->internal_parse_into_string(_quantity);

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_shipdate", "l_quantity", "l_extendedprice", "l_discount"
	});

	bool between = true;

	if (variant < 0) {
		auto range_shipdate = builder.function("between<",
			{ builder.column("l_shipdate"),
				builder.constant(c1), builder.constant(c2) });

		auto range_discount = builder.function("between",
			{ builder.column("l_discount"),
				builder.constant(c3), builder.constant(c4) });

		auto lt_quantity = builder.function("<",
			builder.column("l_quantity"), builder.constant(c5));

		lineitem = builder.select(lineitem, range_shipdate);
		lineitem = builder.select(lineitem, range_discount);
		lineitem = builder.select(lineitem, lt_quantity);
	} else {
		auto ge_shipdate = builder.function(">=",
			builder.column("l_shipdate"), builder.constant(c1));

		auto lt_shipdate = builder.function("<",
			builder.column("l_shipdate"), builder.constant(c2));

		auto ge_discount = builder.function(">=",
			builder.column("l_discount"), builder.constant(c3));

		auto le_discount = builder.function("<=",
			builder.column("l_discount"), builder.constant(c4));

		auto lt_quantity = builder.function("<",
			builder.column("l_quantity"), builder.constant(c5));

		switch (variant) {
		case 0:
			lineitem = builder.select(lineitem, lt_shipdate);
			lineitem = builder.select(lineitem, ge_shipdate);
			lineitem = builder.select(lineitem, lt_quantity);
			lineitem = builder.select(lineitem, ge_discount);
			lineitem = builder.select(lineitem, le_discount);
			break;

		case 1:
			lineitem = builder.select(lineitem, lt_shipdate);
			lineitem = builder.select(lineitem, ge_discount);
			lineitem = builder.select(lineitem, lt_quantity);
			lineitem = builder.select(lineitem, ge_shipdate);
			lineitem = builder.select(lineitem, le_discount);
			break;

		case 2:
			lineitem = builder.select(lineitem, lt_quantity);
			lineitem = builder.select(lineitem, le_discount);
			lineitem = builder.select(lineitem, ge_shipdate);
			lineitem = builder.select(lineitem, ge_discount);
			lineitem = builder.select(lineitem, lt_shipdate);
			break;

		case 3:
			lineitem = builder.select(lineitem, ge_discount);
			lineitem = builder.select(lineitem, lt_quantity);
			lineitem = builder.select(lineitem, le_discount);
			lineitem = builder.select(lineitem, ge_shipdate);
			lineitem = builder.select(lineitem, lt_shipdate);
			break;

		case 4:
			{
				auto a = builder.function("&&",
					lt_shipdate, ge_shipdate);

				auto b = builder.function("&&",
					ge_discount, le_discount);

				auto c = builder.function("&&",
					a, lt_quantity);
				c = builder.function("&&",
					c, b);
				lineitem = builder.select(lineitem, c);
			}
			break;

		default:
			LOG_ERROR("Invalid Q6 variant %d", variant);
			ASSERT(false);
			break;
		}
	}

	RelOpPtr project = builder.project(lineitem, ExprPtrVec {
		builder.assign("revenue",
			builder.function("*",
				builder.column("l_extendedprice"), builder.column("l_discount")
			)
		)
	});


	RelOpPtr aggr = builder.hash_group_by(project,
		ExprPtrVec {},
		ExprPtrVec {
			builder.assign("sum", builder.function("sum", builder.column("revenue"))),
		});

	return aggr;
}

static RelOpPtr
get_q9()
{
	Builder builder;

	RelOpPtr nation = builder.scan("nation", {
		"n_nationkey", "n_name"
	});

	RelOpPtr supplier = builder.scan("supplier", {
		"s_nationkey", "s_suppkey"
	});

	RelOpPtr nationsupplier = builder.hash_join(
		nation,
		ExprPtrVec { builder.column("n_nationkey") },
		ExprPtrVec { builder.column("n_name") },

		supplier,
		ExprPtrVec { builder.column("s_nationkey") },
		ExprPtrVec { builder.column("s_suppkey") },

		true
	);


	RelOpPtr part = builder.scan("part", {
		"p_partkey", "p_name"
	});

	part = builder.select(part,
		builder.function("contains",
			builder.column("p_name"),
			builder.constant("green")
	));

	RelOpPtr partsupp = builder.scan("partsupp", {
		"ps_partkey", "ps_suppkey", "ps_supplycost"
	});

	RelOpPtr partpartsupp = builder.hash_join(
		part,
		ExprPtrVec { builder.column("p_partkey") },
		ExprPtrVec { },

		partsupp,
		ExprPtrVec { builder.column("ps_partkey") },
		ExprPtrVec { builder.column("ps_suppkey"), builder.column("ps_supplycost") },

		true
	);

	RelOpPtr nspps = builder.hash_join(
		nationsupplier,
		ExprPtrVec { builder.column("s_suppkey") },
		ExprPtrVec { builder.column("n_name") },

		partpartsupp,
		ExprPtrVec { builder.column("ps_suppkey") },
		ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_supplycost") },

		true
	);

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"
	});

	RelOpPtr lineitem_nspps = builder.hash_join(
		nspps,
		ExprPtrVec { builder.column("ps_partkey"), builder.column("ps_suppkey") },
		ExprPtrVec { builder.column("n_name"), builder.column("ps_supplycost") },

		lineitem,
		ExprPtrVec { builder.column("l_partkey"), builder.column("l_suppkey") },
		ExprPtrVec { builder.column("l_orderkey"), builder.column("l_quantity"),
		builder.column("l_extendedprice"), builder.column("l_discount") },

		true
	);

	RelOpPtr orders = builder.scan("orders", {
		"o_orderkey", "o_orderdate"
	});

	RelOpPtr lineitem_orders = builder.hash_join(
		lineitem_nspps,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_extendedprice"), builder.column("l_quantity"),
		builder.column("l_discount"), builder.column("ps_supplycost"),
		builder.column("n_name") },

		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderdate") },

		false
	);

	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	auto a = builder.function("-", builder.constant(one), builder.column("l_discount"));
	auto b = builder.column("l_extendedprice");

	auto c = builder.column("ps_supplycost");
	auto d = builder.column("l_quantity");

	RelOpPtr project = builder.project(lineitem_orders, ExprPtrVec {
		builder.assign("amount",
			builder.function("-",
				builder.function("*", a, b),
				builder.function("*", c, d))
		),
		builder.assign("o_year",
			builder.function("extract_year", builder.column("o_orderdate"))
		),
		builder.column("n_name"),
	});

	auto aggr = builder.hash_group_by(project,
		std::vector<ExprPtr> {builder.column("n_name"), builder.column("o_year")},
		std::vector<ExprPtr> {
			builder.assign("sum", builder.function("sum", builder.column("amount"))),
		}
	);

	return aggr;
}


static RelOpPtr
get_q10()
{
	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1994-01-01");
	auto c2 = sql_date->internal_parse_into_string("1993-10-01");
	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto one = sql_decimal->internal_parse_into_string("1.00");

	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_returnflag", "l_discount", "l_extendedprice"
	});

	lineitem = builder.select(lineitem, builder.function("=",
		builder.column("l_returnflag"), builder.constant("82")));

	RelOpPtr orders = builder.scan("orders", {
		"o_custkey", "o_orderkey", "o_orderdate"
	});

	orders = builder.select(orders, builder.function("<",
		builder.column("o_orderdate"), builder.constant(c1)));
	orders = builder.select(orders, builder.function(">=",
		builder.column("o_orderdate"), builder.constant(c2)));

	RelOpPtr join = builder.hash_join(
		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_custkey") },

		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice") },

		true
	);

	RelOpPtr customer = builder.scan("customer", {
		"c_custkey", "c_nationkey", "c_name", "c_acctbal",
		"c_phone", "c_address", "c_comment"
	});

	RelOpPtr join2 = builder.hash_join(
		customer,
		ExprPtrVec { builder.column("c_custkey") },
		ExprPtrVec { builder.column("c_nationkey"), builder.column("c_name"),
			builder.column("c_acctbal"), builder.column("c_phone"),
			builder.column("c_address"), builder.column("c_comment") },

		join,
		ExprPtrVec { builder.column("o_custkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice") },

		true
	);

	RelOpPtr nation = builder.scan("nation", { "n_nationkey", "n_name" });

	RelOpPtr join3 = builder.hash_join(
		nation,
		ExprPtrVec { builder.column("n_nationkey") },
		ExprPtrVec { builder.column("n_name") },

		join2,
		ExprPtrVec { builder.column("c_nationkey") },
		ExprPtrVec { builder.column("l_discount"), builder.column("l_extendedprice"),
			builder.column("c_name"), builder.column("c_acctbal"),
			builder.column("c_phone"), builder.column("c_address"),
			builder.column("c_comment") },

		true
	);

	RelOpPtr project = builder.project(join3, ExprPtrVec {
		builder.assign("_TRSDM_1",
			builder.function("*",
				builder.function("-",
					builder.constant(one), builder.column("l_discount")),
				builder.column("l_extendedprice")
			)
		),
		builder.column("c_comment"),
		builder.column("c_address"),
		builder.column("n_name"),
		builder.column("c_phone"),
		builder.column("c_acctbal"),
		builder.column("c_name"),
		builder.column("c_custkey")
	});

	RelOpPtr groupby = builder.hash_group_by(project,
		ExprPtrVec {
			builder.column("c_custkey"),
		},
		ExprPtrVec {
			builder.column("c_comment"),
			builder.column("c_address"),
			builder.column("n_name"),
			builder.column("c_phone"),
			builder.column("c_acctbal"),
			builder.column("c_name"),
		},
		ExprPtrVec {
			builder.assign("_revenue_3",
				builder.function("sum", builder.column("_TRSDM_1"))),
		}
	);

	RelOpPtr rproject = builder.project(groupby, ExprPtrVec {
		builder.column("c_custkey"),
		builder.column("c_name"),
		builder.column("_revenue_3"),
		builder.column("c_acctbal"),
		builder.column("n_name"),
		builder.column("c_address"),
		builder.column("c_phone"),
		builder.column("c_comment"),
	});

	return rproject;
}


static RelOpPtr
get_q12()
{
	auto sql_date = SqlType::from_string("date");
	auto c1 = sql_date->internal_parse_into_string("1995-01-01");
	auto c2 = sql_date->internal_parse_into_string("1994-01-01");

	Builder builder;

	RelOpPtr lineitem = builder.scan("lineitem", {
		"l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"
	});

	lineitem = builder.select(lineitem, builder.function("|",
		builder.function("=", builder.column("l_shipmode"), builder.constant("MAIL")),
		builder.function("=", builder.column("l_shipmode"), builder.constant("SHIP"))
		));

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_receiptdate"), builder.constant(c1)));

	lineitem = builder.select(lineitem, builder.function(">=",
		builder.column("l_receiptdate"), builder.constant(c2)));

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_commitdate"), builder.column("l_receiptdate")));

	lineitem = builder.select(lineitem, builder.function("<",
		builder.column("l_shipdate"), builder.column("l_commitdate")));

	auto orders = builder.scan("orders", {
		"o_orderkey", "o_orderpriority"
	});

	RelOpPtr join = builder.hash_join(
		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_orderpriority") },

		lineitem,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { builder.column("l_shipmode") },

		true
	);

	auto project = builder.project(join, ExprPtrVec {
		builder.assign("_TRSDM_2",
			builder.function("ifthenelse",
				builder.function("&",
						builder.function("!=", builder.column("o_orderpriority"),
							builder.constant("2-HIGH")),
						builder.function("!=", builder.column("o_orderpriority"),
							builder.constant("1-URGENT"))),
				builder.constant("1"),
				builder.constant("0")
			)),
		builder.assign("_TRSDM_3",
			builder.function("ifthenelse",
				builder.function("|",
						builder.function("=", builder.column("o_orderpriority"),
							builder.constant("2-HIGH")),
						builder.function("=", builder.column("o_orderpriority"),
							builder.constant("1-URGENT"))),
				builder.constant("1"),
				builder.constant("0")
			)),
		builder.column("l_shipmode"),
	});

	auto aggr = builder.hash_group_by(project,
		ExprPtrVec { builder.column("l_shipmode") },
		ExprPtrVec {
			builder.assign("high_line_count",
				builder.function("sum", builder.column("_TRSDM_3"))),
			builder.assign("low_line_count",
				builder.function("sum", builder.column("_TRSDM_2"))),
		}
	);

	return aggr;
}


static RelOpPtr
get_q18()
{
	Builder builder;

	auto sql_decimal = SqlType::from_string("decimal(12,2)");
	auto three_hundred = sql_decimal->internal_parse_into_string("300.00");

	RelOpPtr lineitem1 = builder.scan("lineitem", { "l_orderkey", "l_quantity" });

	lineitem1 = builder.hash_group_by(lineitem1,
		std::vector<ExprPtr> {builder.column("l_orderkey")},
		std::vector<ExprPtr> {
			builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
		});

	lineitem1 = builder.select(lineitem1, builder.function(">",
		builder.column("sum_quantity"), builder.constant(three_hundred)));

	RelOpPtr orders = builder.scan("orders", {
		"o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"
	});

	RelOpPtr lineitem1_orders = builder.hash_join(
		lineitem1,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec { },

		orders,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_custkey"), builder.column("o_orderdate"),
			builder.column("o_totalprice") },

		false
	);


	RelOpPtr customer = builder.scan("customer", {
		"c_custkey", "c_name"
	});

	RelOpPtr custjoin = builder.hash_join(
		lineitem1_orders,
		ExprPtrVec { builder.column("o_custkey") },
		ExprPtrVec { builder.column("o_orderkey"), builder.column("o_orderdate"),
			builder.column("o_totalprice") },

		customer,
		ExprPtrVec { builder.column("c_custkey") },
		ExprPtrVec { builder.column("c_name") },

		false
	);

	RelOpPtr lineitem2 = builder.scan("lineitem", { "l_orderkey", "l_quantity" });

	RelOpPtr linejoin = builder.hash_join(
		custjoin,
		ExprPtrVec { builder.column("o_orderkey") },
		ExprPtrVec { builder.column("o_custkey"), builder.column("o_orderdate"),
			builder.column("o_totalprice"), builder.column("c_name") },

		lineitem2,
		ExprPtrVec { builder.column("l_orderkey") },
		ExprPtrVec {  },

		true
	);

	auto aggr = builder.hash_group_by(linejoin,
		std::vector<ExprPtr> {
			builder.column("c_name"),
			builder.column("o_custkey"),
			builder.column("o_orderdate"),
			builder.column("o_totalprice"),
		},
		std::vector<ExprPtr> {
			builder.assign("sum_quantity", builder.function("sum", builder.column("l_quantity"))),
		});

	return aggr;
}

static std::map<int, std::shared_ptr<plan::RelOp>>
available_queries()
{
	return {
		{9, get_q9()},
		{1, TpchUtils::get_q1()},
		{6, TpchUtils::get_q6()},
		{3, get_q3()},
		{4, get_q4()},
		{10, get_q10()},
		{12, get_q12()},
		{18, get_q18()},
	};
}



std::shared_ptr<plan::RelOp>
TpchUtils::get_query(int n)
{
	auto all = available_queries();

	auto it = all.find(n);
	if (it == all.end()) {
		LOG_ERROR("Invalid TPC-H query %d", n);
		return nullptr;
	}

	return it->second;
}

std::vector<int>
TpchUtils::get_all_queries()
{
	auto all = available_queries();
	std::vector<int> r;
	r.reserve(all.size());

	for (auto& kv : all) {
		r.push_back(kv.first);
	}
	return r;
}

std::vector<std::string>
TpchUtils::get_results(const std::string& sf, int q)
{
	std::ostringstream filename;
	filename << get_result_directory()
		<< "/results/tpch_q" << q << "_sf" << sf << ".txt";

	return TestUtils::get_results_from_file(filename.str());
}
