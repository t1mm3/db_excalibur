#pragma once

#include "voila_lolepop.hpp"
#include "hash_op.hpp"

namespace engine {
namespace voila {

struct HashTable;
struct Constant;

} /* voila */

namespace table {
struct IHashTable;
struct LogicalMasterTable;
} /* table */
} /* engine */

namespace engine::relop {

struct HashGroupBy : HashOpRelOp {
	std::vector<lolepop::Expr> aggregates;
	std::vector<lolepop::Expr> payloads;

	std::shared_ptr<engine::table::LogicalMasterTable> master_hash_table;

	struct GroupStreamState : HashOpRelOp::StreamState {
		std::shared_ptr<voila::HashTable> voila_hash_table;
		std::shared_ptr<engine::table::IHashTable> physical_hash_table;

		std::vector<std::string> key_col_names;
		std::vector<std::string> payload_col_names;
		std::vector<std::string> aggr_col_names;

		lolepop::Lolepop* last_build_phase_op = nullptr;
		std::unique_ptr<engine::voila::ReadPosition> read_position;

		std::shared_ptr<voila::Constant> global_aggr_bucket;

		int64_t count_aggr_col_id = -1;
		void* global_aggr_bucket_ptr = nullptr;
	};

	HashGroupBy(Query& q, const RelOpPtr& child, bool local_group_by,
		bool is_reaggregation, bool is_global_aggregation);
	~HashGroupBy();

	double get_max_tuples() const override;

	void dealloc_resources() override;

	const bool local_group_by;
	const bool is_reaggregation;
	const bool is_global_aggregation;
};

} /* engine::relop */


namespace engine::lolepop {

struct HashGroupByBuildOpHelper {
	HashGroupByBuildOpHelper(
		const std::shared_ptr<engine::relop::HashGroupBy>& hash_group_by,
		VoilaLolepop* op);

	bool is_last() const;

	void post_type_pass(BudgetUser* budget_user);

	~HashGroupByBuildOpHelper();

private:
	std::shared_ptr<engine::relop::HashGroupBy> hash_group_by;
	VoilaLolepop* op;
};

struct HashGroupByFindPos : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;
	void post_type_pass(BudgetUser* budget_user) final {
		build_op_helper->post_type_pass(budget_user);
	}

	HashGroupByFindPos(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child);

private:
	std::unique_ptr<HashGroupByBuildOpHelper> build_op_helper;

};

struct HashGroupByCheck : HashOpCheck {
	HashGroupByCheck(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child);

	std::shared_ptr<voila::HashTable> get_voila_hash_table() override;
};

struct HashGroupByAggregate : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;
	void post_type_pass(BudgetUser* budget_user) final {
		build_op_helper->post_type_pass(budget_user);
	}

	HashGroupByAggregate(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child);

private:
	std::unique_ptr<HashGroupByBuildOpHelper> build_op_helper;
};

struct HashGroupByPartition : Lolepop, HashOpLolepop {
	NextResult next(NextContext& context) final;

	HashGroupByPartition(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context);
};

struct HashGroupByPosition;

struct HashGroupByScan : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) final;

	HashGroupByScan(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context);
	~HashGroupByScan();
};

} /* engine::lolepop */
