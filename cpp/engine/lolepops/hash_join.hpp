#pragma once
#include "voila_lolepop.hpp"
#include "hash_op.hpp"

namespace engine {
namespace voila {

struct HashTable;

} /* voila */

namespace table {
struct IHashTable;
} /* table */

namespace relop {

struct HashJoin : HashOpRelOp {
	std::shared_ptr<voila::HashTable> voila_hash_table;
	std::shared_ptr<engine::table::IHashTable> physical_hash_table;

	bool fk1;

	std::vector<lolepop::Expr> build_keys;
	std::vector<lolepop::Expr> build_payloads;

	std::vector<lolepop::Expr> probe_payloads;

	std::shared_ptr<RelOp> build_relop;
	std::shared_ptr<RelOp> probe_relop;

	struct JoinStreamState : StreamState {

	};

	HashJoin(Query& q, const RelOpPtr& child);
	~HashJoin();

	void dealloc_resources() override;

	double get_max_tuples() const override;
};

} /* relop */


namespace lolepop {

struct HashJoinProbe : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) override;

	HashJoinProbe(size_t bloom_filter_bits, const RelOpPtr& rel_op,
		Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child);

	void post_type_pass(BudgetUser* budget_user) final;

	size_t estimate_hash_table_size(size_t& num_rows, size_t& num_buckets) const;

private:
	const size_t bloom_filter_bits;
};

struct HashJoinCheck : HashOpCheck {
	HashJoinCheck(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child);

	std::shared_ptr<voila::HashTable> get_voila_hash_table() override;
};

struct HashJoinGather : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) override;

	HashJoinGather(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child);
};

struct HashJoinWrite : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) override;

	HashJoinWrite(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context, const LolepopPtr& child);

	void post_type_pass(BudgetUser* budget_user) final;
};

struct HashJoinBuildTable : Lolepop, HashOpLolepop {
	NextResult next(NextContext& context) final;

	HashJoinBuildTable(const RelOpPtr& rel_op, Stream& stream,
		const std::shared_ptr<memory::Context>& mem_context);
};

} /* lolepop */

} /* engine */
