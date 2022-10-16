#pragma once

#include "voila_lolepop.hpp"

namespace engine::relop {

struct HashGroupBy;
struct HashJoin;

struct HashOpRelOp : RelOp {
	HashOpRelOp(Query& q, const RelOpPtr& child);

	std::vector<lolepop::Expr> probe_keys;

	struct StreamState {
		voila::Var var_probe_check;
		voila::Var var_probe_bucket;

		virtual ~StreamState() = default;
	};
	std::unordered_map<size_t, std::unique_ptr<StreamState>> stream_state;

	template<typename S>
	S*
	get_stream_state_or_create(Stream* s)
	{
		size_t id = stream_id(*s);

		std::unique_lock lock(mutex);

		auto it = stream_state.find(id);
		if (it != stream_state.end()) {
			return dynamic_cast<S*>(it->second.get());
		}

		stream_state.insert({id, std::make_unique<S>() });

		it = stream_state.find(id);
		return dynamic_cast<S*>(it->second.get());
	}

	template<typename S>
	S*
	get_stream_state(Stream* s)
	{
		size_t id = stream_id(*s);

		std::unique_lock lock(mutex);

		auto it = stream_state.find(id);
		if (it != stream_state.end()) {
			return dynamic_cast<S*>(it->second.get());
		}
		return nullptr;
	}

private:
	static size_t stream_id(Stream& s);
};

} /* engine::relop */

namespace engine::lolepop {

struct HashOpLolepop {
protected:
	std::shared_ptr<relop::HashGroupBy> hash_group_by;
	std::shared_ptr<relop::HashJoin> hash_join;
	std::shared_ptr<relop::HashOpRelOp> hash_op;

	HashOpLolepop(const RelOpPtr& rel_op, Stream& stream);
};

struct HashOpCheck : VoilaLolepop, HashOpLolepop {
	VoilaCodegenContext codegen_produce(VoilaCodegenContext&& ctx) override;

	HashOpCheck(const std::string& name, const RelOpPtr& rel_op,
		Stream& stream, const std::shared_ptr<memory::Context>& mem_context,
		const LolepopPtr& child);

protected:
	virtual std::shared_ptr<voila::HashTable> get_voila_hash_table() = 0;

	bool fk1 = false;
};


} /* engine::lolepop */
