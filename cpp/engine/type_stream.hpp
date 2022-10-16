#pragma once

#include "voila/voila.hpp"
#include "stream_pass.hpp"

namespace engine {

struct Type;

struct TypeStream : StreamPass {
	using NodeInfo = voila::Context::NodeInfo;
	using NodeInfos = std::vector<NodeInfo>;
	using NodeInfoMap = std::unordered_map<voila::Node*, NodeInfos>;

	NodeInfoMap& infos;
	std::unordered_map<voila::Variable*, NodeInfos> variables;
	std::unordered_map<lolepop::Lolepop*, NodeInfos> lolepops;

	std::shared_ptr<lolepop::Lolepop> current_op;
	std::shared_ptr<lolepop::Lolepop> previous_op;

	TypeStream(Stream& s);


	void lolepop(std::shared_ptr<lolepop::Lolepop>& op) final;
};

} /* engine */
