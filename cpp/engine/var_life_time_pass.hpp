#pragma once

#include "stream_pass.hpp"

namespace engine {

struct VarLifeTimePass : StreamPass {
	VarLifeTimePass(Stream& stream) : StreamPass(stream, "VarLifeTimePass") {}

	void lolepop(std::shared_ptr<lolepop::Lolepop>& op) final;
};

} /* engine */