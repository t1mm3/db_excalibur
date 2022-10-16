#include "stream_pass.hpp"
#include "system/system.hpp"
#include "stream.hpp"

using namespace engine;

#if 0
bool
StreamPass::run()
{
#if 0
	LOG_DEBUG("%sRound %lld", log_prefix(), round);
	for (auto& op : stream.chain) {
		lolepop(op);
	}

	LOG_DEBUG("%sApplied %lld actions", log_prefix(), num_applied);
	return !num_applied;
#else
	return false;
#endif
}

void 
StreamPass::operator()()
{
	round = 0;
	num_applied = 0;
	while (!run()) {
		round++;
		num_applied = 0;
	}
	LOG_DEBUG("%sFinished", log_prefix());
}
#endif