#include "exception.hpp"
#include "error.pb.h"

std::string
Exception::get_stack_trace()
{
	return "";
}

Exception::Exception(int32_t code, const std::string& msg,
		const std::string& stack)
	 : code(code), msg(msg), stack(stack)
{
}

void
Exception::set(proto::ErrorMessage& error) const
{
	error.set_code(code);
	error.set_message(msg);
	error.set_stack_trace(stack);
}
