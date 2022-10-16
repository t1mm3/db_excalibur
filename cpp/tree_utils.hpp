#pragma once

#include <functional>

namespace tree_utils {
	
template<typename T>
struct TreeNode {
	virtual size_t num_children() = 0;
	virtual T get_children(size_t i) = 0;	
};

}