#pragma once

#include <memory>
#include <vector>

#include <string>
#include <unordered_map>

struct PrimitiveTable;
struct TypeSystem;
struct BlockManager;

namespace engine {
struct Lolepop;
struct Stream;
struct Catalog;

struct Engine {
	std::unique_ptr<PrimitiveTable> primitives;
	std::unique_ptr<TypeSystem> type_system;
	std::unique_ptr<BlockManager> block_manager;
	std::unique_ptr<Catalog> catalog;
};

} /* engine */

