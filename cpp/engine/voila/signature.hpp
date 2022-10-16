#pragma once

#include <string>
#include <sstream>
#include <unordered_map>
#include <memory>

#include "system/system.hpp"

namespace engine {
namespace voila {

struct Node;
struct CompileRequest;

typedef std::string Signature;

/* Caches Expressions to accelerate Signature generation */
struct SignatureCache {
	void put(Node* ptr, const Signature& signature);

	bool get(Signature& output, Node* ptr) const;
	Signature get(Node* ptr) const;

	template<typename T>
	static Signature
	try_put(SignatureCache* cache, Node* ptr, const T& ctor)
	{
		Signature result;
		if (!cache || !cache->get(result, ptr)) {
			bool r = ctor(result);
			try_put_check_result(r);

			if (cache) {
				cache->put(ptr, result);
				LOG_DEBUG("SignatureCache: put %p", ptr);
			}
		}
		return result;
	}

	static void try_put_check_result(bool result);

private:
	std::unordered_map<Node*, Signature> signatures;
};

/* Generates Signature for a CompileRequest */
struct SignatureGen {
	SignatureGen(CompileRequest& req) : req(req) {}

	void operator()();

	std::string get() const { return s.str(); }

private:
	std::ostringstream s;
	CompileRequest& req;
};

} /* voila */
} /* engine */
