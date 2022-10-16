#pragma once

#include <ostream>
#include <cmath>

struct QueryConfig;

namespace engine {
namespace voila {

	// A, Name1, Name2, Type, Default, ShortName, InclMin, InclMax
#define FLAVOR_EXPAND(F, A) \
	F(A, UnrollNoSel, unroll_nosel, int, 1, nosel, 0, 32, true) \
	F(A, UnrollSel, unroll_sel, int, 8, sel, 1, 32, true) \
	F(A, Predicated, predicated, int, 0, pred, 0, 2, false) \
	F(A, RerollLoops, reroll_loops, bool, true, beo, 0, 1, false) \
	F(A, SimdizeNoSel, simdize_nosel, int, 0, nosel_simd, 0, 32, true) \
	F(A, SimdizeSel, simdize_sel, int, 1, sel_simd, 0, 32, true)

// Specifies a flavor for a code generated primitive
struct FlavorSpec {
	enum Entry {
		#define DECL(_1, NAME, _2, _3, _4, _5, _6, _7, _8) k##NAME,
		FLAVOR_EXPAND(DECL, error)
		#undef DECL

		__kMAX,
	};

	// Unroll nosel == 0, causes no nosel path to be generated
	// Makes selection vector creation, predicated
	// Predicated: Makes selection vector creation, predicated

	#define DECL(_1, _2, CNAME, TYPE, DEFAULT, _3, _4, _5, _6) TYPE CNAME = DEFAULT;
	FLAVOR_EXPAND(DECL, error)
	#undef DECL

	int get_int(Entry entry) const {
		switch (entry) {
		case kUnrollNoSel: return unroll_nosel;
		case kUnrollSel: return unroll_sel;
		case kSimdizeNoSel: return simdize_nosel;
		case kSimdizeSel: return simdize_sel;
		case kPredicated: return predicated;
		default: return 0;
		}
	}

	bool get_bool(Entry entry) const {
		switch (entry) {
		case kRerollLoops: return reroll_loops;
		default: return false;
		}
	}

	FlavorSpec(const QueryConfig* config);
	~FlavorSpec();

	FlavorSpec(
#define A(_1, _2, CNAME, TYPE, _4, _5, _6, _7, _8) TYPE _##CNAME,
		FLAVOR_EXPAND(A, error)
	bool _useless_extra_arg = false)
	{
#define B(_1, _2, CNAME, _3, _4, _5, _6, _7, _8) CNAME = _##CNAME;
			FLAVOR_EXPAND(B, error)
#undef B
#undef A
	}

	FlavorSpec(const FlavorSpec& s) {
#define A(_1, _2, CNAME, _3, _4, _5, _6, _7, _8) CNAME = s.CNAME;
			FLAVOR_EXPAND(A, error)
#undef A
	}

	void generate_signature(std::ostream& o) const;
	std::string generate_signature() const;

	bool equals(const FlavorSpec& o) const {
		return true
#define A(_1, _2, CNAME, _3, _4, _5, _6, _7, _8) && o.CNAME == CNAME
			FLAVOR_EXPAND(A, error)
#undef A
		;
	}

	int compare(const FlavorSpec& o) const {
#define A(_1, _2, CNAME, _3, _4, _5, _6, _7, _8) \
		if (o.CNAME < CNAME) return -1; \
		if (o.CNAME > CNAME) return 1;

		FLAVOR_EXPAND(A, error)
#undef A

		return 0;
	}

	bool operator==(const FlavorSpec& o) const { return equals(o); }
	bool operator!=(const FlavorSpec& o) const { return !equals(o); }

	bool operator<(const FlavorSpec& o) const { return compare(o) < 0; }
	bool operator<=(const FlavorSpec& o) const { return compare(o) <= 0; }

	bool operator>(const FlavorSpec& o) const { return compare(o) > 0; }
	bool operator>=(const FlavorSpec& o) const { return compare(o) >= 0; }

	static FlavorSpec from_string(const std::string& spec);
	std::string to_string() const;

	static FlavorSpec get_data_centric_flavor(int predicated = 0) {
		return FlavorSpec(0, 1, false, predicated, 1, 1);
	}

	static FlavorSpec get_reasonably_fast_flavor(bool full_eval = true,
			int predicated = 0) {
		return FlavorSpec(1, 1, full_eval, predicated, 1, 1);
	}

	static constexpr bool
	get_range_info(int& min, int& max, Entry entry)
	{
		switch (entry) {
#define A(_1, NAME, _2, _3, _4, _5, MIN, MAX, HAS_RANGE) \
			case k##NAME: \
				if (!HAS_RANGE) return false; \
				min = MIN; max = MAX; \
				return true;

		FLAVOR_EXPAND(A, error)
#undef A
		default: break;
		}

		return false;
	}

	static constexpr size_t gowers_distance_count() {
		size_t count = 0;
#define A(_1, _2, CNAME, _3, _4, _5, MIN, MAX, _6) {\
			count++; \
		}

		FLAVOR_EXPAND(A, error)
#undef A

		return count;
	}

	double gowers_distance_diff(const FlavorSpec& o) const {
		double sum = .0;
#define A(_1, _2, CNAME, _3, _4, _5, MIN, MAX, HAS_RANGE) {\
			if (HAS_RANGE) { \
				const double range = MAX - MIN + 1; \
				double diff = std::abs((double)(o.CNAME - CNAME)); \
				sum += diff/range; \
			} else { \
				if (o.CNAME != CNAME) sum += 1.0; \
			} \
		}

		FLAVOR_EXPAND(A, error)
#undef A

		return sum;
	}

private:
	void to_string(std::ostream& o, bool short_name = false) const;
};

} /* voila */
} /* engine */

namespace std {
	template <>
	struct hash<engine::voila::FlavorSpec>
	{
		std::size_t operator()(const engine::voila::FlavorSpec& k) const
		{
			using std::size_t;

			size_t seed = 13;

#define A(_1, _2, CNAME, TYPE, _4, _5, _6, _7, _8) seed ^= hash<TYPE>()(k.CNAME) + 0x9e3779b9 + (seed<<6) + (seed>>2);;
			FLAVOR_EXPAND(A, error)
#undef A

			return seed;
		}
	};

}
