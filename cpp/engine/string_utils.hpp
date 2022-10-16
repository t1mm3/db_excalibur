#pragma once

#include <stdint.h>
#include <string>
#include "system/system.hpp"

#define MAYBE_UNUSED
#ifdef __SSE4_2__
#include <immintrin.h>
#endif

namespace engine::string_utils {
	namespace bits {

	    template <typename T>
	    inline static T clear_leftmost_set(const T value) {

	        DBG_ASSERT(value != 0);

	        return value & (value - 1);
	    }


	    template <typename T>
	    inline unsigned get_first_bit_set(const T value) {

	        DBG_ASSERT(value != 0);

	        return __builtin_ctz(value);
	    }


	    template <>
	    inline unsigned get_first_bit_set<uint64_t>(const uint64_t value) {

	        DBG_ASSERT(value != 0);

	        return __builtin_ctzl(value);
	    }

	} // namespace bits

    MAYBE_UNUSED
    inline static bool always_true(const char*, const char*) {
        return true;
    }

    MAYBE_UNUSED
    inline static bool memcmp1(const char* a, const char* b) {
        return a[0] == b[0];
    }

    MAYBE_UNUSED
    inline static bool memcmp2(const char* a, const char* b) {
        const uint16_t A = *reinterpret_cast<const uint16_t*>(a);
        const uint16_t B = *reinterpret_cast<const uint16_t*>(b);
        return A == B;
    }

    MAYBE_UNUSED
    inline static bool memcmp3(const char* a, const char* b) {

#ifdef USE_SIMPLE_MEMCMP
        return memcmp2(a, b) && memcmp1(a + 2, b + 2);
#else
        const uint32_t A = *reinterpret_cast<const uint32_t*>(a);
        const uint32_t B = *reinterpret_cast<const uint32_t*>(b);
        return (A & 0x00ffffff) == (B & 0x00ffffff);
#endif
    }

    MAYBE_UNUSED
    inline static bool memcmp4(const char* a, const char* b) {

        const uint32_t A = *reinterpret_cast<const uint32_t*>(a);
        const uint32_t B = *reinterpret_cast<const uint32_t*>(b);
        return A == B;
    }

    MAYBE_UNUSED
    inline static bool memcmp5(const char* a, const char* b) {

#ifdef USE_SIMPLE_MEMCMP
        return memcmp4(a, b) && memcmp1(a + 4, b + 4);
#else
        const uint64_t A = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t B = *reinterpret_cast<const uint64_t*>(b);
        return ((A ^ B) & 0x000000fffffffffflu) == 0;
#endif
    }

    MAYBE_UNUSED
    inline static bool memcmp6(const char* a, const char* b) {

#ifdef USE_SIMPLE_MEMCMP
        return memcmp4(a, b) && memcmp2(a + 4, b + 4);
#else
        const uint64_t A = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t B = *reinterpret_cast<const uint64_t*>(b);
        return ((A ^ B) & 0x0000fffffffffffflu) == 0;
#endif
    }

    MAYBE_UNUSED
    inline static bool memcmp7(const char* a, const char* b) {

#ifdef USE_SIMPLE_MEMCMP 
        return memcmp4(a, b) && memcmp3(a + 4, b + 4);
#else
        const uint64_t A = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t B = *reinterpret_cast<const uint64_t*>(b);
        return ((A ^ B) & 0x00fffffffffffffflu) == 0;
#endif
    }

    MAYBE_UNUSED
    inline static bool memcmp8(const char* a, const char* b) {

        const uint64_t A = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t B = *reinterpret_cast<const uint64_t*>(b);
        return A == B;
    }

    MAYBE_UNUSED
    inline static bool memcmp9(const char* a, const char* b) {

        const uint64_t A = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t B = *reinterpret_cast<const uint64_t*>(b);
        return (A == B) & (a[8] == b[8]);
    }

    MAYBE_UNUSED
    inline static bool memcmp10(const char* a, const char* b) {

        const uint64_t Aq = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t Bq = *reinterpret_cast<const uint64_t*>(b);
        const uint16_t Aw = *reinterpret_cast<const uint16_t*>(a + 8);
        const uint16_t Bw = *reinterpret_cast<const uint16_t*>(b + 8);
        return (Aq == Bq) & (Aw == Bw);
    }

    MAYBE_UNUSED
    inline static bool memcmp11(const char* a, const char* b) {

#ifdef USE_SIMPLE_MEMCMP
        return memcmp8(a, b) && memcmp3(a + 8, b + 8);
#else
        const uint64_t Aq = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t Bq = *reinterpret_cast<const uint64_t*>(b);
        const uint32_t Ad = *reinterpret_cast<const uint32_t*>(a + 8);
        const uint32_t Bd = *reinterpret_cast<const uint32_t*>(b + 8);
        return (Aq == Bq) & ((Ad & 0x00ffffff) == (Bd & 0x00ffffff));
#endif
    }

    MAYBE_UNUSED
    inline static bool memcmp12(const char* a, const char* b) {

        const uint64_t Aq = *reinterpret_cast<const uint64_t*>(a);
        const uint64_t Bq = *reinterpret_cast<const uint64_t*>(b);
        const uint32_t Ad = *reinterpret_cast<const uint32_t*>(a + 8);
        const uint32_t Bd = *reinterpret_cast<const uint32_t*>(b + 8);
        return (Aq == Bq) & (Ad == Bd);
    }

/* Usage of PCMPESTRM instruction from SSE 4.1 */
#ifdef __SSE4_2__
inline static size_t FORCE_INLINE sse42_strstr_anysize(const char* s, size_t n, const char* needle, size_t k) {

    DBG_ASSERT(k > 0);
    DBG_ASSERT(n > 0);

    const __m128i N = _mm_loadu_si128((__m128i*)needle);

    for (size_t i = 0; i < n; i += 16) {
    
        const int mode = _SIDD_UBYTE_OPS 
                       | _SIDD_CMP_EQUAL_ORDERED
                       | _SIDD_BIT_MASK;

        const __m128i D   = _mm_loadu_si128((__m128i*)(s + i));
        const __m128i res = _mm_cmpestrm(N, k, D, n - i, mode);
        uint64_t mask = _mm_cvtsi128_si64(res);

        while (mask != 0) {

            const auto bitpos = bits::get_first_bit_set(mask);

            // we know that at least the first character of needle matches
            if (memcmp(s + i + bitpos + 1, needle + 1, k - 1) == 0) {
                return i + bitpos;
            }

            mask = bits::clear_leftmost_set(mask);
        }
    }

    return std::string::npos;
}


template <size_t k, typename MEMCMP>
inline static size_t FORCE_INLINE sse42_strstr_memcmp(const char* s, size_t n, const char* needle, MEMCMP memcmp_fun) {

    DBG_ASSERT(k > 0);
    DBG_ASSERT(n > 0);

    const __m128i N = _mm_loadu_si128((__m128i*)needle);

    for (size_t i = 0; i < n; i += 16) {
    
        const int mode = _SIDD_UBYTE_OPS 
                       | _SIDD_CMP_EQUAL_ORDERED
                       | _SIDD_BIT_MASK;

        const __m128i D   = _mm_loadu_si128((__m128i*)(s + i));
        const __m128i res = _mm_cmpestrm(N, k, D, n - i, mode);
        uint64_t mask = _mm_cvtsi128_si64(res);

        while (mask != 0) {

            const auto bitpos = bits::get_first_bit_set(mask);

            if (memcmp_fun(s + i + bitpos + 1, needle + 1)) {
                return i + bitpos;
            }

            mask = bits::clear_leftmost_set(mask);
        }
    }

    return std::string::npos;
}

// ------------------------------------------------------------------------

inline static size_t sse42_strstr(const char* s, size_t n, const char* needle, size_t k) {

    size_t result = std::string::npos;

    if (n < k) {
        return result;
    }

	switch (k) {
		case 0:
			return 0;

		case 1: {
            const char* res = reinterpret_cast<const char*>(strchr(s, needle[0]));

			return (res != nullptr) ? res - s : std::string::npos;
            }

        case 2:
            result = sse42_strstr_memcmp<2>(s, n, needle, memcmp1);
            break;

        case 3:
            result = sse42_strstr_memcmp<3>(s, n, needle, memcmp2);
            break;

        case 4:
            result = sse42_strstr_memcmp<4>(s, n, needle, memcmp3);
            break;

        case 5:
            result = sse42_strstr_memcmp<5>(s, n, needle, memcmp4);
            break;

        case 6:
            result = sse42_strstr_memcmp<6>(s, n, needle, memcmp5);
            break;

        case 7:
            result = sse42_strstr_memcmp<7>(s, n, needle, memcmp6);
            break;

        case 8:
            result = sse42_strstr_memcmp<8>(s, n, needle, memcmp7);
            break;

        case 9:
            result = sse42_strstr_memcmp<9>(s, n, needle, memcmp8);
            break;

        case 10:
            result = sse42_strstr_memcmp<10>(s, n, needle, memcmp9);
            break;

        case 11:
            result = sse42_strstr_memcmp<11>(s, n, needle, memcmp10);
            break;

        case 12:
            result = sse42_strstr_memcmp<12>(s, n, needle, memcmp11);
            break;

		default:
			result = sse42_strstr_anysize(s, n, needle, k);
            break;
    }

    if (result <= n - k) {
        return result;
    } else {
        return std::string::npos;
    }
}
#endif

#ifdef __AVX512BW__
inline static size_t avx512bw_strstr_v3_anysize(const char* string, size_t n, const char* needle, size_t k) {

    DBG_ASSERT(n > 0);
    DBG_ASSERT(k > 0);

    const __m512i first = _mm512_set1_epi8(needle[0]);
    const __m512i last  = _mm512_set1_epi8(needle[k - 1]);

    char* haystack = const_cast<char*>(string);
    char* end      = haystack + n;

    for (/**/; haystack < end; haystack += 64) {

        const __m512i   block_first = _mm512_loadu_si512(haystack + 0);
        const __mmask64 first_eq    = _mm512_cmpeq_epi8_mask(block_first, first);

        if (first_eq == 0)
            continue;

        const __m512i block_last  = _mm512_loadu_si512(haystack + k - 1);
        uint64_t mask = _mm512_mask_cmpeq_epi8_mask(first_eq, block_last, last);

        while (mask != 0) {

            const uint64_t bitpos = bits::get_first_bit_set(mask);
            const char* s = reinterpret_cast<const char*>(haystack);

            if (memcmp(s + bitpos + 1, needle + 1, k - 2) == 0) {
                return (s - string) + bitpos;
            }

            mask = bits::clear_leftmost_set(mask);
        }
    }

    return size_t(-1);
}


template <size_t k, typename MEMCMP>
size_t avx512bw_strstr_v3_memcmp(const char* string, size_t n, const char* needle, MEMCMP memeq_fun) {

    DBG_ASSERT(n > 0);
    DBG_ASSERT(k > 0);

    const __m512i first = _mm512_set1_epi8(needle[0]);
    const __m512i last  = _mm512_set1_epi8(needle[k - 1]);

    char* haystack = const_cast<char*>(string);
    char* end      = haystack + n;

    for (/**/; haystack < end; haystack += 64) {

        const __m512i block_first = _mm512_loadu_si512(haystack + 0);
        const __mmask64 first_eq  = _mm512_cmpeq_epi8_mask(block_first, first);

        if (first_eq == 0)
            continue;

        const __m512i block_last  = _mm512_loadu_si512(haystack + k - 1);
        uint64_t mask = _mm512_mask_cmpeq_epi8_mask(first_eq, block_last, last);

        while (mask != 0) {

            const uint64_t bitpos = bits::get_first_bit_set(mask);
            const char* s = reinterpret_cast<const char*>(haystack);

            if (memeq_fun(s + bitpos + 1, needle + 1)) {
                return (s - string) + bitpos;
            }

            mask = bits::clear_leftmost_set(mask);
        }
    }

    return size_t(-1);
}

// ------------------------------------------------------------------------

inline static size_t avx512bw_strstr_v3(const char* s, size_t n, const char* needle, size_t k) {

    size_t result = std::string::npos;

    if (n < k) {
        return result;
    }

	switch (k) {
		case 0:
			return 0;

		case 1: {
            const char* res = reinterpret_cast<const char*>(strchr(s, needle[0]));

			return (res != nullptr) ? res - s : std::string::npos;
            }

        case 2:
            result = avx512bw_strstr_v3_memcmp<2>(s, n, needle, always_true);
            break;

        case 3:
            result = avx512bw_strstr_v3_memcmp<3>(s, n, needle, memcmp1);
            break;

        case 4:
            result = avx512bw_strstr_v3_memcmp<4>(s, n, needle, memcmp2);
            break;

        case 5:
            result = avx512bw_strstr_v3_memcmp<5>(s, n, needle, memcmp3);
            break;

        case 6:
            result = avx512bw_strstr_v3_memcmp<6>(s, n, needle, memcmp4);
            break;

        case 7:
            result = avx512bw_strstr_v3_memcmp<7>(s, n, needle, memcmp5);
            break;

        case 8:
            result = avx512bw_strstr_v3_memcmp<8>(s, n, needle, memcmp6);
            break;

        case 9:
            result = avx512bw_strstr_v3_memcmp<9>(s, n, needle, memcmp7);
            break;

        case 10:
            result = avx512bw_strstr_v3_memcmp<10>(s, n, needle, memcmp8);
            break;

        case 11:
            result = avx512bw_strstr_v3_memcmp<11>(s, n, needle, memcmp9);
            break;

        case 12:
            result = avx512bw_strstr_v3_memcmp<12>(s, n, needle, memcmp10);
            break;

		default:
			result = avx512bw_strstr_v3_anysize(s, n, needle, k);
            break;
    }

    if (result <= n - k) {
        return result;
    } else {
        return std::string::npos;
    }
}
#endif

} /* engine::string_utils */

namespace engine {

struct StringUtils {
	inline static uint64_t hash(const char* key, size_t len, uint64_t seed = 0) {
#ifdef IS_DEBUG_BUILD
		// requires alignment correct implementation, or else sanitizers fail

		// MurmurHashNeutral2, by Austin Appleby
		// Same as MurmurHash2, but endian- and alignment-neutral.
		// Half the speed though, alas.

		const uint32_t m = 0x5bd1e995;
		const int r = 24;

		uint32_t h = seed ^ len;

		const unsigned char * data = (const unsigned char *)key;

		while (len >= 4) {
			uint32_t k;

			k  = data[0];
			k |= data[1] << 8;
			k |= data[2] << 16;
			k |= data[3] << 24;

			k *= m;
			k ^= k >> r;
			k *= m;

			h *= m;
			h ^= k;

			data += 4;
			len -= 4;
		}

		switch(len) {
		case 3: h ^= data[2] << 16;
		case 2: h ^= data[1] << 8;
		case 1: h ^= data[0];
		  h *= m;
		};

		h ^= h >> 13;
		h *= m;
		h ^= h >> 15;

		return h;
#else
		// MurmurHash64A
		// MurmurHash2, 64-bit versions, by Austin Appleby
		// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;

		uint64_t h = seed ^ (len * m);

		const uint64_t* data = (const uint64_t*)key;
		const uint64_t* end = data + (len / 8);

		while (data != end) {
			uint64_t k = *data++;

			k *= m;
			k ^= k >> r;
			k *= m;

			h ^= k;
			h *= m;
		}

		const unsigned char* data2 = (const unsigned char*)data;

		switch (len & 7) {
		case 7: h ^= uint64_t(data2[6]) << 48;
		case 6: h ^= uint64_t(data2[5]) << 40;
		case 5: h ^= uint64_t(data2[4]) << 32;
		case 4: h ^= uint64_t(data2[3]) << 24;
		case 3: h ^= uint64_t(data2[2]) << 16;
		case 2: h ^= uint64_t(data2[1]) << 8;
		case 1: h ^= uint64_t(data2[0]); h *= m;
		};

		h ^= h >> r;
		h *= m;
		h ^= h >> r;

		return h;
#endif
	}

	static uint64_t rehash(const uint64_t h, const char* key, size_t len) {
		return hash(key, len, h);
	}

	static int32_t compare(const char* a, const char* b) {
		return strcmp(a, b);
	}

	static int32_t compare_with_len(const char* a, const char* b, uint32_t n) {
		return strncmp(a, b, n);
	}

	static int32_t contains(const char* a, uint32_t a_len, const char* b, uint32_t b_len) {
#ifdef __AVX512BW__
#ifndef IS_DEBUG_BUILD
		return engine::string_utils::avx512bw_strstr_v3(a, a_len, b, b_len) == size_t(-1) ? 0 : 1;
#endif
#endif

#ifdef __SSE4_2__
#ifndef IS_DEBUG_BUILD
		return engine::string_utils::sse42_strstr(a, a_len, b, b_len) == size_t(-1) ? 0 : 1;
#endif
#endif
		// return engine::string_utils::sse42_strstr(a, a_len, b, b_len);
		void* r = memmem(a, a_len, b, b_len);
		return r ? 1 : 0;
	}
};

} /* engine */
