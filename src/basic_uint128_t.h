#ifndef BASIC_U128_T_H
#define BASIC_U128_T_H

#include <cstdint>

// 128-bit unsigned integer implementation supporting only the bare minimum of operations needed to implement finding the midpoint of two 128-bit UUIDs.
// we could conditionally use the compiler's __uint128_t type if defined (GCC & Clang both do on CPUs that support it) in order to get their optimised
// versions, but since this is only used in the UUID handling code and is going to be compiled fairly efficiently anyway, it's not worth it.
struct basic_uint128_t {
	uint64_t h, l;

	inline uint64_t operator=(uint64_t other) {
		h = 0;
		l = other;
		return other;
	}

	inline basic_uint128_t operator+(const basic_uint128_t &other) const {
		basic_uint128_t result;

		result.l = l + other.l;
		result.h = h + other.h + (result.l < l);

		return result;
	}

	inline basic_uint128_t operator-(const basic_uint128_t &other) const {
		basic_uint128_t result;

		result.l = l - other.l;
		result.h = h - other.h - (result.l > l);

		return result;
	}

	inline basic_uint128_t operator>>(size_t n) const {
		basic_uint128_t result;

		if (n >= 64) {
			result.h = 0;
			result.l = h >> (n - 64);
		} else {
			result.h = (h >> n);
			result.l = (l >> n) | (h << (64 - n));
		}

		return result;
	}

	inline bool operator==(const basic_uint128_t &other) const {
		return (h == other.h && l == other.l);
	}
};

#endif
