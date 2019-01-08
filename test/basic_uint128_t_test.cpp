#include "../../catch2/catch.hpp"

#include "../src/basic_uint128_t.h"

TEST_CASE("addition", "[basic_uint128_t]") {
	REQUIRE(basic_uint128_t{0, 1} + basic_uint128_t{0, 1} == basic_uint128_t{0, 2});
	REQUIRE(basic_uint128_t{1, 0} + basic_uint128_t{1, 0} == basic_uint128_t{2, 0});
	REQUIRE(basic_uint128_t{1, 1} + basic_uint128_t{1, 1} == basic_uint128_t{2, 2});
	REQUIRE(basic_uint128_t{0, 1} + basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{1, 0});
	REQUIRE(basic_uint128_t{0, 1} + basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{2, 0});
	REQUIRE(basic_uint128_t{0, 0xffffffffffffffff} + basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{1, 0xfffffffffffffffe});
	REQUIRE(basic_uint128_t{1, 0xffffffffffffffff} + basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{3, 0xfffffffffffffffe});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0} + basic_uint128_t{0xffffffffffffffff, 0} == basic_uint128_t{0xfffffffffffffffe, 0});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} + basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} == basic_uint128_t{0xffffffffffffffff, 0xfffffffffffffffe});

	REQUIRE(basic_uint128_t{0, 1} + basic_uint128_t{0, 1} == basic_uint128_t{0, 2});
	REQUIRE(basic_uint128_t{1, 0} + basic_uint128_t{1, 0} == basic_uint128_t{2, 0});
	REQUIRE(basic_uint128_t{1, 1} + basic_uint128_t{1, 1} == basic_uint128_t{2, 2});
	REQUIRE(basic_uint128_t{0, 0xffffffffffffffff} + basic_uint128_t{0, 1} == basic_uint128_t{1, 0});
	REQUIRE(basic_uint128_t{1, 0xffffffffffffffff} + basic_uint128_t{0, 1} == basic_uint128_t{2, 0});
	REQUIRE(basic_uint128_t{0, 0xffffffffffffffff} + basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{1, 0xfffffffffffffffe});
	REQUIRE(basic_uint128_t{1, 0xffffffffffffffff} + basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{3, 0xfffffffffffffffe});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0} + basic_uint128_t{0xffffffffffffffff, 0} == basic_uint128_t{0xfffffffffffffffe, 0});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} + basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} == basic_uint128_t{0xffffffffffffffff, 0xfffffffffffffffe});
}

TEST_CASE("subtraction", "[basic_uint128_t]") {
	REQUIRE(basic_uint128_t{0, 2} - basic_uint128_t{0, 1} == basic_uint128_t{0, 1});
	REQUIRE(basic_uint128_t{2, 0} - basic_uint128_t{1, 0} == basic_uint128_t{1, 0});
	REQUIRE(basic_uint128_t{2, 2} - basic_uint128_t{1, 1} == basic_uint128_t{1, 1});
	REQUIRE(basic_uint128_t{1, 0} - basic_uint128_t{0, 1} == basic_uint128_t{0, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{2, 0} - basic_uint128_t{0, 1} == basic_uint128_t{1, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{1, 0xfffffffffffffffe} - basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{0, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{3, 0xfffffffffffffffe} - basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{1, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{0xfffffffffffffffe, 0} - basic_uint128_t{0xffffffffffffffff, 0} == basic_uint128_t{0xffffffffffffffff, 0});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0xfffffffffffffffe} - basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} == basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff});

	REQUIRE(basic_uint128_t{0, 2} - basic_uint128_t{0, 1} == basic_uint128_t{0, 1});
	REQUIRE(basic_uint128_t{2, 0} - basic_uint128_t{1, 0} == basic_uint128_t{1, 0});
	REQUIRE(basic_uint128_t{2, 2} - basic_uint128_t{1, 1} == basic_uint128_t{1, 1});
	REQUIRE(basic_uint128_t{1, 0} - basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{0, 1});
	REQUIRE(basic_uint128_t{2, 0} - basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{0, 1});
	REQUIRE(basic_uint128_t{1, 0xfffffffffffffffe} - basic_uint128_t{0, 0xffffffffffffffff} == basic_uint128_t{0, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{3, 0xfffffffffffffffe} - basic_uint128_t{1, 0xffffffffffffffff} == basic_uint128_t{1, 0xffffffffffffffff});
	REQUIRE(basic_uint128_t{0xfffffffffffffffe, 0} - basic_uint128_t{0xffffffffffffffff, 0} == basic_uint128_t{0xffffffffffffffff, 0});
	REQUIRE(basic_uint128_t{0xffffffffffffffff, 0xfffffffffffffffe} - basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} == basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff});
}

TEST_CASE("right shift", "[basic_uint128_t]") {
	REQUIRE((basic_uint128_t{0, 1} >> 1) == basic_uint128_t{0, 0});
	REQUIRE((basic_uint128_t{0, 2} >> 1) == basic_uint128_t{0, 1});
	REQUIRE((basic_uint128_t{0, 3} >> 1) == basic_uint128_t{0, 1});
	REQUIRE((basic_uint128_t{0, 4} >> 1) == basic_uint128_t{0, 2});
	REQUIRE((basic_uint128_t{0, 0xffffffffffffffff} >> 1) == basic_uint128_t{0, 0x7fffffffffffffff});
	REQUIRE((basic_uint128_t{1, 0} >> 1) == basic_uint128_t{0, 0x8000000000000000});
	REQUIRE((basic_uint128_t{0xffffffffffffffff, 0} >> 1) == basic_uint128_t{0x7fffffffffffffff, 0x8000000000000000});
	REQUIRE((basic_uint128_t{0xffffffffffffffff, 0xffffffffffffffff} >> 1) == basic_uint128_t{0x7fffffffffffffff, 0xffffffffffffffff});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x1234567812345678} >> 4) == basic_uint128_t{0x0123456781234567, 0x8123456781234567});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x1111111111111111} >> 4) == basic_uint128_t{0x0123456781234567, 0x8111111111111111});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x0000000000000000} >> 4) == basic_uint128_t{0x0123456781234567, 0x8000000000000000});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x0000000000000000} >> 8) == basic_uint128_t{0x0012345678123456, 0x7800000000000000});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x1111111111111111} >> 64) == basic_uint128_t{0x0000000000000000, 0x1234567812345678});
	REQUIRE((basic_uint128_t{0x1234567812345678, 0x1111111111111111} >> 68) == basic_uint128_t{0x0000000000000000, 0x0123456781234567});
}
