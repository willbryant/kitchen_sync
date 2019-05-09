#include "../../catch2/catch.hpp"

#include "../src/sql_functions.h"

TEST_CASE("quote_identifier", "[sql_functions]") {
	REQUIRE(quote_identifier("foo", '`') == "`foo`");
	REQUIRE(quote_identifier("foo_bar", '`') == "`foo_bar`");
	REQUIRE(quote_identifier("foo_`bar", '`') == "`foo_``bar`");
	REQUIRE(quote_identifier("foo_``bar", '`') == "`foo_````bar`");
	REQUIRE(quote_identifier("foo_`b`ar", '`') == "`foo_``b``ar`");
	REQUIRE(quote_identifier("`foo_bar", '`') == "```foo_bar`");
	REQUIRE(quote_identifier("foo_bar`", '`') == "`foo_bar```");

	REQUIRE(quote_identifier("foo", '"') == "\"foo\"");
	REQUIRE(quote_identifier("foo_bar", '"') == "\"foo_bar\"");
	REQUIRE(quote_identifier("foo_\"bar", '"') == "\"foo_\"\"bar\"");
	REQUIRE(quote_identifier("foo_\"\"bar", '"') == "\"foo_\"\"\"\"bar\"");
	REQUIRE(quote_identifier("foo_\"b\"ar", '"') == "\"foo_\"\"b\"\"ar\"");
	REQUIRE(quote_identifier("\"foo_bar", '"') == "\"\"\"foo_bar\"");
	REQUIRE(quote_identifier("foo_bar\"", '"') == "\"foo_bar\"\"\"");
}
