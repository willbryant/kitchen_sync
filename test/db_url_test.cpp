#include "../../catch2/catch.hpp"

#include "../src/db_url.h"

TEST_CASE("parsing full URLs", "[db_url]") {
	DbUrl url("postgresql://someuser:mypassword@localhost:1000/sourcedb");
	REQUIRE(url.protocol == "postgresql");
	REQUIRE(url.username == "someuser");
	REQUIRE(url.password == "mypassword");
	REQUIRE(url.host == "localhost");
	REQUIRE(url.port == "1000");
	REQUIRE(url.database == "sourcedb");
}

TEST_CASE("parsing minimal URLs", "[db_url]") {
	DbUrl url("mysql://localhost/x");
	REQUIRE(url.protocol == "mysql");
	REQUIRE(url.username == "");
	REQUIRE(url.password == "");
	REQUIRE(url.host == "localhost");
	REQUIRE(url.port == "");
	REQUIRE(url.database == "x");
}
