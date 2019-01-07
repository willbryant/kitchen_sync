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

TEST_CASE("parsing IPv6 URLs with ports", "[db_url]") {
	DbUrl url("postgresql://someuser:mypassword@[2001:0db8:f00f::0553:1211:0088]:1000/sourcedb");
	REQUIRE(url.protocol == "postgresql");
	REQUIRE(url.username == "someuser");
	REQUIRE(url.password == "mypassword");
	REQUIRE(url.host == "2001:0db8:f00f::0553:1211:0088");
	REQUIRE(url.port == "1000");
	REQUIRE(url.database == "sourcedb");
}

TEST_CASE("parsing IPv6 URLs without ports", "[db_url]") {
	DbUrl url("postgresql://someuser:mypassword@[2001:0db8:f00f::0553:1211:0088]/sourcedb");
	REQUIRE(url.protocol == "postgresql");
	REQUIRE(url.username == "someuser");
	REQUIRE(url.password == "mypassword");
	REQUIRE(url.host == "2001:0db8:f00f::0553:1211:0088");
	REQUIRE(url.port == "");
	REQUIRE(url.database == "sourcedb");
}

TEST_CASE("parsing IPv6 URLs without ports but with port separator", "[db_url]") {
	DbUrl url("postgresql://someuser:mypassword@[2001:0db8:f00f::0553:1211:0088]:/sourcedb");
	REQUIRE(url.protocol == "postgresql");
	REQUIRE(url.username == "someuser");
	REQUIRE(url.password == "mypassword");
	REQUIRE(url.host == "2001:0db8:f00f::0553:1211:0088");
	REQUIRE(url.port == "");
	REQUIRE(url.database == "sourcedb");
}
