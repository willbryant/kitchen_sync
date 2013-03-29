#include "endpoint.h"

#include <stdexcept>
#include <libpq-fe.h>

class PostgreSQLClient {
public:
	PostgreSQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password);
	~PostgreSQLClient();

private:
	PGconn *conn;
};

PostgreSQLClient::PostgreSQLClient(
	const char *database_host,
	const char *database_port,
	const char *database_name,
	const char *database_username,
	const char *database_password) {

	conn = PQconnectdbParams(
		(const char *[]) { "host",        "port",        "dbname",      "user",            "password"        },
		(const char *[]) { database_host, database_port, database_name, database_username, database_password },
		1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
