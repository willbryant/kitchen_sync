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
		const char *database_password,
		bool readonly);
	~PostgreSQLClient();

protected:
	void execute(const char *sql);
	void start_transaction(bool readonly);

private:
	PGconn *conn;
};

PostgreSQLClient::PostgreSQLClient(
	const char *database_host,
	const char *database_port,
	const char *database_name,
	const char *database_username,
	const char *database_password,
	bool readonly) {

	conn = PQconnectdbParams(
		(const char *[]) { "host",        "port",        "dbname",      "user",            "password"        },
		(const char *[]) { database_host, database_port, database_name, database_username, database_password },
		1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}

	// postgresql has transactional DDL, so by starting our transaction before we've even looked at the tables,
	// we'll get a 100% consistent view.
	start_transaction(readonly);
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

void PostgreSQLClient::execute(const char *sql) {
    PGresult *res = PQexec(conn, sql);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PQclear(res);
		throw runtime_error(PQerrorMessage(conn));
    }
}

void PostgreSQLClient::start_transaction(bool readonly) {
	execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	execute(readonly ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
}

int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
