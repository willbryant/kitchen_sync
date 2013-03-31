#include "endpoint.h"

#include <stdexcept>
#include <libpq-fe.h>

#include "row_printer.h"

using namespace std;

class PostgreSQLRes {
public:
	PostgreSQLRes(PGresult *res);
	~PostgreSQLRes();

	inline PGresult *res() { return _res; }
	inline ExecStatusType status() { return PQresultStatus(_res); }
	inline int n_tuples() { return _n_tuples; }
	inline int n_columns() { return _n_columns; }

private:
	PGresult *_res;
	int _n_tuples;
	int _n_columns;
};

PostgreSQLRes::PostgreSQLRes(PGresult *res) {
	_res = res;
	_n_tuples = PQntuples(_res);
	_n_columns = PQnfields(_res);
}

PostgreSQLRes::~PostgreSQLRes() {
	if (_res) {
		PQclear(_res);
	}
}


class PostgreSQLRow {
public:
	inline PostgreSQLRow(PostgreSQLRes &res, int row_number): _res(res), _row_number(row_number) { }

	inline    int n_columns() { return _res.n_columns(); }
	inline   bool   null_at(int column_number) { return PQgetisnull(_res.res(), _row_number, column_number); }
	inline  void *result_at(int column_number) { return PQgetvalue(_res.res(), _row_number, column_number); }
	inline    int length_of(int column_number) { return PQgetlength(_res.res(), _row_number, column_number); }
	inline string string_at(int column_number) { return string((char *)result_at(column_number), length_of(column_number)); }

private:
	PostgreSQLRes &_res;
	int _row_number;
};


class PostgreSQLClient {
public:
	PostgreSQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password,
		bool readonly);
	virtual ~PostgreSQLClient();

	void print_tables();

protected:
	void execute(const char *sql);
	void start_transaction(bool readonly);

	template <class RowFunction>
	void query(const string &sql, RowFunction &row_handler) {
	    PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, NULL, NULL, NULL, NULL, 1 /* binary results */));

	    if (res.status() != PGRES_TUPLES_OK) {
			throw runtime_error(PQerrorMessage(conn));
	    }

	    for (int row_number = 0; row_number < res.n_tuples(); row_number++) {
	    	PostgreSQLRow row(res, row_number);
	    	row_handler(row);
	    }
	}

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
		(const char *[]) { "host",        "port",        "dbname",      "user",            "password",        NULL },
		(const char *[]) { database_host, database_port, database_name, database_username, database_password, NULL },
		1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}

	// postgresql has transactional DDL, so by starting our transaction before we've even looked at the tables,
	// we'll get a 100% consistent view.
	start_transaction(readonly);

	print_tables();
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

void PostgreSQLClient::execute(const char *sql) {
    PostgreSQLRes res(PQexec(conn, sql));

    if (res.status() != PGRES_COMMAND_OK) {
		throw runtime_error(PQerrorMessage(conn));
    }
}

void PostgreSQLClient::start_transaction(bool readonly) {
	execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	execute(readonly ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
}

void PostgreSQLClient::print_tables() {
	RowPrinter<PostgreSQLRow> row_printer;
	query< RowPrinter<PostgreSQLRow> >("SELECT tablename FROM pg_tables WHERE schemaname = ANY (current_schemas(false))", row_printer);
}

int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
