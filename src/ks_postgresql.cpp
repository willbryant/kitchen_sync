#include "endpoint.h"

#include <stdexcept>
#include <libpq-fe.h>

#include "database_client.h"
#include "row_printer.h"

class PostgreSQLRes {
public:
	PostgreSQLRes(PGresult *res);
	~PostgreSQLRes();

	inline PGresult *res() { return _res; }
	inline ExecStatusType status() { return PQresultStatus(_res); }
	inline int n_tuples() const  { return _n_tuples; }
	inline int n_columns() const { return _n_columns; }

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
	inline const PostgreSQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }
	inline        bool   null_at(int column_number) const { return PQgetisnull(_res.res(), _row_number, column_number); }
	inline const void *result_at(int column_number) const { return PQgetvalue (_res.res(), _row_number, column_number); }
	inline         int length_of(int column_number) const { return PQgetlength(_res.res(), _row_number, column_number); }
	inline      string string_at(int column_number) const { return string((char *)result_at(column_number), length_of(column_number)); }

private:
	PostgreSQLRes &_res;
	int _row_number;
};


class PostgreSQLClient: public DatabaseClient {
public:
	typedef PostgreSQLRow RowType;

	PostgreSQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password,
		bool readonly);
	~PostgreSQLClient();

	template <class RowPacker>
	void retrieve_rows(const Table &table, const ColumnValues &prev_key, size_t row_count, RowPacker &row_packer) {
		query(retrieve_rows_sql(table, prev_key, row_count), row_packer);
	}

	template <class RowPacker>
	void retrieve_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, RowPacker &row_packer) {
		query(retrieve_rows_sql(table, prev_key, last_key), row_packer);
	}

	void execute(const string &sql);
	void commit_transaction();

protected:
	friend class PostgreSQLTableLister;

	void start_transaction(bool readonly);
	void populate_database_schema();

	template <class RowFunction>
	void query(const string &sql, RowFunction &row_handler) {
	    PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, NULL, NULL, NULL, NULL, 0 /* text-format results only */));

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

	// forbid copying
	PostgreSQLClient(const PostgreSQLClient& copy_from) { throw logic_error("copying forbidden"); }
};

PostgreSQLClient::PostgreSQLClient(
	const char *database_host,
	const char *database_port,
	const char *database_name,
	const char *database_username,
	const char *database_password,
	bool readonly) {

	const char *keywords[] = { "host",        "port",        "dbname",      "user",            "password",        NULL };
	const char *values[]   = { database_host, database_port, database_name, database_username, database_password, NULL };

	conn = PQconnectdbParams(keywords, values, 1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}

	// postgresql has transactional DDL, so by starting our transaction before we've even looked at the tables,
	// we'll get a 100% consistent view.
	start_transaction(readonly);

	populate_database_schema();
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

void PostgreSQLClient::execute(const string &sql) {
    PostgreSQLRes res(PQexec(conn, sql.c_str()));

    if (res.status() != PGRES_COMMAND_OK) {
		throw runtime_error(PQerrorMessage(conn));
    }
}

void PostgreSQLClient::start_transaction(bool readonly) {
	execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	execute(readonly ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
}

void PostgreSQLClient::commit_transaction() {
	execute("COMMIT");
}

struct PostgreSQLColumnLister {
	inline PostgreSQLColumnLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		Column column(row.string_at(0));
		table.columns.push_back(column);
	}

	Table &table;
};

struct PostgreSQLKeyLister {
	inline PostgreSQLKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string column_name = row.string_at(0);
		size_t column_index = table.index_of_column(column_name);
		table.primary_key_columns.push_back(column_index);
	}

	Table &table;
};

struct PostgreSQLTableLister {
	PostgreSQLTableLister(PostgreSQLClient &client): _client(client) {}

	void operator()(PostgreSQLRow &row) {
		Table table(row.string_at(0));

		PostgreSQLColumnLister column_lister(table);
		_client.query(
			"SELECT attname "
			  "FROM pg_attribute, pg_class "
			 "WHERE attrelid = pg_class.oid AND "
			       "attnum > 0 AND "
			       "NOT attisdropped AND "
			       "relname = '" + row.string_at(0) + "' "
			 "ORDER BY attnum",
			column_lister);

		PostgreSQLKeyLister key_lister(table);
		_client.query(
			"SELECT column_name "
			  "FROM information_schema.table_constraints, "
			       "information_schema.key_column_usage "
			 "WHERE information_schema.table_constraints.table_name = '" + row.string_at(0) + "' AND "
			       "information_schema.key_column_usage.table_name = information_schema.table_constraints.table_name AND "
			       "constraint_type = 'PRIMARY KEY' "
			 "ORDER BY ordinal_position",
			key_lister);

		_client.database.tables.push_back(table);
	}

	PostgreSQLClient &_client;
};

void PostgreSQLClient::populate_database_schema() {
	PostgreSQLTableLister table_lister(*this);
	query("SELECT tablename "
		    "FROM pg_tables "
		   "WHERE schemaname = ANY (current_schemas(false))",
		  table_lister);
	index_database_tables();
}


int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
