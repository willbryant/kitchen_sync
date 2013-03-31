#include "endpoint.h"

#include <stdexcept>
#include <mysql.h>

#include "row_printer.h"

#define MYSQL_5_6_5 50605

class MySQLRes {
public:
	MySQLRes(MYSQL &mysql);
	~MySQLRes();

	inline MYSQL_RES *res() { return _res; }
	inline int n_tuples() { return _n_tuples; }
	inline int n_columns() { return _n_columns; }

private:
	MYSQL_RES *_res;
	int _n_tuples;
	int _n_columns;
};

MySQLRes::MySQLRes(MYSQL &mysql) {
	_res = mysql_use_result(&mysql);
	_n_tuples = mysql_num_rows(_res);
	_n_columns = mysql_num_fields(_res);
}

MySQLRes::~MySQLRes() {
	if (_res) {
		while (mysql_fetch_row(_res)) ; // must throw away any remaining results or else they'll be returned in the next requested resultset!
		mysql_free_result(_res);
	}
}


class MySQLRow {
public:
	inline MySQLRow(MySQLRes &res, MYSQL_ROW row): _res(res), _row(row) { _lengths = mysql_fetch_lengths(_res.res()); }

	inline    int n_columns() { return _res.n_columns(); }
	inline   bool   null_at(int column_number) { return     _row[column_number] == NULL; }
	inline  void *result_at(int column_number) { return     _row[column_number]; }
	inline    int length_of(int column_number) { return _lengths[column_number]; }
	inline string string_at(int column_number) { return string((char *)result_at(column_number), length_of(column_number)); }

private:
	MySQLRes &_res;
	MYSQL_ROW _row;
	unsigned long *_lengths;
};


class MySQLClient {
public:
	MySQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password,
		bool readonly);
	virtual ~MySQLClient();

	kitchen_sync::Database database_schema();

protected:
	void execute(const char *sql);
	void start_transaction(bool readonly);

	template <class RowFunction>
	void query(const string &sql, RowFunction &row_handler) {
		if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
			throw runtime_error(mysql_error(&mysql));
		}

	    MySQLRes res(mysql);

	    while (true) {
	    	MYSQL_ROW mysql_row = mysql_fetch_row(res.res());
	    	if (!mysql_row) break;
	    	MySQLRow row(res, mysql_row);
	    	row_handler(row);
	    }
	}

private:
	MYSQL mysql;

	// forbid copying
	MySQLClient(const MySQLClient& copy_from) { throw logic_error("copying forbidden"); }
};

MySQLClient::MySQLClient(
	const char *database_host,
	const char *database_port,
	const char *database_name,
	const char *database_username,
	const char *database_password,
	bool readonly) {

	// mysql_real_connect takes separate params for numeric ports and unix domain sockets
	int port = 0;
	const char *socket = NULL;
	if (database_port && *database_port) {
		if (*database_port >= '0' && *database_port <= '9') {
			port = atoi(database_port);
		} else {
			socket = database_port;
		}
	}

	mysql_init(&mysql);
	mysql_options(&mysql, MYSQL_READ_DEFAULT_GROUP, "ks_mysql");
	if (!mysql_real_connect(&mysql, database_host, database_username, database_password, database_name, port, socket, 0)) {
		throw runtime_error(mysql_error(&mysql));
	}

	// although we start the transaction here, in reality mysql's system catalogs are non-transactional
	// and do not give a consistent snapshot
	start_transaction(readonly);
}

MySQLClient::~MySQLClient() {
	mysql_close(&mysql);
}

void MySQLClient::execute(const char *sql) {
	if (mysql_query(&mysql, "BEGIN")) {
		throw runtime_error(mysql_error(&mysql));
	}
}

void MySQLClient::start_transaction(bool readonly) {
	execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	execute(readonly && mysql_get_server_version(&mysql) >= MYSQL_5_6_5 ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
}

/* dumps out the results of a query for debugging - but only supports string values currently */
struct MySQLTableLister {
	MySQLTableLister(kitchen_sync::Database &database): _database(database) {}

	void operator()(MySQLRow &row) {
		kitchen_sync::Table *table = _database.add_table();
		table->set_name(row.string_at(0));
	}

	kitchen_sync::Database &_database;
};

kitchen_sync::Database MySQLClient::database_schema() {
	kitchen_sync::Database database;
	MySQLTableLister table_lister(database);
	query<MySQLTableLister>("SHOW TABLES", table_lister);
	return database;
}

int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
