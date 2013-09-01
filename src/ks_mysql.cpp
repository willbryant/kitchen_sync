#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <mysql.h>

#include "database_client.h"
#include "row_printer.h"

#define MYSQL_5_6_5 50605

class MySQLRes {
public:
	MySQLRes(MYSQL &mysql, bool buffer);
	~MySQLRes();

	inline MYSQL_RES *res() { return _res; }
	inline int n_tuples() const { return _n_tuples; }
	inline int n_columns() const { return _n_columns; }

private:
	MYSQL_RES *_res;
	int _n_tuples;
	int _n_columns;
};

MySQLRes::MySQLRes(MYSQL &mysql, bool buffer) {
	_res = buffer ? mysql_store_result(&mysql) : mysql_use_result(&mysql);
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
	inline const MySQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }
	inline        bool   null_at(int column_number) const { return     _row[column_number] == NULL; }
	inline const void *result_at(int column_number) const { return     _row[column_number]; }
	inline         int length_of(int column_number) const { return _lengths[column_number]; }
	inline      string string_at(int column_number) const { return string((char *)result_at(column_number), length_of(column_number)); }

private:
	MySQLRes &_res;
	MYSQL_ROW _row;
	unsigned long *_lengths;
};


class MySQLClient: public DatabaseClient {
public:
	typedef MySQLRow RowType;

	MySQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password,
		bool readonly);
	~MySQLClient();

	template <class RowPacker>
	void retrieve_rows(const Table &table, const ColumnValues &prev_key, size_t row_count, RowPacker &row_packer) {
		query(retrieve_rows_sql(table, prev_key, row_count), row_packer, false /* as above */);
	}

	template <class RowPacker>
	void retrieve_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, RowPacker &row_packer) {
		query(retrieve_rows_sql(table, prev_key, last_key), row_packer, false /* nb. n_tuples won't work, which is ok since we send rows individually */);
	}

	void execute(const string &sql);
	void disable_referential_integrity();
	void enable_referential_integrity();
	void commit_transaction();
	string escape_value(const string &value);

protected:
	friend class MySQLTableLister;

	void start_transaction(bool readonly);
	void populate_database_schema();

	template <class RowFunction>
	void query(const string &sql, RowFunction &row_handler, bool buffer) {
		if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
			throw runtime_error(mysql_error(&mysql) + string("\n") + sql);
		}

		MySQLRes res(mysql, buffer);

		while (true) {
			MYSQL_ROW mysql_row = mysql_fetch_row(res.res());
			if (!mysql_row) break;
			MySQLRow row(res, mysql_row);
			row_handler(row);
		}

		// check again for errors, as mysql_fetch_row would return NULL for both errors & no more rows
		if (mysql_errno(&mysql)) {
			throw runtime_error(mysql_error(&mysql) + string("\n") + sql);
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

	populate_database_schema();
}

MySQLClient::~MySQLClient() {
	mysql_close(&mysql);
}

void MySQLClient::execute(const string &sql) {
	if (mysql_real_query(&mysql, sql.c_str(), sql.size())) {
		throw runtime_error(mysql_error(&mysql));
	}
}

void MySQLClient::start_transaction(bool readonly) {
	execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	execute(readonly && mysql_get_server_version(&mysql) >= MYSQL_5_6_5 ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
}

void MySQLClient::commit_transaction() {
	execute("COMMIT");
}

void MySQLClient::disable_referential_integrity() {
	execute("SET foreign_key_checks = 0");
}

void MySQLClient::enable_referential_integrity() {
	execute("SET foreign_key_checks = 1");
}

string MySQLClient::escape_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = mysql_real_escape_string(&mysql, (char*)result.data(), value.c_str(), value.size());
	result.resize(result_length);
	return result;
}

struct MySQLColumnLister {
	inline MySQLColumnLister(Table &table): table(table) {}

	inline void operator()(MySQLRow &row) {
		Column column(row.string_at(0));
		table.columns.push_back(column);
	}

	Table &table;
};

struct MySQLKeyLister {
	inline MySQLKeyLister(Table &table): table(table) {}

	inline void operator()(MySQLRow &row) {
		string key_name = row.string_at(2);
		if (key_name == "PRIMARY") {
			string column_name = row.string_at(4);
			size_t column_index = table.index_of_column(column_name);
			table.primary_key_columns.push_back(column_index);
		}
		if (table.primary_key_columns.empty()) {
			// if we have no primary key, we might need to use another unique key as a surrogate - see MySQLTableLister below
			string non_unique = row.string_at(1);
			if (non_unique == "0") {
				// furthermore this key must have no NULLable columns, as they effectively make the index not unique
				string nullable = row.string_at(9);
				if (nullable == "YES") {
					// mark this as unusable
					unique_but_nullable_keys.insert(key_name);
				} else {
					string column_name = row.string_at(4);
					size_t column_index = table.index_of_column(column_name);
					unique_keys[key_name].push_back(column_index);
				}
			}
		}
	}

	Table &table;
	map<string, ColumnIndices> unique_keys;
	set<string> unique_but_nullable_keys;
};

struct MySQLTableLister {
	inline MySQLTableLister(MySQLClient &client): _client(client) {}

	inline void operator()(MySQLRow &row) {
		Table table(row.string_at(0));

		MySQLColumnLister column_lister(table);
		_client.query("SHOW COLUMNS FROM " + table.name, column_lister, false);

		MySQLKeyLister key_lister(table);
		_client.query("SHOW KEYS FROM " + table.name, key_lister, false);

		if (table.primary_key_columns.empty()) {
			// if the table has no primary key, we need to find a unique key with no nullable columns to act as a surrogate primary key
			for (set<string>::const_iterator key = key_lister.unique_but_nullable_keys.begin(); key != key_lister.unique_but_nullable_keys.end(); ++key) {
				key_lister.unique_keys.erase(*key);
			}
			if (key_lister.unique_keys.empty()) {
				// of course this falls apart if there are no unique keys, so we don't allow that
				throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
			}
			table.primary_key_columns = key_lister.unique_keys.begin()->second; // will use the first key alphabetically, so should be comparable at the other end
		}

		_client.database.tables.push_back(table);
	}

private:
	MySQLClient &_client;
};

void MySQLClient::populate_database_schema() {
	MySQLTableLister table_lister(*this);
	query("SHOW TABLES", table_lister, true /* buffer so we can make further queries during iteration */);
	index_database_tables();
}


int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
