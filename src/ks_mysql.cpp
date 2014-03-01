#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <mysql.h>

#include "schema.h"
#include "row_printer.h"

#define MYSQL_5_6_5 50605

class MySQLRes {
public:
	MySQLRes(MYSQL &mysql, bool buffer);
	~MySQLRes();

	inline MYSQL_RES *res() { return _res; }
	inline int n_tuples() const { return mysql_num_rows(_res); }
	inline int n_columns() const { return _n_columns; }

private:
	MYSQL_RES *_res;
	int _n_tuples;
	int _n_columns;
};

MySQLRes::MySQLRes(MYSQL &mysql, bool buffer) {
	_res = buffer ? mysql_store_result(&mysql) : mysql_use_result(&mysql);
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


class MySQLClient {
public:
	typedef MySQLRow RowType;

	MySQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password);
	~MySQLClient();

	template <typename RowReceiver>
	size_t retrieve_rows(const Table &table, const ColumnValues &prev_key, size_t row_count, RowReceiver &row_packer) {
		return query(retrieve_rows_sql(table, prev_key, row_count, '`'), row_packer, false /* as above */);
	}

	template <typename RowReceiver>
	size_t retrieve_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, RowReceiver &row_packer) {
		return query(retrieve_rows_sql(table, prev_key, last_key, '`'), row_packer, false /* nb. n_tuples won't work, which is ok since we send rows individually */);
	}

	size_t count_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		return atoi(select_one(count_rows_sql(table, prev_key, last_key, '`')).c_str());
	}

	void execute(const string &sql);
	void disable_referential_integrity();
	void enable_referential_integrity();
	string export_snapshot();
	void import_snapshot(const string &snapshot);
	void unhold_snapshot();
	void start_read_transaction();
	void start_write_transaction();
	void commit_transaction();
	void rollback_transaction();
	void populate_database_schema(Database &database);
	string escape_value(const string &value);

	inline const char* replace_sql_prefix() { return "REPLACE INTO "; }
	inline char quote_identifiers_with() { return '`'; }

	inline bool need_primary_key_clearer_to_replace() { return false; /* not needed since we support REPLACE */ }

	template <typename UniqueKeyClearerClass>
	inline void add_replace_clearers(vector<UniqueKeyClearerClass> &unique_key_clearers, const Table &table) { /* not needed since we support REPLACE */ }

protected:
	friend class MySQLTableLister;

	template <typename RowFunction>
	size_t query(const string &sql, RowFunction &row_handler, bool buffer) {
		if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
			backtrace();
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
			backtrace();
			throw runtime_error(mysql_error(&mysql) + string("\n") + sql);
		}

		return res.n_tuples();
	}

	string select_one(const string &sql) {
		if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
			backtrace();
			throw runtime_error(mysql_error(&mysql) + string("\n") + sql);
		}

		MySQLRes res(mysql, true);

		if (res.n_tuples() != 1 || res.n_columns() != 1) {
			throw runtime_error("Expected query to return only one row with only one column\n" + sql);
		}

		return MySQLRow(res, mysql_fetch_row(res.res())).string_at(0);
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
	const char *database_password) {

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
	mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "binary");
	if (!mysql_real_connect(&mysql, database_host, database_username, database_password, database_name, port, socket, 0)) {
		throw runtime_error(mysql_error(&mysql));
	}

	// increase the timeouts so that the connection doesn't get killed while trying to write large rowsets to the client over slow pipes
	execute("SET SESSION net_read_timeout = 300, net_write_timeout = 600");
}

MySQLClient::~MySQLClient() {
	mysql_close(&mysql);
}

void MySQLClient::execute(const string &sql) {
	if (mysql_real_query(&mysql, sql.c_str(), sql.size())) {
		throw runtime_error(mysql_error(&mysql) + string("\n") + sql);
	}
}

void MySQLClient::start_read_transaction() {
	execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	if (mysql_get_server_version(&mysql) >= MYSQL_5_6_5 && strstr(mysql_get_server_info(&mysql), "MariaDB") == NULL) {
		execute("START TRANSACTION READ ONLY WITH CONSISTENT SNAPSHOT");
	} else {
		execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
	}
}

void MySQLClient::start_write_transaction() {
	execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); // use read committed instead of the default repeatable read - we don't want to take gap locks
	execute("START TRANSACTION");
}

void MySQLClient::commit_transaction() {
	execute("COMMIT");
}

void MySQLClient::rollback_transaction() {
	execute("ROLLBACK");
}

string MySQLClient::export_snapshot() {
	// mysql's system catalogs are non-transactional and do not give a consistent snapshot; furthermore,
	// it doesn't support export/import of transactions, so we need to exclude other transactions while
	// we start up our set of read transactions so that they see consistent data.
	execute("FLUSH TABLES"); // wait for current update statements to finish, without blocking other connections
	execute("FLUSH TABLES WITH READ LOCK"); // then block other connections from updating/committing
	start_read_transaction(); // and start our transaction, and signal the other workers to start theirs
	return "locked";
}

void MySQLClient::import_snapshot(const string &snapshot) { // note the argument isn't needed for mysql
	start_read_transaction();
}

void MySQLClient::unhold_snapshot() {
	execute("UNLOCK TABLES");
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
		bool unique = (row.string_at(1) == "0");
		string key_name = row.string_at(2);
		string column_name = row.string_at(4);
		size_t column_index = table.index_of_column(column_name);
		// FUTURE: consider representing collation, sub_part, packed, index_type, and perhaps comment/index_comment

		if (key_name == "PRIMARY") {
			// there is of course only one primary key; we get a row for each column it includes
			table.primary_key_columns.push_back(column_index);

		} else {
			// a column in a generic key, which may or may not be unique
			if (table.keys.empty() || table.keys.back().name != key_name) {
				table.keys.push_back(Key(key_name, unique));
			}
			table.keys.back().columns.push_back(column_index);

			if (table.primary_key_columns.empty()) {
				// if we have no primary key, we might need to use another unique key as a surrogate - see MySQLTableLister below -
				// but this key must have no NULLable columns, as they effectively make the index not unique
				string nullable = row.string_at(9);
				if (unique && nullable == "YES") {
					// mark this as unusable
					unique_but_nullable_keys.insert(key_name);
				}
			}
		}
	}

	Table &table;
	set<string> unique_but_nullable_keys;
};

struct MySQLTableLister {
	inline MySQLTableLister(MySQLClient &client, Database &database): _client(client), database(database) {}

	inline void operator()(MySQLRow &row) {
		Table table(row.string_at(0));

		MySQLColumnLister column_lister(table);
		_client.query("SHOW COLUMNS FROM " + table.name, column_lister, false);

		MySQLKeyLister key_lister(table);
		_client.query("SHOW KEYS FROM " + table.name, key_lister, false);

		// if the table has no primary key, we need to find a unique key with no nullable columns to act as a surrogate primary key
		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name
		
		for (Keys::const_iterator key = table.keys.begin(); key != table.keys.end() && table.primary_key_columns.empty(); ++key) {
			if (key->unique && !key_lister.unique_but_nullable_keys.count(key->name)) {
				table.primary_key_columns = key->columns;
			}
		}
		if (table.primary_key_columns.empty()) {
			// of course this falls apart if there are no unique keys, so we don't allow that
			throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
		}

		database.tables.push_back(table);
	}

private:
	MySQLClient &_client;
	Database &database;
};

void MySQLClient::populate_database_schema(Database &database) {
	MySQLTableLister table_lister(*this, database);
	query("SELECT table_name FROM information_schema.tables WHERE table_schema = schema() ORDER BY data_length DESC, table_name ASC", table_lister, true /* buffer so we can make further queries during iteration */);
}


int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
