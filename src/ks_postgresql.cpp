#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <libpq-fe.h>

#include "schema.h"
#include "database_client_traits.h"
#include "row_printer.h"

class PostgreSQLRes {
public:
	PostgreSQLRes(PGresult *res);
	~PostgreSQLRes();

	inline PGresult *res() { return _res; }
	inline ExecStatusType status() { return PQresultStatus(_res); }
	inline int n_tuples() const  { return _n_tuples; }
	inline int n_columns() const { return _n_columns; }
	inline Oid type_of(int column_number) const { return types[column_number]; }

private:
	PGresult *_res;
	int _n_tuples;
	int _n_columns;
	vector<Oid> types;
};

PostgreSQLRes::PostgreSQLRes(PGresult *res) {
	_res = res;

	_n_tuples = PQntuples(_res);
	_n_columns = PQnfields(_res);

	types.resize(_n_columns);
	for (size_t i = 0; i < _n_columns; i++) {
		types[i] = PQftype(_res, i);
	}
}

PostgreSQLRes::~PostgreSQLRes() {
	if (_res) {
		PQclear(_res);
	}
}


// from pg_type.h, which isn't available/working on all distributions.
#define BOOLOID			16
#define BYTEAOID		17
#define INT2OID			21
#define INT4OID			23
#define INT8OID			20

class PostgreSQLRow {
public:
	inline PostgreSQLRow(PostgreSQLRes &res, int row_number): _res(res), _row_number(row_number) { }
	inline const PostgreSQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }

	inline        bool   null_at(int column_number) const { return PQgetisnull(_res.res(), _row_number, column_number); }
	inline const void *result_at(int column_number) const { return PQgetvalue (_res.res(), _row_number, column_number); }
	inline         int length_of(int column_number) const { return PQgetlength(_res.res(), _row_number, column_number); }
	inline      string string_at(int column_number) const { return string((const char *)result_at(column_number), length_of(column_number)); }
	inline        bool   bool_at(int column_number) const { return (strcmp((const char *)result_at(column_number), "t") == 0); }
	inline     int64_t    int_at(int column_number) const { return strtoll((const char *)result_at(column_number), NULL, 10); }

	string decoded_byte_string_at(int column_number) const;

	template <typename Packer>
	inline void pack_column_into(Packer &packer, int column_number) const {
		if (null_at(column_number)) {
			packer << nullptr;
		} else {
			switch (_res.type_of(column_number)) {
				case BOOLOID:
					packer << bool_at(column_number);
					break;

				case BYTEAOID:
					packer << decoded_byte_string_at(column_number);
					break;

				case INT2OID:
				case INT4OID:
				case INT8OID:
					packer << int_at(column_number);
					break;

				default:
					// we use our non-copied memory class, equivalent to but faster than using string_at
					packer << memory(result_at(column_number), length_of(column_number));
			}
		}
	}

	template <typename Packer>
	void pack_row_into(Packer &packer) const {
		pack_array_length(packer, n_columns());

		for (size_t column_number = 0; column_number < n_columns(); column_number++) {
			pack_column_into(packer, column_number);
		}
	}

private:
	PostgreSQLRes &_res;
	int _row_number;
};

string PostgreSQLRow::decoded_byte_string_at(int column_number) const {
	const unsigned char *value = (const unsigned char *)result_at(column_number);
	size_t decoded_length;
	const unsigned char *decoded = PQunescapeBytea(value, &decoded_length);
	string result(decoded, decoded + decoded_length);
	PQfreemem((void *)decoded);
	return result;
}


class PostgreSQLClient: public GlobalKeys, public SequenceColumns, public DropKeysWhenColumnsDropped, public SetNullability {
public:
	typedef PostgreSQLRow RowType;

	PostgreSQLClient(
		const string &database_host,
		const string &database_port,
		const string &database_name,
		const string &database_username,
		const string &database_password,
		const string &variables);
	~PostgreSQLClient();

	template <typename RowReceiver>
	size_t retrieve_rows(RowReceiver &row_receiver, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
		return query(retrieve_rows_sql(*this, table, prev_key, last_key, row_count), row_receiver);
	}

	size_t count_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key);
	ColumnValues first_key(const Table &table);
	ColumnValues  last_key(const Table &table);

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
	void convert_unsupported_database_schema(Database &database);
	string escape_value(const string &value);
	string escape_column_value(const Column &column, const string &value);
	string column_type(const Column &column);
	string column_sequence_name(const Table &table, const Column &column);
	string column_default(const Table &table, const Column &column);
	string column_definition(const Table &table, const Column &column);

	inline char quote_identifiers_with() const { return '"'; }
	inline ColumnFlags supported_flags() const { return ColumnFlags::time_zone; }

protected:
	friend class PostgreSQLTableLister;

	template <typename RowFunction>
	size_t query(const string &sql, RowFunction &row_handler) {
		PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */));

		if (res.status() != PGRES_TUPLES_OK) {
			throw runtime_error(sql_error(sql));
		}

		for (int row_number = 0; row_number < res.n_tuples(); row_number++) {
			PostgreSQLRow row(res, row_number);
			row_handler(row);
		}

		return res.n_tuples();
	}

	string select_one(const string &sql);
	string sql_error(const string &sql);

private:
	PGconn *conn;

	// forbid copying
	PostgreSQLClient(const PostgreSQLClient& copy_from) { throw logic_error("copying forbidden"); }
};

PostgreSQLClient::PostgreSQLClient(
	const string &database_host,
	const string &database_port,
	const string &database_name,
	const string &database_username,
	const string &database_password,
	const string &variables) {

	const char *keywords[] = { "host",                "port",                "dbname",              "user",                    "password",                nullptr };
	const char *values[]   = { database_host.c_str(), database_port.c_str(), database_name.c_str(), database_username.c_str(), database_password.c_str(), nullptr };

	conn = PQconnectdbParams(keywords, values, 1 /* allow expansion */);

	if (PQstatus(conn) != CONNECTION_OK) {
		throw runtime_error(PQerrorMessage(conn));
	}
	if (PQsetClientEncoding(conn, "SQL_ASCII")) {
		throw runtime_error(PQerrorMessage(conn));
	}

	execute("SET client_min_messages TO WARNING");

	if (!variables.empty()) {
		execute("SET " + variables);
	}
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

size_t PostgreSQLClient::count_rows(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	return atoi(select_one(count_rows_sql(*this, table, prev_key, last_key)).c_str());
}

ColumnValues PostgreSQLClient::first_key(const Table &table) {
	ValueCollector receiver;
	query(select_first_key_sql(*this, table), receiver);
	return receiver.values;
}

ColumnValues PostgreSQLClient::last_key(const Table &table) {
	ValueCollector receiver;
	query(select_last_key_sql(*this, table), receiver);
	return receiver.values;
}

void PostgreSQLClient::execute(const string &sql) {
    PostgreSQLRes res(PQexec(conn, sql.c_str()));

    if (res.status() != PGRES_COMMAND_OK && res.status() != PGRES_TUPLES_OK) {
		throw runtime_error(sql_error(sql));
    }
}

string PostgreSQLClient::select_one(const string &sql) {
	PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */));

	if (res.status() != PGRES_TUPLES_OK) {
		throw runtime_error(sql_error(sql));
	}

	if (res.n_tuples() != 1 || res.n_columns() != 1) {
		throw runtime_error("Expected query to return only one row with only one column\n" + sql);
	}

	return PostgreSQLRow(res, 0).string_at(0);
}

string PostgreSQLClient::sql_error(const string &sql) {
	if (sql.size() < 200) {
		return PQerrorMessage(conn) + string("\n") + sql;
	} else {
		return PQerrorMessage(conn) + string("\n") + sql.substr(0, 200) + "...";
	}
}

void PostgreSQLClient::start_read_transaction() {
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
}

void PostgreSQLClient::start_write_transaction() {
	execute("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
}

void PostgreSQLClient::commit_transaction() {
	execute("COMMIT");
}

void PostgreSQLClient::rollback_transaction() {
	execute("ROLLBACK");
}

string PostgreSQLClient::export_snapshot() {
	// postgresql has transactional DDL, so by starting our transaction before we've even looked at the tables,
	// we'll get a 100% consistent view.
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
	return select_one("SELECT pg_export_snapshot()");
}

void PostgreSQLClient::import_snapshot(const string &snapshot) {
	execute("START TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ");
	execute("SET TRANSACTION SNAPSHOT '" + escape_value(snapshot) + "'");
}

void PostgreSQLClient::unhold_snapshot() {
	// do nothing - only needed for lock-based systems like mysql
}

void PostgreSQLClient::disable_referential_integrity() {
	execute("SET CONSTRAINTS ALL DEFERRED");

	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + table.name + " DISABLE TRIGGER ALL");
	}
	*/
}

void PostgreSQLClient::enable_referential_integrity() {
	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + table.name + " ENABLE TRIGGER ALL");
	}
	*/
}

string PostgreSQLClient::escape_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)result.data(), value.c_str(), value.size(), nullptr);
	result.resize(result_length);
	return result;
}

string PostgreSQLClient::escape_column_value(const Column &column, const string &value) {
	if (column.column_type != ColumnTypes::BLOB) {
		return escape_value(value);
	}

	size_t encoded_length;
	const unsigned char *encoded = PQescapeByteaConn(conn, (const unsigned char *)value.c_str(), value.size(), &encoded_length);
	string result(encoded, encoded + encoded_length);
	PQfreemem((void *)encoded);

	// bizarrely, the bytea parser is an extra level on top of the normal escaping, so you still need the latter after PQescapeByteaConn, even though PQunescapeBytea doesn't do the reverse
	return escape_value(result);
}

void PostgreSQLClient::convert_unsupported_database_schema(Database &database) {
	for (Table &table : database.tables) {
		for (Column &column : table.columns) {
			if (column.column_type == ColumnTypes::UINT) {
				// postgresql doesn't support unsigned columns; to make migration from databases that do
				// easier, we don't reject unsigned columns, we just convert them to the signed equivalent
				// and rely on it raising if we try to insert an invalid value
				column.column_type = ColumnTypes::SINT;
			}

			if (column.column_type == ColumnTypes::SINT && column.size == 1) {
				// not used by postgresql; smallint is the nearest equivalent
				column.size = 2;
			}

			if (column.column_type == ColumnTypes::SINT && column.size == 3) {
				// not used by postgresql; integer is the nearest equivalent
				column.size = 4;
			}

			if (column.column_type == ColumnTypes::TEXT || column.column_type == ColumnTypes::BLOB) {
				// postgresql doesn't have different sized TEXT/BLOB columns, they're all equivalent to mysql's biggest type
				column.size = 0;
			}
		}

		for (Key &key : table.keys) {
			if (key.name.size() >= 63) {
				// postgresql has a hardcoded limit of 63 characters for index names
				key.name = key.name.substr(0, 63);
			}
		}
	}
}

string PostgreSQLClient::column_type(const Column &column) {
	if (column.column_type == ColumnTypes::BLOB) {
		return "bytea";

	} else if (column.column_type == ColumnTypes::TEXT) {
		return "text";

	} else if (column.column_type == ColumnTypes::VCHR) {
		string result("character varying(");
		result += to_string(column.size);
		result += ")";
		return result;

	} else if (column.column_type == ColumnTypes::FCHR) {
		string result("character(");
		result += to_string(column.size);
		result += ")";
		return result;

	} else if (column.column_type == ColumnTypes::UUID) {
		return "uuid";

	} else if (column.column_type == ColumnTypes::BOOL) {
		return "boolean";

	} else if (column.column_type == ColumnTypes::SINT) {
		switch (column.size) {
			case 2:
				return "smallint";

			case 4:
				return "integer";

			case 8:
				return "bigint";

			default:
				throw runtime_error("Don't know how to create integer column " + column.name + " of size " + to_string(column.size));
		}

	} else if (column.column_type == ColumnTypes::REAL) {
		return (column.size == 4 ? "real" : "double precision");

	} else if (column.column_type == ColumnTypes::DECI) {
		string result("numeric(");
		result += to_string(column.size);
		result += ',';
		result += to_string(column.scale);
		result += ')';
		return result;

	} else if (column.column_type == ColumnTypes::DATE) {
		return "date";

	} else if (column.column_type == ColumnTypes::TIME) {
		if (column.flags & ColumnFlags::time_zone) {
			return "time with time zone";
		} else {
			return "time without time zone";
		}

	} else if (column.column_type == ColumnTypes::DTTM) {
		if (column.flags & ColumnFlags::time_zone) {
			return "timestamp with time zone";
		} else {
			return "timestamp without time zone";
		}

	} else {
		throw runtime_error("Don't know how to express column type of " + column.name + " (" + column.column_type + ")");
	}
}

string PostgreSQLClient::column_sequence_name(const Table &table, const Column &column) {
	// name to match what postgresql creates for serial columns
	return table.name + "_" + column.name + "_seq";
}

string PostgreSQLClient::column_default(const Table &table, const Column &column) {
	string result(" DEFAULT ");

	switch (column.default_type) {
		case DefaultType::no_default:
			result += "NULL";
			break;

		case DefaultType::sequence:
			result += "nextval('";
			result += escape_value(column_sequence_name(table, column));
			result += "'::regclass)";
			break;

		case DefaultType::default_value:
			if (column.column_type == ColumnTypes::BOOL ||
				column.column_type == ColumnTypes::SINT ||
				column.column_type == ColumnTypes::UINT ||
				column.column_type == ColumnTypes::REAL ||
				column.column_type == ColumnTypes::DECI) {
				result += column.default_value;
			} else {
				result += "'";
				result += escape_column_value(column, column.default_value);
				result += "'";
			}
			break;

		case DefaultType::default_function:
			result += column.default_value;
	}

	return result;
}

string PostgreSQLClient::column_definition(const Table &table, const Column &column) {
	string result;
	result += quote_identifiers_with();
	result += column.name;
	result += quote_identifiers_with();
	result += ' ';

	result += column_type(column);

	if (!column.nullable) {
		result += " NOT NULL";
	}

	if (column.default_type) {
		result += column_default(table, column);
	}

	return result;
}

struct PostgreSQLColumnLister {
	inline PostgreSQLColumnLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string name(row.string_at(0));
		string db_type(row.string_at(1));
		bool nullable(row.string_at(2) == "f");
		DefaultType default_type(DefaultType::no_default);
		string default_value;

		if (row.string_at(3) == "t") {
			default_type = DefaultType::default_value;
			default_value = row.string_at(4);

			if (default_value.length() > 20 &&
				default_value.substr(0, 9) == "nextval('" &&
				default_value.substr(default_value.length() - 12, 12) == "'::regclass)") {
				default_type = DefaultType::sequence;
				default_value = "";

			} else if (default_value.length() > 2 && default_value[0] == '\'') {
				default_value = unescape_value(default_value.substr(1, default_value.rfind('\'') - 1));

			} else if (default_value.length() > 0 && (default_value[0] < '0' || default_value[0] > '9') && default_value != "false" && default_value != "true") {
				default_type = DefaultType::default_function;

				// postgresql converts CURRENT_TIMESTAMP to now(); convert it back for portability
				if (default_value == "now()") {
					default_value = "CURRENT_TIMESTAMP";

				// do the same for its conversion of CURRENT_DATE
				} else if (default_value == "('now'::text)::date") {
					default_value = "CURRENT_DATE";

				// other SQL-reserved zero-argument functions come back with quoted identifiers and brackets, see Note on the
				// 'System Information Functions' page; the list here is shorter because some get converted to one of the others by pg
				} else if (default_value == "\"current_schema\"()" || default_value == "\"current_user\"()" || default_value == "\"session_user\"()") {
					default_value = default_value.substr(1, default_value.length() - 4);
				}
			}
		}

		if (db_type == "boolean") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BOOL);
		} else if (db_type == "smallint") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::SINT, 2);
		} else if (db_type == "integer") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::SINT, 4);
		} else if (db_type == "bigint") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::SINT, 8);
		} else if (db_type == "real") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::REAL, 4);
		} else if (db_type == "double precision") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::REAL, 8);
		} else if (db_type.substr(0, 8) == "numeric(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DECI, extract_column_length(db_type), extract_column_scale(db_type));
		} else if (db_type.substr(0, 18) == "character varying(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::VCHR, extract_column_length(db_type));
		} else if (db_type.substr(0, 10) == "character(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::FCHR, extract_column_length(db_type));
		} else if (db_type == "text") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TEXT);
		} else if (db_type == "bytea") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BLOB);
		} else if (db_type == "uuid") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::UUID);
		} else if (db_type == "date") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DATE);
		} else if (db_type == "time without time zone") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TIME);
		} else if (db_type == "time with time zone") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TIME, 0, 0, ColumnFlags::time_zone);
		} else if (db_type == "timestamp without time zone") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DTTM);
		} else if (db_type == "timestamp with time zone") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DTTM, 0, 0, ColumnFlags::time_zone);
		} else {
			// not supported, but leave it till sync_to's check_tables_usable to complain about it so that it can be ignored
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::UNKN, 0, 0, ColumnFlags::nothing, db_type);
		}
	}

	inline string unescape_value(const string &escaped) {
		string result;
		result.reserve(escaped.length());
		for (string::size_type n = 0; n < escaped.length(); n++) {
			// this is by no means a complete unescaping function, it only handles the cases seen in
			// the output of pg_get_expr so far
			if (escaped[n] == '\\' || escaped[n] == '\'') {
				n += 1;
			}
			result += escaped[n];
		}
		return result;
	}

	Table &table;
};

struct PostgreSQLPrimaryKeyLister {
	inline PostgreSQLPrimaryKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string column_name = row.string_at(0);
		size_t column_index = table.index_of_column(column_name);
		table.primary_key_columns.push_back(column_index);
	}

	Table &table;
};

struct PostgreSQLKeyLister {
	inline PostgreSQLKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		// if we have no primary key, we might need to use another unique key as a surrogate - see PostgreSQLTableLister below
		// furthermore this key must have no NULLable columns, as they effectively make the index not unique
		string key_name = row.string_at(0);
		bool unique = (row.string_at(1) == "t");
		string column_name = row.string_at(2);
		size_t column_index = table.index_of_column(column_name);
		// FUTURE: consider representing collation, index type, partial keys etc.

		if (table.keys.empty() || table.keys.back().name != key_name) {
			table.keys.push_back(Key(key_name, unique));
		}
		table.keys.back().columns.push_back(column_index);

		if (table.primary_key_columns.empty()) {
			// if we have no primary key, we might need to use another unique key as a surrogate - see PostgreSQLTableLister below -
			// but this key must have no NULLable columns, as they effectively make the index not unique
			bool nullable = (row.string_at(3) == "f");
			if (unique && nullable) {
				// mark this as unusable
				unique_but_nullable_keys.insert(key_name);
			}
		}
	}

	Table &table;
	set<string> unique_but_nullable_keys;
};

struct PostgreSQLTableLister {
	PostgreSQLTableLister(PostgreSQLClient &client, Database &database): client(client), database(database) {}

	void operator()(PostgreSQLRow &row) {
		Table table(row.string_at(0));

		PostgreSQLColumnLister column_lister(table);
		client.query(
			"SELECT attname, format_type(atttypid, atttypmod), attnotnull, atthasdef, pg_get_expr(adbin, adrelid) "
			  "FROM pg_attribute "
			  "JOIN pg_class ON attrelid = pg_class.oid "
			  "JOIN pg_type ON atttypid = pg_type.oid "
			  "LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum "
			 "WHERE attnum > 0 AND "
			       "NOT attisdropped AND "
			       "relname = '" + table.name + "' "
			 "ORDER BY attnum",
			column_lister);

		PostgreSQLPrimaryKeyLister primary_key_lister(table);
		client.query(
			"SELECT column_name "
			  "FROM information_schema.table_constraints, "
			       "information_schema.key_column_usage "
			 "WHERE information_schema.table_constraints.table_name = '" + table.name + "' AND "
			       "information_schema.key_column_usage.table_name = information_schema.table_constraints.table_name AND "
			       "constraint_type = 'PRIMARY KEY' "
			 "ORDER BY ordinal_position",
			primary_key_lister);

		PostgreSQLKeyLister key_lister(table);
		client.query(
			"SELECT indexname, indisunique, attname, attnotnull "
			  "FROM (SELECT table_class.oid AS table_oid, index_class.relname AS indexname, pg_index.indisunique, generate_series(1, array_length(indkey, 1)) AS position, unnest(indkey) AS attnum "
			          "FROM pg_class table_class, pg_class index_class, pg_index "
			         "WHERE table_class.relname = '" + table.name + "' AND "
			               "table_class.relkind = 'r' AND "
			               "index_class.relkind = 'i' AND "
			               "pg_index.indrelid = table_class.oid AND "
			               "pg_index.indexrelid = index_class.oid AND "
			               "NOT pg_index.indisprimary) index_attrs,"
			       "pg_attribute "
			 "WHERE pg_attribute.attrelid = table_oid AND "
			       "pg_attribute.attnum = index_attrs.attnum "
			 "ORDER BY indexname, index_attrs.position",
			key_lister);

		// if the table has no primary key, we need to find a unique key with no nullable columns to act as a surrogate primary key
		// of course this falls apart if there are no unique keys, so we don't allow that - we let sync_to's compare_schema() handle that so it can tell the user
		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name
		
		for (Keys::const_iterator key = table.keys.begin(); key != table.keys.end() && table.primary_key_columns.empty(); ++key) {
			if (key->unique && !key_lister.unique_but_nullable_keys.count(key->name)) {
				table.primary_key_columns = key->columns;
			}
		}

		database.tables.push_back(table);
	}

	PostgreSQLClient &client;
	Database &database;
};

void PostgreSQLClient::populate_database_schema(Database &database) {
	PostgreSQLTableLister table_lister(*this, database);
	query("SELECT tablename "
		    "FROM pg_tables "
		   "WHERE schemaname = ANY (current_schemas(false)) "
		   "ORDER BY pg_relation_size(tablename::text) DESC, tablename ASC",
		  table_lister);
}


int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
