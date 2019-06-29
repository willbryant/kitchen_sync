#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <mysql.h>

#include "schema.h"
#include "database_client_traits.h"
#include "sql_functions.h"
#include "row_printer.h"
#include "ewkb.h"

#define MYSQL_5_6_5 50605
#define MYSQL_5_7_8 50708
#define MYSQL_8_0_0 80000
#define MARIADB_10_0_0 100000
#define MARIADB_10_2_7 100207

enum MySQLColumnConversion {
	encode_raw,
	encode_bool,
	encode_uint,
	encode_sint,
	encode_geom,
};

class MySQLRes {
public:
	MySQLRes(MYSQL &mysql, bool buffer);
	~MySQLRes();

	inline MYSQL_RES *res() { return _res; }
	inline int n_tuples() const { return mysql_num_rows(_res); } // if buffer was false, this will only work after reading all the rows
	inline int n_columns() const { return _n_columns; }
	inline MySQLColumnConversion conversion_for(int column_number) { if (conversions.empty()) populate_conversions(); return conversions[column_number]; }
	string qualified_name_of_column(int column_number);

private:
	void populate_conversions();
	MySQLColumnConversion conversion_for_field(const MYSQL_FIELD &field);

	MYSQL_RES *_res;
	int _n_columns;
	MYSQL_FIELD *_fields;
	vector<MySQLColumnConversion> conversions;
};

MySQLRes::MySQLRes(MYSQL &mysql, bool buffer) {
	_res = buffer ? mysql_store_result(&mysql) : mysql_use_result(&mysql);
	_n_columns = mysql_num_fields(_res);
	_fields = mysql_fetch_fields(_res);
}

MySQLRes::~MySQLRes() {
	if (_res) {
		while (mysql_fetch_row(_res)) ; // must throw away any remaining results or else they'll be returned in the next requested resultset!
		mysql_free_result(_res);
	}
}

void MySQLRes::populate_conversions() {
	conversions.resize(_n_columns);

	for (size_t i = 0; i < _n_columns; i++) {
		conversions[i] = conversion_for_field(_fields[i]);
	}
}

MySQLColumnConversion MySQLRes::conversion_for_field(const MYSQL_FIELD &field) {
	switch (field.type) {
		case MYSQL_TYPE_TINY:
			if (field.length == 1) {
				return encode_bool;
			} // else [[fallthrough]];

		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
			if (field.flags & UNSIGNED_FLAG) {
				return encode_uint;
			} else {
				return encode_sint;
			}
			break;

		case MYSQL_TYPE_GEOMETRY:
			return encode_geom;

		default:
			return encode_raw;
	}
}

string MySQLRes::qualified_name_of_column(int column_number) {
	return string(_fields[column_number].table) + "." + string(_fields[column_number].name);
}


class MySQLRow {
public:
	inline MySQLRow(MySQLRes &res, MYSQL_ROW row): _res(res), _row(row) { _lengths = mysql_fetch_lengths(_res.res()); }
	inline const MySQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }

	inline        bool   null_at(int column_number) const { return     _row[column_number] == nullptr; }
	inline const char *result_at(int column_number) const { return     _row[column_number]; }
	inline         int length_of(int column_number) const { return _lengths[column_number]; }
	inline      string string_at(int column_number) const { return string(result_at(column_number), length_of(column_number)); }
	inline     int64_t    int_at(int column_number) const { return strtoll(result_at(column_number), nullptr, 10); }
	inline    uint64_t   uint_at(int column_number) const { return strtoull(result_at(column_number), nullptr, 10); }

	template <typename Packer>
	inline void pack_column_into(Packer &packer, int column_number) const {
		if (null_at(column_number)) {
			packer << nullptr;
		} else {
			switch (_res.conversion_for(column_number)) {
				case encode_bool:
					// although we have made the assumption in this codebase that tinyint(1) is used only for booleans,
					// in fact even if a field is tinyint(1) any tinyint value can be stored, even those with multiple digits,
					// so we check our assumption here and raise an error rather than risk silently syncing incorrectly
					if (length_of(column_number) == 1) {
						if (*result_at(column_number) == '0') {
							packer << false;
							break;
						} else if (*result_at(column_number) == '1') {
							packer << true;
							break;
						}
					}
					throw runtime_error("Invalid value for boolean column " + _res.qualified_name_of_column(column_number) + ": " + string_at(column_number) + " (we assume tinyint(1) is used for booleans)");

				case encode_uint:
					packer << uint_at(column_number);
					break;

				case encode_sint:
					packer << int_at(column_number);
					break;

				case encode_geom:
					packer << mysql_bin_to_ewkb_bin(result_at(column_number), length_of(column_number));
					break;

				case encode_raw:
					packer << uncopied_byte_string(result_at(column_number), length_of(column_number));
					break;
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
	MySQLRes &_res;
	MYSQL_ROW _row;
	unsigned long *_lengths;
};


class MySQLClient: public SupportsReplace, public SupportsAddNonNullableColumns {
public:
	typedef MySQLRow RowType;

	MySQLClient(
		const string &database_host,
		const string &database_port,
		const string &database_name,
		const string &database_username,
		const string &database_password,
		const string &variables);
	~MySQLClient();

	void disable_referential_integrity();
	void enable_referential_integrity();
	string export_snapshot();
	void import_snapshot(const string &snapshot);
	void unhold_snapshot();
	bool supports_explicit_read_only_transactions();
	void start_read_transaction();
	void start_write_transaction();
	void commit_transaction();
	void rollback_transaction();
	void populate_database_schema(Database &database);
	void convert_unsupported_database_schema(Database &database);

	inline string quote_identifier(const string &name) { return ::quote_identifier(name, '`'); };
	string escape_string_value(const string &value);
	string &append_escaped_generic_value_to(string &result, const string &value);
	string &append_escaped_spatial_value_to(string &result, const string &value);
	string &append_escaped_json_value_to(string &result, const string &value);
	string &append_escaped_column_value_to(string &result, const Column &column, const string &value);
	tuple<string, string> column_type(const Column &column);
	string column_default(const Table &table, const Column &column);
	string column_definition(const Table &table, const Column &column);
	string key_definition(const Table &table, const Key &key);

	inline ColumnFlags supported_flags() const { return (ColumnFlags)(mysql_timestamp | mysql_on_update_timestamp); }
	inline bool information_schema_column_default_shows_escaped_expressions() const { return (server_is_mariadb && server_version >= MARIADB_10_2_7); }
	inline bool supports_srid_settings_on_columns() const { return (!server_is_mariadb && server_version >= MYSQL_8_0_0); }
	inline bool supports_check_constraints() const { return check_constraints_table_exists; }
	inline bool explicit_json_column_type() const { return (!server_is_mariadb && server_version >= MYSQL_5_7_8); }

	size_t execute(const string &sql);
	string select_one(const string &sql);

	template <typename RowFunction>
	size_t query(const string &sql, RowFunction &row_handler, bool buffer = false) {
		if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
			throw runtime_error(sql_error(sql));
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
			throw runtime_error(sql_error(sql));
		}

		return res.n_tuples();
	}

protected:
	string sql_error(const string &sql);

private:
	MYSQL mysql;
	bool server_is_mariadb;
	bool check_constraints_table_exists;
	unsigned long server_version;

	// forbid copying
	MySQLClient(const MySQLClient &_) = delete;
	MySQLClient &operator=(const MySQLClient &_) = delete;
};

MySQLClient::MySQLClient(
	const string &database_host,
	const string &database_port,
	const string &database_name,
	const string &database_username,
	const string &database_password,
	const string &variables) {

	// mysql_real_connect takes separate params for numeric ports and unix domain sockets
	int port = 0;
	const char *socket = nullptr;
	if (!database_port.empty()) {
		if (database_port.front() >= '0' && database_port.front() <= '9') {
			port = atoi(database_port.c_str());
		} else {
			socket = database_port.c_str();
		}
	}

	mysql_init(&mysql);
	mysql_options(&mysql, MYSQL_READ_DEFAULT_GROUP, "ks_mysql");
	mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "binary");
	if (!mysql_real_connect(&mysql, database_host.c_str(), database_username.c_str(), database_password.c_str(), database_name.c_str(), port, socket, 0)) {
		throw runtime_error(mysql_error(&mysql));
	}

	// increase the timeouts so that the connection doesn't get killed while trying to write large rowsets to the client over slow pipes
	execute("SET SESSION net_read_timeout = CAST(GREATEST(@@net_read_timeout, 600) AS UNSIGNED), net_write_timeout = CAST(GREATEST(@@net_write_timeout, 600) AS UNSIGNED), long_query_time = CAST(GREATEST(@@long_query_time, 600) AS UNSIGNED), sql_mode = 'traditional,pipes_as_concat'");

	if (!variables.empty()) {
		execute("SET " + variables);
	}

	server_is_mariadb = strstr(mysql_get_server_info(&mysql), "MariaDB") != nullptr;
	server_version = mysql_get_server_version(&mysql);
	check_constraints_table_exists = atoi(select_one("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='INFORMATION_SCHEMA' AND TABLE_NAME='CHECK_CONSTRAINTS'").c_str());
}

MySQLClient::~MySQLClient() {
	mysql_close(&mysql);
}

size_t MySQLClient::execute(const string &sql) {
	if (mysql_real_query(&mysql, sql.c_str(), sql.size())) {
		throw runtime_error(sql_error(sql));
	}

	return mysql_affected_rows(&mysql);
}

string MySQLClient::select_one(const string &sql) {
	if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
		throw runtime_error(sql_error(sql));
	}

	MySQLRes res(mysql, true);

	if (res.n_tuples() != 1 || res.n_columns() != 1) {
		throw runtime_error("Expected query to return only one row with only one column\n" + sql);
	}

	return MySQLRow(res, mysql_fetch_row(res.res())).string_at(0);
}

string MySQLClient::sql_error(const string &sql) {
	if (sql.size() < 200) {
		return mysql_error(&mysql) + string("\n") + sql;
	} else {
		return mysql_error(&mysql) + string("\n") + sql.substr(0, 200) + "...";
	}
}


bool MySQLClient::supports_explicit_read_only_transactions() {
	if (server_is_mariadb) {
		return (server_version >= MARIADB_10_0_0);
	} else {
		return (server_version >= MYSQL_5_6_5);
	}
}

void MySQLClient::start_read_transaction() {
	execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	if (supports_explicit_read_only_transactions()) {
		execute("START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT");
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
	execute("FLUSH NO_WRITE_TO_BINLOG TABLES"); // wait for current update statements to finish, without blocking other connections
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

string MySQLClient::escape_string_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = mysql_real_escape_string(&mysql, (char*)result.data(), value.c_str(), value.size());
	result.resize(result_length);
	return result;
}

string &MySQLClient::append_escaped_generic_value_to(string &result, const string &value) {
	string buffer;
	buffer.resize(value.size()*2 + 1);
	size_t result_length = mysql_real_escape_string(&mysql, (char*)buffer.data(), value.c_str(), value.size());
	result += '\'';
	result.append(buffer, 0, result_length);
	result += '\'';
	return result;
}

string &MySQLClient::append_escaped_spatial_value_to(string &result, const string &value) {
	return append_escaped_generic_value_to(result, ewkb_bin_to_mysql_bin(value));
}

string &MySQLClient::append_escaped_json_value_to(string &result, const string &value) {
	// normally you wouldn't have to do anything special to be able to insert into a JSON field.
	// but we set the MYSQL_SET_CHARSET_NAME option to "binary" to avoid having to interpret
	// character sets, and unfortunately they added an explicit check which causes
	// "Cannot create a JSON value from a string with CHARACTER SET 'binary'" errors, so we have
	// to tell it to convert our "binary" string (which should already actually be UTF-8);
	// conversion from binary effectively just changes the encoding setting, it doesn't actually
	// convert anything.  note that mysql has defined the JSON type as always having encoding
	// utf8mb4 - it isn't like varchar and text.
	result += "CONVERT(";
	append_escaped_generic_value_to(result, value);
	result += " USING utf8mb4)";
	return result;
}

string &MySQLClient::append_escaped_column_value_to(string &result, const Column &column, const string &value) {
	if (column.column_type == ColumnTypes::SPAT) {
		return append_escaped_spatial_value_to(result, value);
	} else if (column.column_type == ColumnTypes::JSON && explicit_json_column_type()) {
		return append_escaped_json_value_to(result, value);
	} else {
		return append_escaped_generic_value_to(result, value);
	}
}

void MySQLClient::convert_unsupported_database_schema(Database &database) {
	for (Table &table : database.tables) {
		for (Column &column : table.columns) {
			// postgresql allows numeric with no precision or scale specification and preserves the given input data
			// up to an implementation-defined precision and scale limit; mysql doesn't, and silently converts
			// `numeric` to `numeric(10, 0)`.  lacking any better knowledge, we follow their lead and do the same
			// conversion here, just so that the schema matcher sees that the column definition is already the same.
			if (column.column_type == ColumnTypes::DECI && !column.size && !column.scale) {
				column.size = 10;
			}

			// postgresql treats no default and DEFAULT NULL as separate things, even though they behave much the same.
			// although mysql would happily accept the DEFAULT NULL strings we would produce below, we want to go ahead
			// and convert it here so that the schema matcher also sees them as the same thing.
			if (column.default_type == DefaultType::default_expression && column.default_value == "NULL") {
				column.default_type = DefaultType::no_default;
				column.default_value = "";
			}
		}
	}
}

tuple<string, string> MySQLClient::column_type(const Column &column) {
	if (column.column_type == ColumnTypes::BLOB) {
		switch (column.size) {
			case 1:
				return make_tuple("tinyblob", "");

			case 2:
				return make_tuple("blob", "");

			case 3:
				return make_tuple("mediumblob", "");

			default:
				return make_tuple("longblob", "");
		}

	} else if (column.column_type == ColumnTypes::TEXT) {
		switch (column.size) {
			case 1:
				return make_tuple("tinytext", "");

			case 2:
				return make_tuple("text", "");

			case 3:
				return make_tuple("mediumtext", "");

			default:
				return make_tuple("longtext", "");
		}

	} else if (column.column_type == ColumnTypes::VCHR) {
		string result("varchar");
		if (column.size > 0) {
			// mysql never uses VCHR without a length specification, but you can see it from postgresql; we don't know what length to make the column because the maximum mysql permits depends on the character set and what other columns there are on the row, so we just let mysql take the default
			result += '(';
			result += to_string(column.size);
			result += ')';
		}
		return make_tuple(result, "");

	} else if (column.column_type == ColumnTypes::FCHR) {
		string result("char(");
		result += to_string(column.size);
		result += ")";
		return make_tuple(result, "");

	} else if (column.column_type == ColumnTypes::UUID) {
		// mysql and mariadb still don't have a proper UUID type (may come in mariadb 10.5), so we convert UUIDs
		// to the equivalent strings. this is obviously not the only choice - packed binary columns would be more
		// efficient - but we'd need to know about the application specifics to make any other choice (eg. for
		// binary, have the bytes been swapped using the option for timestamp UUIDs?).  we somewhat arbitrarily
		// use char - we could use varchar, since char is more old-fashioned, but since UUIDs are all the same
		// length char seems more logical.
		return make_tuple("char(36)", " COMMENT 'UUID'");

	} else if (column.column_type == ColumnTypes::JSON) {
		// mysql has a proper json type (5.7.8+), but mariadb doesn't, instead they aliased json to longtext (10.2.7+)
		// and later added a json_valid check constraint (10.4.3+) when asking for the json column type.  we do this
		// ourselves so that we can get the same behavior when running our tests against earlier versions.  note that
		// check constraints themselves were only added in mariadb 10.2 (and mysql 8, but we don't need them there
		// since mysql 8 has the actual column type), but earlier versions accepted the syntax (and ignored them).
		// part 2 is in the column_definition() function because we need to put the NOT NULL bit in first.  (we also
		// support using a column comment as a marker on older versions which don't support check constraints.)
		if (explicit_json_column_type()) {
			return make_tuple("json", "");
		} else if (supports_check_constraints()) {
			return make_tuple("longtext", " CHECK (json_valid(" + quote_identifier(column.name) + "))");
		} else {
			return make_tuple("longtext", " COMMENT 'JSON'");
		}

	} else if (column.column_type == ColumnTypes::BOOL) {
		return make_tuple("tinyint(1)", "");

	} else if (column.column_type == ColumnTypes::SINT || column.column_type == ColumnTypes::UINT) {
		string result;

		switch (column.size) {
			case 1:
				result = "tinyint";
				break;

			case 2:
				result = "smallint";
				break;

			case 3:
				result = "mediumint";
				break;

			case 4:
				result = "int";
				break;

			default:
				result = "bigint";
		}

		if (column.column_type == ColumnTypes::UINT) {
			result += " unsigned";
		}

		return make_tuple(result, "");

	} else if (column.column_type == ColumnTypes::REAL) {
		return make_tuple(column.size == 4 ? "float" : "double", "");

	} else if (column.column_type == ColumnTypes::DECI) {
		string result("decimal(");
		result += to_string(column.size);
		result += ',';
		result += to_string(column.scale);
		result += ')';
		return make_tuple(result, "");

	} else if (column.column_type == ColumnTypes::DATE) {
		return make_tuple("date", "");

	} else if (column.column_type == ColumnTypes::TIME) {
		return make_tuple("time", "");

	} else if (column.column_type == ColumnTypes::DTTM) {
		if (column.flags & ColumnFlags::mysql_timestamp) {
			return make_tuple("timestamp", "");
		} else {
			return make_tuple("datetime", "");
		}

	} else if (column.column_type == ColumnTypes::SPAT) {
		string result(column.type_restriction.empty() ? string("geometry") : column.type_restriction);

		if (!column.reference_system.empty() && supports_srid_settings_on_columns()) {
			// nb. mysql outputs this after options like 'NOT NULL', but it accepts it here too; it also
			// outputs it using the special backwards-compatible /*! syntax, which we don't use since we
			// need the version detection code for the information_schema query anyway.
			result += " SRID ";
			result += column.reference_system;
		}

		return make_tuple(result, "");

	} else {
		throw runtime_error("Don't know how to express column type of " + column.name + " (" + column.column_type + ")");
	}
}

string MySQLClient::column_default(const Table &table, const Column &column) {
	switch (column.default_type) {
		case DefaultType::no_default:
			return " DEFAULT NULL";

		case DefaultType::sequence:
			return " AUTO_INCREMENT";

		case DefaultType::default_value: {
			string result(" DEFAULT ");
			if (column.column_type == ColumnTypes::BOOL ||
				column.column_type == ColumnTypes::SINT ||
				column.column_type == ColumnTypes::UINT ||
				column.column_type == ColumnTypes::REAL ||
				column.column_type == ColumnTypes::DECI) {
				result += column.default_value;
			} else {
				append_escaped_column_value_to(result, column, column.default_value);
			}
			return result;
		}

		case DefaultType::default_expression:
			if (column.default_value == "CURRENT_TIMESTAMP") {
				return " DEFAULT " + column.default_value; // the only expression accepted prior to support for arbitrary expressions
			} else {
				return " DEFAULT (" + column.default_value + ")"; // mysql requires brackets around the expression; mariadb accepts and ignores them
			}

		default:
			throw runtime_error("Don't know how to express default of " + column.name + " (" + to_string(column.default_type) + ")");
	}
}

string MySQLClient::column_definition(const Table &table, const Column &column) {
	string result;
	result += quote_identifier(column.name);
	result += ' ';

	string type, extra;
	tie(type, extra) = column_type(column);
	result += type;

	if (!column.nullable) {
		result += " NOT NULL";
	} else if (column.flags & ColumnFlags::mysql_timestamp) {
		result += " NULL";
	}

	// used for JSON etc. which need to add stuff after the NOT NULL bit in some cases; see discussion in column_type()
	if (!extra.empty()) {
		result += extra;
	}

	if (column.default_type) {
		result += column_default(table, column);
	}

	if (column.flags & mysql_on_update_timestamp) {
		result += " ON UPDATE CURRENT_TIMESTAMP";
	}

	return result;
}

string MySQLClient::key_definition(const Table &table, const Key &key) {
	string result;
	switch (key.key_type) {
		case standard_key:
			result = "CREATE INDEX";
			break;

		case unique_key:
			result = "CREATE UNIQUE INDEX";
			break;

		case spatial_key:
			result = "CREATE SPATIAL INDEX";
			break;
	}
	result += quote_identifier(key.name);
	result += " ON ";
	result += quote_identifier(table.name);
	result += ' ';
	result += columns_tuple(*this, table.columns, key.columns);
	return result;
}

struct MySQLColumnLister {
	inline MySQLColumnLister(Table &table, bool information_schema_column_default_shows_escaped_expressions): table(table), information_schema_column_default_shows_escaped_expressions(information_schema_column_default_shows_escaped_expressions) {}

	inline void operator()(MySQLRow &row) {
		string name(row.string_at(0));
		string db_type(row.string_at(1));
		bool nullable(row.string_at(2) == "YES");
		bool unsign(db_type.length() > 8 && db_type.substr(db_type.length() - 8, 8) == "unsigned");
		DefaultType default_type(row.null_at(3) ? DefaultType::no_default : DefaultType::default_value);
		string default_value(default_type ? row.string_at(3) : string(""));
		string extra(row.string_at(4));

		if (!row.null_at(3)) {
			if (information_schema_column_default_shows_escaped_expressions) {
				// mariadb 10.2.7 and above always represent the default value as an expression, which we want to turn back into a plain value if it is one (for compatibility)
				if (default_value == "NULL") {
					default_type = DefaultType::no_default;
					default_value.clear();
				} else if (default_value.length() >= 2 && default_value[0] == '\'' && default_value[default_value.length() - 1] == '\'') {
					default_value = unescape_string_value(default_value.substr(1, default_value.length() - 2));
				} else if (!default_value.empty() && default_value.find_first_not_of("0123456789.") != string::npos) {
					default_type = DefaultType::default_expression;
				}
			} else {
				// mysql 8.0 and above show literal strings and expressions the same way in the COLUMN_DEFAULT field, but set the EXTRA field to DEFAULT_GENERATED for expressions
				if (extra.find("DEFAULT_GENERATED") != string::npos) {
					default_type = DefaultType::default_expression;
				}
			}
		}

		if (extra.find("auto_increment") != string::npos) {
			default_type = DefaultType::sequence;
		}

		if (db_type == "tinyint(1)") {
			if (default_type) {
				if (default_value == "0") {
					default_value = "false";
				} else if (default_value == "1") {
					default_value = "true";
				} else {
					throw runtime_error("Invalid default value for boolean column " + table.name + "." + name + ": " + default_value + " (we assume tinyint(1) is used for booleans)");
				}
			}
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BOOL);
		} else if (db_type.substr(0, 8) == "tinyint(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, unsign ? ColumnTypes::UINT : ColumnTypes::SINT, 1);
		} else if (db_type.substr(0, 9) == "smallint(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, unsign ? ColumnTypes::UINT : ColumnTypes::SINT, 2);
		} else if (db_type.substr(0, 10) == "mediumint(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, unsign ? ColumnTypes::UINT : ColumnTypes::SINT, 3);
		} else if (db_type.substr(0, 4) == "int(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, unsign ? ColumnTypes::UINT : ColumnTypes::SINT, 4);
		} else if (db_type.substr(0, 7) == "bigint(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, unsign ? ColumnTypes::UINT : ColumnTypes::SINT, 8);
		} else if (db_type.substr(0, 8) == "decimal(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DECI, extract_column_length(db_type), extract_column_scale(db_type));
		} else if (db_type == "float") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::REAL, 4);
		} else if (db_type == "double") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::REAL, 8);
		} else if (db_type.substr(0, 8) == "varchar(") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::VCHR, extract_column_length(db_type));
		} else if (db_type.substr(0, 5) == "char(") {
			size_t length = extract_column_length(db_type);
			if (length == 36 && !row.null_at(6) && row.string_at(6).substr(0, 4) == "UUID") {
				table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::UUID);
			} else {
				while (default_type && default_value.length() < length) default_value += ' ';
				table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::FCHR, length);
			}
		} else if (db_type == "tinytext") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TEXT, 1);
		} else if (db_type == "text") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TEXT, 2);
		} else if (db_type == "mediumtext") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TEXT, 3);
		} else if (db_type == "longtext") {
			if ((!row.null_at(6) && row.string_at(6).substr(0, 4) == "JSON") || (!row.null_at(7) && row.int_at(7))) { // 6 is the comment and 7 the check constraint; the check constraint is the canonical way to handle JSON in mariadb, but we also support using the comment for the sake of older versions (mariadb 10.1 and below, mysql 5.5)
				table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::JSON);
			} else {
				table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TEXT); // no specific size for compatibility, but 4 in current mysql
			}
		} else if (db_type == "json") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::JSON);
		} else if (db_type == "tinyblob") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BLOB, 1);
		} else if (db_type == "blob") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BLOB, 2);
		} else if (db_type == "mediumblob") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BLOB, 3);
		} else if (db_type == "longblob") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::BLOB); // no specific size for compatibility, but 4 in current mysql
		} else if (db_type == "date") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DATE);
		} else if (db_type == "time") {
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::TIME);
		} else if (db_type == "datetime" || db_type == "timestamp") {
			ColumnFlags flags = db_type == "timestamp" ? ColumnFlags::mysql_timestamp : ColumnFlags::nothing;
			if (default_value == "CURRENT_TIMESTAMP" || default_value == "current_timestamp()") {
				default_type = DefaultType::default_expression;
				default_value = "CURRENT_TIMESTAMP";
			}
			if (extra.find("on update CURRENT_TIMESTAMP") != string::npos || extra.find("on update current_timestamp()") != string::npos) {
				flags = (ColumnFlags)(flags | mysql_on_update_timestamp);
			}
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::DTTM, 0, 0, flags);
		} else if (db_type == "geometry" || db_type == "point" || db_type == "linestring" || db_type == "polygon" || db_type == "geometrycollection" || db_type == "multipoint" || db_type == "multilinestring" || db_type == "multipolygon") {
			string type_restriction(db_type);
			if (type_restriction == "geometry") type_restriction.clear();
			string reference_system;
			if (!row.null_at(5)) reference_system = row.string_at(5);
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::SPAT, 0, 0, ColumnFlags::nothing, type_restriction, reference_system);
		} else {
			// not supported, but leave it till sync_to's check_tables_usable to complain about it so that it can be ignored
			table.columns.emplace_back(name, nullable, default_type, default_value, ColumnTypes::UNKN, 0, 0, ColumnFlags::nothing, "", "", db_type);
		}
	}

	inline string unescape_string_value(const string &escaped) {
		// there's no library API to do this, so we do it ourselves.  used to parse column default values.
		string result;
		result.reserve(escaped.length());
		for (size_t pos = 0; pos < escaped.length(); pos++) {
			char c = escaped[pos];
			if (c == '\'') {
				pos++;
			} else if (c == '\\') {
				switch (c = escaped[++pos]) {
					case '0': c = '\0'; break;
					case 'b': c = '\b'; break;
					case 'r': c = '\r'; break;
					case 'n': c = '\n'; break;
					case 't': c = '\t'; break;
				}
			}
			result += c;
		}
		return result;
	}

	Table &table;
	bool information_schema_column_default_shows_escaped_expressions;
};

struct MySQLKeyLister {
	inline MySQLKeyLister(Table &table): table(table) {}

	inline void operator()(MySQLRow &row) {
		string key_name(row.string_at(2));
		string column_name(row.string_at(4));
		size_t column_index(table.index_of_column(column_name));
		// FUTURE: consider representing collation, sub_part, packed, and perhaps comment/index_comment

		if (key_name == "PRIMARY") {
			// there is of course only one primary key; we get a row for each column it includes
			table.primary_key_columns.push_back(column_index);
			table.primary_key_type = explicit_primary_key;

		} else {
			// a column in a generic key, which may or may not be unique
			if (table.keys.empty() || table.keys.back().name != key_name) {
				KeyType key_type(standard_key);
				if (row.string_at(1) == "0") {
					key_type = unique_key;
				} else if (row.string_at(10) == "SPATIAL") {
					key_type = spatial_key;
				}
				table.keys.push_back(Key(key_name, key_type));
			}
			table.keys.back().columns.push_back(column_index);
		}
	}

	Table &table;
};

struct MySQLTableLister {
	inline MySQLTableLister(MySQLClient &client, Database &database): client(client), database(database) {}

	inline void operator()(MySQLRow &row) {
		Table table(row.string_at(0));

		MySQLColumnLister column_lister(table, client.information_schema_column_default_shows_escaped_expressions());
		client.query("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, " + srid_column() + " AS SRS_ID, COLUMN_COMMENT, " + json_check_constraint_expression() + " AS JSON_CHECK_CONSTRAINT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = SCHEMA() AND TABLE_NAME = '" + client.escape_string_value(table.name) + "' ORDER BY ORDINAL_POSITION", column_lister);

		MySQLKeyLister key_lister(table);
		client.query("SHOW KEYS FROM " + client.quote_identifier(table.name), key_lister);
		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name

		database.tables.push_back(table);
	}

	inline string srid_column() {
		return (client.supports_srid_settings_on_columns() ? "SRS_ID" : "NULL");
	}

	inline string json_check_constraint_expression() {
		if (client.explicit_json_column_type() || !client.supports_check_constraints()) {
			return "NULL";
		} else {
			return "DATA_TYPE = 'longtext' AND EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = SCHEMA() AND CONSTRAINT_NAME = COLUMN_NAME AND CHECK_CONSTRAINTS.TABLE_NAME = COLUMNS.TABLE_NAME AND REPLACE(CHECK_CLAUSE, '`', '') = CONCAT('json_valid(', COLUMN_NAME, ')'))";
		}
	}

private:
	MySQLClient &client;
	Database &database;
};

void MySQLClient::populate_database_schema(Database &database) {
	MySQLTableLister table_lister(*this, database);
	query(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = schema() AND table_type = \"BASE TABLE\" ORDER BY data_length DESC, table_name ASC",
		table_lister,
		true /* buffer so we can make further queries during iteration */);
}


int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
