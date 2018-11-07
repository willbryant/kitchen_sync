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
	encode_dttm,
	encode_time,
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

		case MYSQL_TYPE_DATETIME:
			return encode_dttm;

		case MYSQL_TYPE_TIME:
			return encode_time;

		case MYSQL_TYPE_GEOMETRY:
			return encode_geom;

		default:
			return encode_raw;
	}
}

string MySQLRes::qualified_name_of_column(int column_number) {
	return string(_fields[column_number].table) + "." + string(_fields[column_number].name);
}


size_t length_of_value_after_trimming_fractional_zeros(const char *str, size_t length, size_t decimal_point_at) {
	if (length <= decimal_point_at || str[decimal_point_at] != '.') return length;
	while (true) {
		switch (str[length - 1]) {
			case '0':
				length--;
				break;

			case '.':
				return length - 1;

			default:
				return length;
		}
	}
}

inline size_t length_of_datetime_value_after_trimming_fractional_zeros(const char *str, size_t length) {
	return length_of_value_after_trimming_fractional_zeros(str, length, 19);
}

inline size_t length_of_time_value_after_trimming_fractional_zeros(const char *str, size_t length) {
	return length_of_value_after_trimming_fractional_zeros(str, length, 8);
}

inline string datetime_value_after_trimming_fractional_zeros(const string &str) {
	return string(str.data(), length_of_datetime_value_after_trimming_fractional_zeros(str.data(), str.length()));
}

inline string time_value_after_trimming_fractional_zeros(const string &str) {
	return string(str.data(), length_of_time_value_after_trimming_fractional_zeros(str.data(), str.length()));
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

				case encode_dttm:
					packer << uncopied_byte_string(result_at(column_number), length_of_datetime_value_after_trimming_fractional_zeros(result_at(column_number), length_of(column_number)));
					break;

				case encode_time:
					packer << uncopied_byte_string(result_at(column_number), length_of_time_value_after_trimming_fractional_zeros(result_at(column_number), length_of(column_number)));
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

	ColumnTypeList supported_types();
	void populate_database_schema(Database &database, const ColumnTypeList &accepted_types);
	void convert_unsupported_database_schema(Database &database);

	inline string quote_identifier(const string &name) { return ::quote_identifier(name, '`'); };
	string escape_string_value(const string &value);
	string &append_quoted_generic_value_to(string &result, const string &value);
	string &append_quoted_spatial_value_to(string &result, const string &value);
	string &append_quoted_json_value_to(string &result, const string &value);
	string &append_quoted_column_value_to(string &result, const Column &column, const string &value);
	tuple<string, string> column_type(const Column &column);
	string column_default(const Table &table, const Column &column);
	string column_definition(const Table &table, const Column &column);
	string key_definition(const Table &table, const Key &key);

	inline bool information_schema_column_default_shows_escaped_expressions() const { return (server_is_mariadb && server_version >= MARIADB_10_2_7); }
	inline bool information_schema_brackets_generated_column_expressions() const { return server_is_mariadb; }
	inline bool supports_srid_settings_on_columns() const { return srid_column_exists; }
	inline bool supports_fractional_seconds() const { return (server_version >= (server_is_mariadb ? MARIADB_10_0_0 : MYSQL_8_0_0)); } // mariadb 5.3 does support microseconds, but doesn't support all the required functions
	inline bool supports_check_constraints() const { return check_constraints_table_exists; }
	inline bool explicit_json_column_type() const { return (!server_is_mariadb && server_version >= MYSQL_5_7_8); }
	inline bool supports_json_column_type() const { return (explicit_json_column_type() || supports_check_constraints()); }
	inline bool supports_generated_columns() const { return generation_expression_column_exists; }

	size_t execute(const string &sql);
	string select_one(const string &sql);
	vector<string> select_all(const string &sql);

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
	bool srid_column_exists;
	bool generation_expression_column_exists;
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
	execute("SET SESSION net_read_timeout = CAST(GREATEST(@@net_read_timeout, 600) AS UNSIGNED), net_write_timeout = CAST(GREATEST(@@net_write_timeout, 600) AS UNSIGNED), long_query_time = CAST(GREATEST(@@long_query_time, 600) AS UNSIGNED), sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,PIPES_AS_CONCAT,NO_AUTO_VALUE_ON_ZERO'");

	if (!variables.empty()) {
		execute("SET " + variables);
	}

	server_is_mariadb = strstr(mysql_get_server_info(&mysql), "MariaDB") != nullptr;
	server_version = mysql_get_server_version(&mysql);

	// mysql doesn't represent the INFORMATION_SCHEMA tables themselves in INFORMATION_SCHEMA, so we have to use the old-style schema info SHOW statements and can't use COUNT(*) etc.
	check_constraints_table_exists = !select_all("SHOW TABLES FROM INFORMATION_SCHEMA LIKE 'CHECK_CONSTRAINTS'").empty();
	srid_column_exists = !select_all("SHOW COLUMNS FROM INFORMATION_SCHEMA.COLUMNS LIKE 'SRS_ID'").empty();
	generation_expression_column_exists = !select_all("SHOW COLUMNS FROM INFORMATION_SCHEMA.COLUMNS LIKE 'GENERATION_EXPRESSION'").empty();
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

vector<string> MySQLClient::select_all(const string &sql) {
	if (mysql_real_query(&mysql, sql.c_str(), sql.length())) {
		throw runtime_error(sql_error(sql));
	}

	MySQLRes res(mysql, false);
	vector<string> results;

	while (true) {
		MYSQL_ROW mysql_row = mysql_fetch_row(res.res());
		if (!mysql_row) break;
		MySQLRow row(res, mysql_row);
		results.push_back(row.string_at(0));
	}

	// check again for errors, as mysql_fetch_row would return NULL for both errors & no more rows
	if (mysql_errno(&mysql)) {
		throw runtime_error(sql_error(sql));
	}

	return results;
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
	execute("SET unique_checks = 0");
}

void MySQLClient::enable_referential_integrity() {
	execute("SET unique_checks = 1");
	execute("SET foreign_key_checks = 1");
}

string MySQLClient::escape_string_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = mysql_real_escape_string(&mysql, (char*)result.data(), value.c_str(), value.size());
	result.resize(result_length);
	return result;
}

string &MySQLClient::append_quoted_generic_value_to(string &result, const string &value) {
	string buffer;
	buffer.resize(value.size()*2 + 1);
	size_t result_length = mysql_real_escape_string(&mysql, (char*)buffer.data(), value.c_str(), value.size());
	result += '\'';
	result.append(buffer, 0, result_length);
	result += '\'';
	return result;
}

string &MySQLClient::append_quoted_spatial_value_to(string &result, const string &value) {
	return append_quoted_generic_value_to(result, ewkb_bin_to_mysql_bin(value));
}

string &MySQLClient::append_quoted_json_value_to(string &result, const string &value) {
	// normally you wouldn't have to do anything special to be able to insert into a JSON field.
	// but we set the MYSQL_SET_CHARSET_NAME option to "binary" to avoid having to interpret
	// character sets, and unfortunately they added an explicit check which causes
	// "Cannot create a JSON value from a string with CHARACTER SET 'binary'" errors, so we have
	// to tell it to convert our "binary" string (which should already actually be UTF-8);
	// conversion from binary effectively just changes the encoding setting, it doesn't actually
	// convert anything.  note that mysql has defined the JSON type as always having encoding
	// utf8mb4 - it isn't like varchar and text.
	result += "CONVERT(";
	append_quoted_generic_value_to(result, value);
	result += " USING utf8mb4)";
	return result;
}

string &MySQLClient::append_quoted_column_value_to(string &result, const Column &column, const string &value) {
	if (!column.values_need_quoting()) {
		return result += value;
	} else if (column.column_type == ColumnType::spatial) {
		return append_quoted_spatial_value_to(result, value);
	} else if (column.column_type == ColumnType::json && explicit_json_column_type()) {
		return append_quoted_json_value_to(result, value);
	} else {
		return append_quoted_generic_value_to(result, value);
	}
}

void MySQLClient::convert_unsupported_database_schema(Database &database) {
	for (Table &table : database.tables) {
		for (Column &column : table.columns) {
			// postgresql allows numeric with no precision or scale specification and preserves the given input data
			// up to an implementation-defined precision and scale limit; mysql doesn't, and silently converts
			// `numeric` to `numeric(10, 0)`.  lacking any better knowledge, we follow their lead and do the same
			// conversion here, just so that the schema matcher sees that the column definition is already the same.
			if (column.column_type == ColumnType::decimal && !column.size && !column.scale) {
				column.size = 10;
			}

			// postgresql allows character varying with no length specification (an extension to the standard).
			// mysql never uses VCHR without a length specification; we don't know what length to make the column -
			// we could make it long but because the maximum mysql permits in a row depends on the character set and
			// what other columns there are on the row, and the maximum permits in an index similarly depends.
			// we change it to a text type to bypass the row limit, and hope that the user hasn't indexed it (which
			// is actually possible, but requires you to specify the length to index); if they have indexed it,
			// they'll need to make their source schema standards-conformant by specifying a length or create the
			// destination schema with a suitable index themselves - no worse than any other case in which they use
			// prefix indexes.
			if (column.column_type == ColumnType::text_varchar && !column.size) {
				column.column_type = ColumnType::text;
			}

			// mariadb, mysql 8.0.0+, and postgresql all support fractional seconds, but earlier mysql versions don't.
			// we strip that information here so the schema compares equal and we don't attempt pointless alters.
			if (!supports_fractional_seconds() && column.size &&
				(column.column_type == ColumnType::datetime || column.column_type == ColumnType::time)) {
				column.size = 0;
			}

			// postgresql treats no default and DEFAULT NULL as separate things, even though they behave much the same.
			// although mysql would happily accept the DEFAULT NULL strings we would produce below, we want to go ahead
			// and convert it here so that the schema matcher also sees them as the same thing.
			if (column.default_type == DefaultType::default_expression && column.default_value == "NULL") {
				column.default_type = DefaultType::no_default;
				column.default_value.clear();
			}

			// postgresql allows you to set an SRID for the geometry type, even though it doesn't do anything.  mysql
			// doesn't have separate geometry and geography types, so anything with an SRID is equivalent to geography.
			if (column.column_type == ColumnType::spatial && !column.reference_system.empty()) {
				column.reference_system.clear();
			}

			// mysql doesn't use named types for enumerations, but postgresql does; mask out any enumeration names we
			// receive so that the tables compare equal in the schema matcher.
			if (column.column_type == ColumnType::enumeration && !column.subtype.empty()) {
				column.subtype.clear();
			}

			// turn off unsupported flags; we always define flags in such a way that this is a graceful degradation
			column.flags.identity_generated_always = false;
		}
	}
}

map<ColumnType, string> SimpleColumnTypes{
	{ColumnType::binary_varbinary,        "varbinary"},
	{ColumnType::binary_fixed,            "binary"},
	{ColumnType::text_varchar,            "varchar"},
	{ColumnType::text_fixed,              "char"},
	{ColumnType::boolean,                 "tinyint(1)"},
	{ColumnType::sint_8bit,               "tinyint"},
	{ColumnType::sint_16bit,              "smallint"},
	{ColumnType::sint_24bit,              "mediumint"},
	{ColumnType::sint_32bit,              "int"},
	{ColumnType::sint_64bit,              "bigint"},
	{ColumnType::uint_8bit,               "tinyint unsigned"},
	{ColumnType::uint_16bit,              "smallint unsigned"},
	{ColumnType::uint_24bit,              "mediumint unsigned"},
	{ColumnType::uint_32bit,              "int unsigned"},
	{ColumnType::uint_64bit,              "bigint unsigned"},
	{ColumnType::float_64bit,             "double"},
	{ColumnType::float_32bit,             "float"},
	{ColumnType::decimal,                 "decimal"},
	{ColumnType::date,                    "date"},
	{ColumnType::time,                    "time"},
	{ColumnType::datetime,                "datetime"},
	{ColumnType::datetime_mysqltimestamp, "timestamp"},
};

string column_type_suffix(const Column &column, size_t default_size = 0) {
	string result;
	if (column.size != default_size) {
		result += '(';
		result += to_string(column.size);
		if (column.scale) {
			result += ',';
			result += to_string(column.scale);
		}
		result += ')';
	}
	return result;
}

tuple<string, string> MySQLClient::column_type(const Column &column) {
	auto simple_type = SimpleColumnTypes.find(column.column_type);
	if (simple_type != SimpleColumnTypes.cend()) {
		return make_tuple(simple_type->second + column_type_suffix(column), "");
	}

	switch (column.column_type) {
		case ColumnType::binary:
			if (column.size < 256) {
				return make_tuple("tinyblob", "");
			} else if (column.size < 65536) {
				return make_tuple("blob", "");
			} else if (column.size < 16777216) {
				return make_tuple("mediumblob", "");
			} else {
				return make_tuple("longblob", "");
			}

		case ColumnType::text:
			if (column.size < 256) {
				return make_tuple("tinytext", "");
			} else if (column.size < 65536) {
				return make_tuple("text", "");
			} else if (column.size < 16777216) {
				return make_tuple("mediumtext", "");
			} else {
				return make_tuple("longtext", "");
			}

		case ColumnType::json:
			// mysql has a proper json type (5.7.8+), but mariadb doesn't, instead they aliased json to longtext (10.2.7+)
			// and later added a json_valid check constraint (10.4.3+) when asking for the json column type.  we do this
			// ourselves so that we can get the same behavior when running our tests against earlier versions.  note that
			// check constraints themselves were only added in mariadb 10.2 (and mysql 8, but we don't need them there
			// since mysql 8 has the actual column type), but earlier versions accepted the syntax (and ignored them).
			// part 2 is in the column_definition() function because we need to put the NOT NULL bit in first.
			if (explicit_json_column_type()) {
				return make_tuple("json", "");
			} else {
				return make_tuple("longtext", " CHECK (json_valid(" + quote_identifier(column.name) + "))");
			}

		case ColumnType::spatial:
			// as discussed in convert_unsupported_schema(), mysql doesn't have separate geometry & geography types, but
			// we use SPATIAL for columns without an SRS
			return make_tuple(column.subtype.empty() ? string("geometry") : column.subtype, "");

		case ColumnType::spatial_geography: {
			string result(column.subtype.empty() ? string("geometry") : column.subtype);

			// nb. mysql outputs this after options like 'NOT NULL', but it accepts it here too; it also
			// outputs it using the special backwards-compatible /*! syntax, which we don't use since we
			// need the version detection code for the information_schema query anyway.
			result += " SRID ";
			result += column.reference_system;

			return make_tuple(result, "");
		}

		case ColumnType::enumeration:
			return make_tuple("ENUM" + values_list(*this, column.enumeration_values), "");

		case ColumnType::mysql_specific:
			// as long as the versions are compatible, this should 'just work'
			return make_tuple(column.subtype, "");

		case ColumnType::unknown:
			// fall back to the raw type string given by the other end, which is really only likely to
			// work if the other end is the same type of database server (and maybe even a compatible
			// version). this also implies we don't know anything about parsing/formatting values for
			// this column type, so it'll only work if the database accepts exactly the same input as
			// it gives in output.
			return make_tuple(column.subtype, "");

		default:
			throw runtime_error("Don't know how to express column type of " + column.name + " (" + to_string(static_cast<int>(column.column_type)) + ")");
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
			append_quoted_column_value_to(result, column, column.default_value);
			return result;
		}

		case DefaultType::default_expression:
			if (column.default_value.substr(0, 17) == "CURRENT_TIMESTAMP") {
				return " DEFAULT " + column.default_value; // the only expression accepted prior to support for arbitrary expressions
			} else {
				return " DEFAULT (" + column.default_value + ")"; // mysql requires brackets around the expression; mariadb accepts and ignores them
			}

		case DefaultType::generated_always_virtual:
			return " GENERATED ALWAYS AS (" + column.default_value + ") VIRTUAL"; // again, mysql requires brackets around the expression; mariadb accepts and ignores them

		case DefaultType::generated_always_stored:
			return " GENERATED ALWAYS AS (" + column.default_value + ") STORED"; // as above

		default:
			throw runtime_error("Don't know how to express default of " + column.name + " (" + to_string((int)column.default_type) + ")");
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
	} else if (column.column_type == ColumnType::datetime_mysqltimestamp) {
		result += " NULL";
	}

	if (column.default_type != DefaultType::no_default) {
		result += column_default(table, column);
	}

	// used for JSON etc. which need to add stuff after the NOT NULL bit in some cases; see discussion in column_type()
	if (!extra.empty()) {
		result += extra;
	}

	if (column.flags.auto_update_timestamp) {
		if (column.size) {
			result += " ON UPDATE CURRENT_TIMESTAMP(" + to_string(column.size) + ")"; // mysql 8 insists on the precision being explicitly specified
		} else {
			result += " ON UPDATE CURRENT_TIMESTAMP";
		}
	}

	return result;
}

string MySQLClient::key_definition(const Table &table, const Key &key) {
	string result;
	switch (key.key_type) {
		case KeyType::standard_key:
			result = "CREATE INDEX";
			break;

		case KeyType::unique_key:
			result = "CREATE UNIQUE INDEX";
			break;

		case KeyType::spatial_key:
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

inline ColumnTypeList MySQLClient::supported_types() {
	ColumnTypeList result{
		ColumnType::binary,
		ColumnType::text,
		ColumnType::spatial,
		ColumnType::enumeration,
		ColumnType::mysql_specific,
		ColumnType::unknown,
	};
	for (const auto &it: SimpleColumnTypes) {
		result.insert(it.first);
	}
	if (supports_json_column_type()) {
		result.insert(ColumnType::json);
	}
	if (supports_srid_settings_on_columns()) {
		result.insert(ColumnType::spatial_geography);
	}
	return result;
}

struct MySQLColumnLister {
	inline MySQLColumnLister(MySQLClient &client, Table &table, const ColumnTypeList &accepted_types): client(client), table(table), accepted_types(accepted_types) {}

	inline void operator()(MySQLRow &row) {
		Column column;

		column.name = row.string_at(0);
		string db_type(row.string_at(1));
		column.nullable = (row.string_at(2) == "YES");
		bool unsign(db_type.length() > 8 && db_type.substr(db_type.length() - 8, 8) == "unsigned");
		string extra(row.string_at(4));
		string generation_expression(row.string_at(5));
		string comment(row.string_at(7));

		if (extra.find("VIRTUAL GENERATED") != string::npos) {
			column.default_type = DefaultType::generated_always_virtual;
			column.default_value = (client.information_schema_brackets_generated_column_expressions() ? '(' + generation_expression + ')' : generation_expression); // mariadb uses fewer brackets than mysql & postgresql; normalize purely to make tests easier

		} else if (extra.find("STORED GENERATED") != string::npos) {
			column.default_type = DefaultType::generated_always_stored;
			column.default_value = (client.information_schema_brackets_generated_column_expressions() ? '(' + generation_expression + ')' : generation_expression); // as above

		} else if (!row.null_at(3)) {
			column.default_type = DefaultType::default_value;
			column.default_value = row.string_at(3);

			if (client.information_schema_column_default_shows_escaped_expressions()) {
				// mariadb 10.2.7 and above always represent the default value as an expression, which we want to turn back into a plain value if it is one (for compatibility)
				if (column.default_value == "NULL") {
					column.default_type = DefaultType::no_default;
					column.default_value.clear();
				} else if (column.default_value.length() >= 2 && column.default_value[0] == '\'' && column.default_value[column.default_value.length() - 1] == '\'') {
					column.default_value = unescape_string_value(column.default_value.substr(1, column.default_value.length() - 2));
				} else if (!column.default_value.empty() && column.default_value.find_first_not_of("0123456789.") != string::npos) {
					column.default_type = DefaultType::default_expression;
				}
			} else {
				// mysql 8.0 and above show literal strings and expressions the same way in the COLUMN_DEFAULT field, but set the EXTRA field to DEFAULT_GENERATED for expressions
				if (extra.find("DEFAULT_GENERATED") != string::npos) {
					column.default_type = DefaultType::default_expression;
				}
			}
		}

		if (extra.find("auto_increment") != string::npos) {
			column.default_type = DefaultType::sequence;
		}


		if (db_type == "tinyint(1)") {
			if (column.default_type != DefaultType::no_default) {
				if (column.default_value == "0") {
					column.default_value = "false";
				} else if (column.default_value == "1") {
					column.default_value = "true";
				} else {
					throw runtime_error("Invalid default value for boolean column " + table.name + "." + column.name + ": " + column.default_value + " (we assume tinyint(1) is used for booleans)");
				}
			}
			column.column_type = ColumnType::boolean;

		} else if (db_type.substr(0, 8) == "tinyint(") {
			// select the equivalent-width signed type if unsigned types aren't supported by the other
			// end (and rely on runtime checks that there's no values that overflow).  select 16-bit
			// types if 8-bit types aren't supported by the other end.
			if (unsign) {
				column.column_type = select_supported_type(ColumnType::uint_8bit, ColumnType::sint_8bit, ColumnType::sint_16bit);
			} else {
				column.column_type = select_supported_type(ColumnType::sint_8bit, ColumnType::sint_16bit);
			}

		} else if (db_type.substr(0, 9) == "smallint(") {
			if (unsign) {
				column.column_type = select_supported_type(ColumnType::uint_16bit, ColumnType::sint_16bit);
			} else {
				column.column_type = ColumnType::sint_16bit;
			}

		} else if (db_type.substr(0, 10) == "mediumint(") {
			// select 32-bit types if 24-bit types aren't supported by the other end.
			if (unsign) {
				column.column_type = select_supported_type(ColumnType::uint_24bit, ColumnType::sint_24bit, ColumnType::sint_32bit);
			} else {
				column.column_type = select_supported_type(ColumnType::sint_24bit, ColumnType::sint_32bit);
			}

		} else if (db_type.substr(0, 4) == "int(") {
			if (unsign) {
				column.column_type = select_supported_type(ColumnType::uint_32bit, ColumnType::sint_32bit);
			} else {
				column.column_type = ColumnType::sint_32bit;
			}

		} else if (db_type.substr(0, 7) == "bigint(") {
			if (unsign) {
				column.column_type = select_supported_type(ColumnType::uint_64bit, ColumnType::sint_64bit);
			} else {
				column.column_type = ColumnType::sint_64bit;
			}

		} else if (db_type.substr(0, 8) == "decimal(") {
			column.column_type = ColumnType::decimal;
			column.size = extract_column_length(db_type);
			column.scale = extract_column_scale(db_type);

		} else if (db_type == "float") {
			column.column_type = ColumnType::float_32bit;

		} else if (db_type == "double") {
			column.column_type = ColumnType::float_64bit;

		} else if (db_type == "tinyblob") {
			column.column_type = ColumnType::binary;
			column.size = 255;

		} else if (db_type == "blob") {
			column.column_type = ColumnType::binary;
			column.size = 65535;

		} else if (db_type == "mediumblob") {
			column.column_type = ColumnType::binary;
			column.size = 16777215;

		} else if (db_type == "longblob") {
			column.column_type = ColumnType::binary; // leave size 0 to mean max to match other dbs, for compatibility, but the longblob limit is 4294967295

		} else if (db_type.substr(0, 10) == "varbinary(") {
			column.column_type = select_supported_type(ColumnType::binary_varbinary, ColumnType::binary);
			column.size = extract_column_length(db_type);

		} else if (db_type.substr(0, 7) == "binary(") {
			column.column_type = select_supported_type(ColumnType::binary_fixed, ColumnType::binary);
			column.size = extract_column_length(db_type);

		} else if (db_type == "tinytext") {
			column.column_type = ColumnType::text;
			column.size = 255;

		} else if (db_type == "text") {
			column.column_type = ColumnType::text;
			column.size = 65535;

		} else if (db_type == "mediumtext") {
			column.column_type = ColumnType::text;
			column.size = 16777215;

		} else if (db_type == "longtext") {
			if (!row.null_at(8) && row.int_at(8)) { // column 8 is the JSON check constraint; the check constraint is the canonical way to handle JSON in mariadb, whereas mysql 8+ has the explicit column type
				column.column_type = ColumnType::json;
			} else {
				column.column_type = ColumnType::text; // leave size 0 to mean max to match other dbs, for compatibility, but the longtext limit is 4294967295
			}

		} else if (db_type.substr(0, 8) == "varchar(") {
			column.column_type = ColumnType::text_varchar;
			column.size = extract_column_length(db_type);

		} else if (db_type.substr(0, 5) == "char(") {
			column.column_type = ColumnType::text_fixed;
			column.size = extract_column_length(db_type);
			while (column.default_type != DefaultType::no_default && column.default_value.length() < column.size) column.default_value += ' ';

		} else if (db_type == "json") {
			column.column_type = ColumnType::json;

		} else if (db_type == "date") {
			column.column_type = ColumnType::date;

		} else if (db_type == "time" || db_type.substr(0, 5) == "time(") {
			if (column.default_type == DefaultType::default_value) {
				column.default_value = time_value_after_trimming_fractional_zeros(column.default_value);
			}
			column.column_type = ColumnType::time;
			column.size = extract_time_precision(db_type);

		} else if (db_type == "datetime" || db_type.substr(0, 9) ==  "datetime(" || db_type == "timestamp" || db_type.substr(0, 10) == "timestamp(") {
			if (db_type == "timestamp" || db_type.substr(0, 10) == "timestamp(") {
				column.column_type = select_supported_type(ColumnType::datetime_mysqltimestamp, ColumnType::datetime);
			} else {
				column.column_type = ColumnType::datetime;
			}
			if (column.default_value == "CURRENT_TIMESTAMP" || column.default_value == "CURRENT_TIMESTAMP()" || column.default_value == "current_timestamp()") {
				column.default_type = DefaultType::default_expression;
				column.default_value = "CURRENT_TIMESTAMP"; // normalize
			} else if (column.default_value.length() == 20 && column.default_value[19] == ')' && (column.default_value.substr(0, 18) == "CURRENT_TIMESTAMP("  || column.default_value.substr(0, 18) == "current_timestamp(")) {
				column.default_type = DefaultType::default_expression;
				column.default_value.replace(0, 17, "CURRENT_TIMESTAMP"); // normalize case
			}
			if (extra.find("on update CURRENT_TIMESTAMP") != string::npos || extra.find("on update current_timestamp(") != string::npos) {
				column.flags.auto_update_timestamp = true;
			}
			if (column.default_type == DefaultType::default_value) {
				column.default_value = datetime_value_after_trimming_fractional_zeros(column.default_value);
			}
			column.size = extract_time_precision(db_type);

		} else if (db_type == "geometry" || db_type == "point" || db_type == "linestring" || db_type == "polygon" || db_type == "geometrycollection" || db_type == "multipoint" || db_type == "multilinestring" || db_type == "multipolygon") {
			if (db_type != "geometry") column.subtype = db_type;

			// mysql doesn't have separate geometry & geography types like postgresql, but for compatibility with it
			// we use SPATIAL_GEOGRAPHY for columns with an SRS and SPATIAL for those without
			if (!row.null_at(6)) {
				column.reference_system = row.string_at(6);
				column.column_type = ColumnType::spatial_geography;
			} else {
				column.column_type = ColumnType::spatial;
			}

		} else if (db_type.substr(0, 5) == "enum(" && db_type.length() > 6 && db_type[db_type.length() - 1] == ')') {
			column.column_type = ColumnType::enumeration;
			column.enumeration_values = parse_bracketed_list(db_type, 4);

		} else {
			// not supported, but leave it till sync_to's check_tables_usable to complain about it so that it can be ignored
			column.column_type = ColumnType::mysql_specific;
		}

		// degrade to 'unknown' if the type we want isn't supported by the version at the other end
		if (!accepted_types.count(column.column_type)) {
			column.column_type = ColumnType::unknown;
			column.size = column.scale = 0;
		}

		// send the raw database-specific type for unknown or unsupported types
		if (column.column_type == ColumnType::mysql_specific || column.column_type == ColumnType::unknown) {
			column.subtype = db_type;
		}

		table.columns.push_back(column);
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

	vector<string> parse_bracketed_list(const string &str, string::size_type pos = 0) {
		vector<string> result;
		if (str[pos] != '(' || str[pos + 1] != '\'' || str[str.length() - 1] != ')' || str[str.length() - 2] != '\'') return result;

		string value;
		pos += 2;
		while (true) {
			if (str[pos] != '\'') {
				value += str[pos++];
			} else if (str[++pos] == '\'') {
				value += str[pos++];
			} else {
				result.push_back(value);
				if (str[pos] == ')') return result;
				if (str[pos] !=	',') throw runtime_error("invalid value list at position " + to_string(pos) + ": " + str);
				value.clear();
				pos += 2; // skip the comma and the opening quote for the next string
			}
		}
	}

	size_t extract_time_precision(const string &db_type) {
		if (db_type[db_type.length() - 1] == ')') {
			return extract_column_length(db_type);
		} else {
			return 0;
		}
	}

	inline ColumnType select_supported_type(ColumnType type1, ColumnType type2, ColumnType type3 = ColumnType::mysql_specific) {
		if (accepted_types.count(type1)) return type1;
		if (accepted_types.count(type2)) return type2;
		if (accepted_types.count(type3)) return type3;
		return ColumnType::mysql_specific;
	}

	MySQLClient &client;
	Table &table;
	const ColumnTypeList &accepted_types;
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
			table.primary_key_type = PrimaryKeyType::explicit_primary_key;

		} else {
			// a column in a generic key, which may or may not be unique
			if (table.keys.empty() || table.keys.back().name != key_name) {
				KeyType key_type(KeyType::standard_key);
				if (row.string_at(1) == "0") {
					key_type = KeyType::unique_key;
				} else if (row.string_at(10) == "SPATIAL") {
					key_type = KeyType::spatial_key;
				}
				table.keys.push_back(Key(key_name, key_type));
			}
			table.keys.back().columns.push_back(column_index);
		}
	}

	Table &table;
};

struct MySQLTableLister {
	inline MySQLTableLister(MySQLClient &client, Database &database, const ColumnTypeList &accepted_types): client(client), database(database), accepted_types(accepted_types) {}

	inline void operator()(MySQLRow &row) {
		Table table(row.string_at(0));

		MySQLColumnLister column_lister(client, table, accepted_types);
		client.query("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, " + generation_expression_column() + ", " + srid_column() + ", COLUMN_COMMENT, " + json_check_constraint_expression() + " AS JSON_CHECK_CONSTRAINT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = SCHEMA() AND TABLE_NAME = '" + client.escape_string_value(table.name) + "' ORDER BY ORDINAL_POSITION", column_lister);

		MySQLKeyLister key_lister(table);
		client.query("SHOW KEYS FROM " + client.quote_identifier(table.name), key_lister);
		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name

		database.tables.push_back(table);
	}

	inline string srid_column() {
		return (client.supports_srid_settings_on_columns() ? "SRS_ID" : "NULL AS SRS_ID");
	}

	inline string generation_expression_column() {
		return (client.supports_generated_columns() ? "GENERATION_EXPRESSION" : "NULL AS GENERATION_EXPRESSION");
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
	const ColumnTypeList &accepted_types;
};

void MySQLClient::populate_database_schema(Database &database, const ColumnTypeList &accepted_types) {
	MySQLTableLister table_lister(*this, database, accepted_types);
	query(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = schema() AND table_type = \"BASE TABLE\" ORDER BY data_length DESC, table_name ASC",
		table_lister,
		true /* buffer so we can make further queries during iteration */);
}


int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
