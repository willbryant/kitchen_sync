#include "endpoint.h"

#include <stdexcept>
#include <set>
#include <cctype>
#include <libpq-fe.h>

#include "schema.h"
#include "database_client_traits.h"
#include "sql_functions.h"
#include "row_printer.h"
#include "ewkb.h"

#define POSTGRESQL_9_4 90400
#define POSTGRESQL_10 100000

struct TypeMap {
	set<Oid> spatial;
	map<string, vector<string>> enum_type_values;
};

enum PostgreSQLColumnConversion {
	encode_raw,
	encode_bool,
	encode_sint,
	encode_bytea,
	encode_geom,
};

class PostgreSQLRes {
public:
	PostgreSQLRes(PGresult *res, const TypeMap &type_map);
	~PostgreSQLRes();

	inline PGresult *res() { return _res; }
	inline ExecStatusType status() { return PQresultStatus(_res); }
	inline size_t rows_affected() const { return atoi(PQcmdTuples(_res)); }
	inline int n_tuples() const  { return _n_tuples; }
	inline int n_columns() const { return _n_columns; }
	inline PostgreSQLColumnConversion conversion_for(int column_number) { if (conversions.empty()) populate_conversions(); return conversions[column_number]; }

private:
	void populate_conversions();
	PostgreSQLColumnConversion conversion_for_type(Oid typid);

	PGresult *_res;
	const TypeMap &_type_map;
	int _n_tuples;
	int _n_columns;
	vector<PostgreSQLColumnConversion> conversions;
};

PostgreSQLRes::PostgreSQLRes(PGresult *res, const TypeMap &type_map): _res(res), _type_map(type_map) {
	_n_tuples = PQntuples(_res);
	_n_columns = PQnfields(_res);
}

PostgreSQLRes::~PostgreSQLRes() {
	if (_res) {
		PQclear(_res);
	}
}

void PostgreSQLRes::populate_conversions() {
	conversions.resize(_n_columns);

	for (size_t i = 0; i < _n_columns; i++) {
		Oid typid = PQftype(_res, i);
		conversions[i] = conversion_for_type(typid);
	}
}

// from pg_type.h, which isn't available/working on all distributions.
#define BOOLOID			16
#define BYTEAOID		17
#define CHAROID			18
#define INT2OID			21
#define INT4OID			23
#define INT8OID			20
#define TEXTOID			25

PostgreSQLColumnConversion PostgreSQLRes::conversion_for_type(Oid typid) {
	switch (typid) {
		case BOOLOID:
			return encode_bool;

		case INT2OID:
		case INT4OID:
		case INT8OID:
			return encode_sint;

		case BYTEAOID:
			return encode_bytea;

		case CHAROID:
		case TEXTOID:
			return encode_raw; // so this is actually just an optimised version of the default block below

		default:
			// because the Geometry type comes from the PostGIS extension, its OID isn't a constant, so we can't use it in a case statement.
			// we also need to look for the type for Geography, so we've used a set instead of a scalar; this means we'll also tolerate
			// finding more than one OID found per type (presumably from different installs of the extension).
			if (_type_map.spatial.count(typid)) {
				return encode_geom;
			} else {
				return encode_raw;
			}
	}
}


class PostgreSQLRow {
public:
	inline PostgreSQLRow(PostgreSQLRes &res, int row_number): _res(res), _row_number(row_number) { }
	inline const PostgreSQLRes &results() const { return _res; }

	inline         int n_columns() const { return _res.n_columns(); }

	inline        bool   null_at(int column_number) const { return PQgetisnull(_res.res(), _row_number, column_number); }
	inline const char *result_at(int column_number) const { return PQgetvalue (_res.res(), _row_number, column_number); }
	inline         int length_of(int column_number) const { return PQgetlength(_res.res(), _row_number, column_number); }
	inline      string string_at(int column_number) const { return string(result_at(column_number), length_of(column_number)); }
	inline        bool   bool_at(int column_number) const { return (strcmp(result_at(column_number), "t") == 0); }
	inline     int64_t    int_at(int column_number) const { return strtoll(result_at(column_number), nullptr, 10); }
	inline    uint64_t   uint_at(int column_number) const { return strtoull(result_at(column_number), nullptr, 10); }

	template <typename Packer>
	inline void pack_column_into(Packer &packer, int column_number) const {
		if (null_at(column_number)) {
			packer << nullptr;
		} else {
			switch (_res.conversion_for(column_number)) {
				case encode_bool:
					packer << bool_at(column_number);
					break;

				case encode_sint:
					packer << int_at(column_number);
					break;

				case encode_bytea: {
					size_t decoded_length;
					void *decoded = PQunescapeBytea((const unsigned char *)result_at(column_number), &decoded_length);
					packer << uncopied_byte_string(decoded, decoded_length);
					PQfreemem(decoded);
					break;
				}

				case encode_geom:
					packer << hex_to_bin_string(result_at(column_number), length_of(column_number));
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
	PostgreSQLRes &_res;
	int _row_number;
};


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

	void disable_referential_integrity();
	void enable_referential_integrity();
	string export_snapshot();
	void import_snapshot(const string &snapshot);
	void unhold_snapshot();
	void start_read_transaction();
	void start_write_transaction();
	void commit_transaction();
	void rollback_transaction();
	void populate_types();
	void populate_database_schema(Database &database);
	void convert_unsupported_database_schema(Database &database);
	string escape_string_value(const string &value);
	string &append_escaped_string_value_to(string &result, const string &value);
	string &append_escaped_bytea_value_to(string &result, const string &value);
	string &append_escaped_spatial_value_to(string &result, const string &value);
	string &append_escaped_column_value_to(string &result, const Column &column, const string &value);
	string column_type(const Column &column);
	string column_sequence_name(const Table &table, const Column &column);
	string column_default(const Table &table, const Column &column);
	string column_definition(const Table &table, const Column &column);
	string key_definition(const Table &table, const Key &key);

	inline string quote_identifier(const string &name) { return ::quote_identifier(name, '"'); };
	inline bool supports_jsonb_column_type() const { return (server_version >= POSTGRESQL_9_4); }
	inline bool supports_generated_as_identity() const { return (server_version >= POSTGRESQL_10); }

	size_t execute(const string &sql);
	string select_one(const string &sql);

	template <typename RowFunction>
	size_t query(const string &sql, RowFunction &row_handler) {
		PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */), type_map);

		if (res.status() != PGRES_TUPLES_OK) {
			throw runtime_error(sql_error(sql));
		}

		for (int row_number = 0; row_number < res.n_tuples(); row_number++) {
			PostgreSQLRow row(res, row_number);
			row_handler(row);
		}

		return res.n_tuples();
	}

protected:
	string sql_error(const string &sql);

private:
	PGconn *conn;
	int server_version;
	TypeMap type_map;

	// forbid copying
	PostgreSQLClient(const PostgreSQLClient &_) = delete;
	PostgreSQLClient &operator=(const PostgreSQLClient &_) = delete;
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

	server_version = PQserverVersion(conn);

	// we call this ourselves as all instances need to know the type OIDs that need special conversion,
	// whereas populate_database_schema is only called for the leader at the 'to' end
	populate_types();
}

PostgreSQLClient::~PostgreSQLClient() {
	if (conn) {
		PQfinish(conn);
	}
}

size_t PostgreSQLClient::execute(const string &sql) {
    PostgreSQLRes res(PQexec(conn, sql.c_str()), type_map);

    if (res.status() != PGRES_COMMAND_OK && res.status() != PGRES_TUPLES_OK) {
		throw runtime_error(sql_error(sql));
    }

    return res.rows_affected();
}

string PostgreSQLClient::select_one(const string &sql) {
	PostgreSQLRes res(PQexecParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0 /* text-format results only */), type_map);

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
	execute("SET TRANSACTION SNAPSHOT '" + escape_string_value(snapshot) + "'");
}

void PostgreSQLClient::unhold_snapshot() {
	// do nothing - only needed for lock-based systems like mysql
}

void PostgreSQLClient::disable_referential_integrity() {
	execute("SET CONSTRAINTS ALL DEFERRED");

	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + client.quote_identifier(table.name) + " DISABLE TRIGGER ALL");
	}
	*/
}

void PostgreSQLClient::enable_referential_integrity() {
	/* TODO: investigate the pros and cons of disabling triggers - this blocks if there's a read transaction open
	for (const Table &table : database.tables) {
		execute("ALTER TABLE " + client.quote_identifier(table.name) + " ENABLE TRIGGER ALL");
	}
	*/
}

string PostgreSQLClient::escape_string_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)result.data(), value.c_str(), value.size(), nullptr);
	result.resize(result_length);
	return result;
}

string &PostgreSQLClient::append_escaped_string_value_to(string &result, const string &value) {
	string buffer;
	buffer.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)buffer.data(), value.c_str(), value.size(), nullptr);
	result += '\'';
	result.append(buffer, 0, result_length);
	result += '\'';
	return result;
}

string &PostgreSQLClient::append_escaped_bytea_value_to(string &result, const string &value) {
	size_t encoded_length;
	const unsigned char *encoded = PQescapeByteaConn(conn, (const unsigned char *)value.c_str(), value.size(), &encoded_length);
	result += '\'';
	result.append(encoded, encoded + encoded_length - 1); // encoded_length includes the null terminator
	result += '\'';
	PQfreemem((void *)encoded);
	return result;
}

string &PostgreSQLClient::append_escaped_spatial_value_to(string &result, const string &value) {
	result.append("ST_GeomFromEWKB(");
	append_escaped_bytea_value_to(result, value);
	result.append(")");
	return result;
}

string &PostgreSQLClient::append_escaped_column_value_to(string &result, const Column &column, const string &value) {
	if (column.column_type == ColumnTypes::BLOB) {
		return append_escaped_bytea_value_to(result, value);
	} else if (column.column_type == ColumnTypes::SPAT) {
		return append_escaped_spatial_value_to(result, value);
	} else {
		return append_escaped_string_value_to(result, value);
	}
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

			if (column.column_type == ColumnTypes::DTTM || column.column_type == ColumnTypes::TIME) {
				// postgresql only supports microsecond precision on time columns; enforce this on the definitions on
				// incoming column definitions so the schema compares equal and we don't attempt pointless alters.
				column.size = 6;
			}

			if (column.column_type == ColumnTypes::ENUM && column.type_restriction.empty()) {
				// postgresql requires that you create a material type for each enumeration, whereas mysql just lists the
				// possible values on the column itself.  we don't currently implement creation/maintainance of these custom
				// types ourselves - users need to do that - but we need to find the name of the type they've (hopefully) created.
				for (auto it = type_map.enum_type_values.begin(); it != type_map.enum_type_values.end() && column.type_restriction.empty(); ++it) {
					if (it->second == column.enumeration_values) {
						column.type_restriction = it->first;
					}
				}
			}

			// turn off unsupported flags; we always define flags in such a way that this is a graceful degradation
			column.flags.mysql_timestamp = column.flags.mysql_on_update_timestamp = false;
			if (!supports_jsonb_column_type()) column.flags.binary_storage = false;
			if (!supports_generated_as_identity()) column.flags.identity_generated_always = false;
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
		string result("character varying");
		if (column.size > 0) {
			result += '(';
			result += to_string(column.size);
			result += ')';
		}
		return result;

	} else if (column.column_type == ColumnTypes::FCHR) {
		string result("character(");
		result += to_string(column.size);
		result += ')';
		return result;

	} else if (column.column_type == ColumnTypes::JSON) {
		if (column.flags.binary_storage) {
			return "jsonb";
		} else {
			return "json";
		}

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
		if (column.size) {
			string result("numeric(");
			result += to_string(column.size);
			result += ',';
			result += to_string(column.scale);
			result += ')';
			return result;
		} else {
			return "numeric";
		}

	} else if (column.column_type == ColumnTypes::DATE) {
		return "date";

	} else if (column.column_type == ColumnTypes::TIME) {
		if (column.flags.time_zone) {
			return "time with time zone";
		} else {
			return "time without time zone";
		}

	} else if (column.column_type == ColumnTypes::DTTM) {
		if (column.flags.time_zone) {
			return "timestamp with time zone";
		} else {
			return "timestamp without time zone";
		}

	} else if (column.column_type == ColumnTypes::SPAT) {
		// note that we have made the assumption that all the mysql geometry types should be mapped to
		// PostGIS objects, rather than to the built-in geometric types such as POINT, because
		// postgresql's built-in geometric types don't support spatial reference systems (SRIDs), don't
		// have any equivalent to the multi* types, the built-in POLYGON type doesn't support 'holes' (as
		// created using the two-argument form on mysql), etc.
		// this still leaves us with a choice between geometry and geography; the difference is that
		// geography does calculations on spheroids whereas geometry calculates using simple planes.
		// geography defaults to SRID 4326 (and until recently, only supported 4326) and will even use
		// that default if you specify 0.  geometry allows you to specify any SRID and remembers it, but
		// always calculates is as if using SRID 0, so there's no real benefit to doing so.  so we have
		// made the assumption that generally SRID set => want the geography type, when coming from other
		// databases, but we've added a simple_geometry flag so we can round-trip geometry with an SRID.
		string result(column.reference_system.empty() || column.flags.simple_geometry ? "geometry" : "geography");
		if (!column.reference_system.empty()) {
			result += '(';
			result += (column.type_restriction.empty() ? string("geometry") : column.type_restriction);
			result += ',';
			result += column.reference_system;
			result += ')';
		} else if (!column.type_restriction.empty()) {
			result += '(';
			result += column.type_restriction;
			result += ')';
		}
		return result;

	} else if (column.column_type == ColumnTypes::ENUM) {
		// a named ENUM type (presumably from another postgresql instance); we don't create/maintain
		// these types ourselves currently, they need to exist already.
		if (column.type_restriction.empty()) {
			throw runtime_error("Can't find an enumerated type with possible values " + values_list(*this, column.enumeration_values) + " for column " + column.name + ", please create one using CREATE TYPE");
		}
		if (!type_map.enum_type_values.count(column.type_restriction)) {
			throw runtime_error("Need an enumerated type named " + column.type_restriction + " with possible values " + values_list(*this, column.enumeration_values) + " for column " + column.name + ", please create one using CREATE TYPE");
		}
		if (type_map.enum_type_values[column.type_restriction] != column.enumeration_values) {
			throw runtime_error("The enumerated type named " + column.type_restriction + " has possible values " + values_list(*this, type_map.enum_type_values[column.type_restriction]) + " but should have possible values " + values_list(*this, column.enumeration_values) + " for column " + column.name + ", please alter the type");
		}
		return column.type_restriction;

	} else {
		throw runtime_error("Don't know how to express column type of " + column.name + " (" + column.column_type + ")");
	}
}

string PostgreSQLClient::column_sequence_name(const Table &table, const Column &column) {
	// name to match what postgresql creates for serial columns
	return table.name + "_" + column.name + "_seq";
}

string PostgreSQLClient::column_default(const Table &table, const Column &column) {
	switch (column.default_type) {
		case DefaultType::no_default:
			return " DEFAULT NULL";

		case DefaultType::sequence:
			if (!supports_generated_as_identity()) {
				string result(" DEFAULT nextval('");
				result += escape_string_value(quote_identifier(column_sequence_name(table, column)));
				result += "'::regclass)";
				return result;
			} else if (column.flags.identity_generated_always) {
				return " GENERATED ALWAYS AS IDENTITY";
			} else {
				return " GENERATED BY DEFAULT AS IDENTITY";
			}

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
			return " DEFAULT " + column.default_value; // the only expression accepted prior to support for arbitrary expressions

		default:
			throw runtime_error("Don't know how to express default of " + column.name + " (" + to_string((int)column.default_type) + ")");
	}
}

string PostgreSQLClient::column_definition(const Table &table, const Column &column) {
	string result;
	result += quote_identifier(column.name);
	result += ' ';

	result += column_type(column);

	if (!column.nullable) {
		result += " NOT NULL";
	}

	if (column.default_type != DefaultType::no_default) {
		result += column_default(table, column);
	}

	return result;
}

string PostgreSQLClient::key_definition(const Table &table, const Key &key) {
	string result(key.unique() ? "CREATE UNIQUE INDEX " : "CREATE INDEX ");
	result += quote_identifier(key.name);
	result += " ON ";
	result += quote_identifier(table.name);
	result += ' ';
	if (key.spatial()) result += "USING gist ";
	result += columns_tuple(*this, table.columns, key.columns);
	return result;
}

struct PostgreSQLColumnLister {
	inline PostgreSQLColumnLister(Table &table, TypeMap &type_map): table(table), type_map(type_map) {}

	inline void operator()(PostgreSQLRow &row) {
		Column column;

		column.name = row.string_at(0);
		string db_type(row.string_at(1));
		column.nullable = (row.string_at(2) == "f");

		if (!row.null_at(5) && row.string_at(5) != "\0") {
			// attidentity set (will be 'a' for 'always' and 'd' for 'by default')
			column.default_type = DefaultType::sequence;
			if (row.string_at(5) == "a") column.flags.identity_generated_always = true;

		} else if (row.string_at(3) == "t") {
			column.default_type = DefaultType::default_value;
			column.default_value = row.string_at(4);

			if (column.default_value.length() > 20 &&
				column.default_value.substr(0, 9) == "nextval('" &&
				column.default_value.substr(column.default_value.length() - 12, 12) == "'::regclass)") {
				// this is what you got back when you used the historic SERIAL pseudo-type (subsequently replaced by the new standard IDENTITY GENERATED ... AS IDENTITY)
				column.default_type = DefaultType::sequence;
				column.default_value = "";

			} else if (column.default_value.substr(0, 6) == "NULL::" && db_type.substr(0, column.default_value.length() - 6) == column.default_value.substr(6)) {
				// postgresql treats a NULL default as distinct to no default, so we try to respect that by keeping the value as a function,
				// but chop off the type conversion for the sake of portability
				column.default_type = DefaultType::default_expression;
				column.default_value = "NULL";

			} else if (column.default_value.length() > 2 && column.default_value[0] == '\'') {
				column.default_value = unescape_string_value(column.default_value.substr(1, column.default_value.rfind('\'') - 1));

			} else if (column.default_value.length() > 0 && column.default_value != "false" && column.default_value != "true" && column.default_value.find_first_not_of("0123456789.") != string::npos) {
				column.default_type = DefaultType::default_expression;

				// earlier versions of postgresql convert CURRENT_TIMESTAMP to now(); convert it back for portability
				if (column.default_value == "now()") {
					column.default_value = "CURRENT_TIMESTAMP";

				// do the same for its conversion of CURRENT_TIMESTAMP(n)
				} else if (column.default_value.length() == 42 && column.default_value.substr(0, 25) == "('now'::text)::timestamp(" && column.default_value.substr(26, 16) == ") with time zone") {
					column.default_value = "CURRENT_TIMESTAMP(" + column.default_value.substr(25, 1) + ")";

				// and do the same for its conversion of CURRENT_DATE
				} else if (column.default_value == "('now'::text)::date") {
					column.default_value = "CURRENT_DATE";

				// other SQL-reserved zero-argument functions come back with quoted identifiers and brackets, see Note on the
				// 'System Information Functions' page; the list here is shorter because some get converted to one of the others by pg
				} else if (column.default_value == "\"current_schema\"()" || column.default_value == "\"current_user\"()" || column.default_value == "\"session_user\"()") {
					column.default_value = column.default_value.substr(1, column.default_value.length() - 4);
				}
			}
		}

		if (db_type == "boolean") {
			column.column_type = ColumnTypes::BOOL;

		} else if (db_type == "smallint") {
			column.column_type = ColumnTypes::SINT;
			column.size = 2;

		} else if (db_type == "integer") {
			column.column_type = ColumnTypes::SINT;
			column.size = 4;

		} else if (db_type == "bigint") {
			column.column_type = ColumnTypes::SINT;
			column.size = 8;

		} else if (db_type == "real") {
			column.column_type = ColumnTypes::REAL;
			column.size = 4;

		} else if (db_type == "double precision") {
			column.column_type = ColumnTypes::REAL;
			column.size = 8;

		} else if (db_type.substr(0, 8) == "numeric(") {
			column.column_type = ColumnTypes::DECI;
			column.size = extract_column_length(db_type);
			column.scale = extract_column_scale(db_type);

		} else if (db_type.substr(0, 7) == "numeric") {
			column.column_type = ColumnTypes::DECI;

		} else if (db_type.substr(0, 18) == "character varying(") {
			column.column_type = ColumnTypes::VCHR;
			column.size = extract_column_length(db_type);

		} else if (db_type.substr(0, 18) == "character varying") {
			column.column_type = ColumnTypes::VCHR; /* no length limit */

		} else if (db_type.substr(0, 10) == "character(") {
			column.column_type = ColumnTypes::FCHR;
			column.size = extract_column_length(db_type);

		} else if (db_type == "text") {
			column.column_type = ColumnTypes::TEXT;

		} else if (db_type == "bytea") {
			column.column_type = ColumnTypes::BLOB;

		} else if (db_type == "json") {
			column.column_type = ColumnTypes::JSON;

		} else if (db_type == "jsonb") {
			column.column_type = ColumnTypes::JSON;
			column.flags.binary_storage = true;

		} else if (db_type == "uuid") {
			column.column_type = ColumnTypes::UUID;

		} else if (db_type == "date") {
			column.column_type = ColumnTypes::DATE;

		} else if (db_type == "time without time zone") {
			column.column_type = ColumnTypes::TIME;
			column.size = 6; /* microsecond precision */

		} else if (db_type == "time with time zone") {
			column.column_type = ColumnTypes::TIME;
			column.flags.time_zone = true;
			column.size = 6; /* microsecond precision */

		} else if (db_type == "timestamp without time zone") {
			column.column_type = ColumnTypes::DTTM;
			column.size = 6; /* microsecond precision */

		} else if (db_type == "timestamp with time zone") {
			column.column_type = ColumnTypes::DTTM;
			column.flags.time_zone = true;
			column.size = 6; /* microsecond precision */

		} else if (db_type == "geometry") {
			column.column_type = ColumnTypes::SPAT;

		} else if (db_type == "geography") {
			column.column_type = ColumnTypes::SPAT;
			column.reference_system = "4326"; // this default SRID is baked into PostGIS (and was the only SRID supported for the geography type in early version)

		} else if (db_type.substr(0, 9) == "geometry(") {
			tie(column.type_restriction, column.reference_system) = extract_spatial_type_restriction_and_reference_system(db_type.substr(9, db_type.length() - 10));
			if (!column.reference_system.empty()) column.flags.simple_geometry = true; // as discussed in column_type, we mainly expect SRIDs to be used with the geography type, but use this flag to turn the column back into a geometry type for this case
			column.column_type = ColumnTypes::SPAT;

		} else if (db_type.substr(0, 10) == "geography(") {
			tie(column.type_restriction, column.reference_system) = extract_spatial_type_restriction_and_reference_system(db_type.substr(10, db_type.length() - 11));
			column.column_type = ColumnTypes::SPAT;

		} else if (type_map.enum_type_values.count(db_type)) {
			column.column_type = ColumnTypes::ENUM;
			column.type_restriction = db_type;
			column.enumeration_values = type_map.enum_type_values[db_type];

		} else {
			// not supported, but leave it till sync_to's check_tables_usable to complain about it so that it can be ignored
			column.column_type = ColumnTypes::UNKN;
			column.db_type_def = db_type;
		}

		table.columns.push_back(column);
	}

	inline string unescape_string_value(const string &escaped) {
		string result;
		result.reserve(escaped.length());
		for (string::size_type n = 0; n < escaped.length(); n++) {
			// this is by no means a complete unescaping function, it only handles the cases seen in
			// the output of pg_get_expr so far.  note that pg does not interpret regular character
			// escapes such as \t and \n when outputting these default definitions.
			if (escaped[n] == '\\' || escaped[n] == '\'') {
				n += 1;
			}
			result += escaped[n];
		}
		return result;
	}

	inline tuple<string, string> extract_spatial_type_restriction_and_reference_system(string type_restriction) {
		transform(type_restriction.begin(), type_restriction.end(), type_restriction.begin(), [](unsigned char c){ return tolower(c); });

		size_t comma_pos = type_restriction.find(',');
		if (comma_pos == string::npos) {
			return make_tuple(type_restriction, "");
		}

		string reference_system(type_restriction.substr(comma_pos + 1));
		type_restriction.resize(comma_pos);
		if (type_restriction == "geometry") type_restriction.clear(); // normalize; note that it still says "geometry" for geography types

		return make_tuple(type_restriction, reference_system);
	}

	Table &table;
	TypeMap &type_map;
};

struct PostgreSQLPrimaryKeyLister {
	inline PostgreSQLPrimaryKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		string column_name = row.string_at(0);
		size_t column_index = table.index_of_column(column_name);
		table.primary_key_columns.push_back(column_index);
		table.primary_key_type = PrimaryKeyType::explicit_primary_key;
	}

	Table &table;
};

struct PostgreSQLKeyLister {
	inline PostgreSQLKeyLister(Table &table): table(table) {}

	inline void operator()(PostgreSQLRow &row) {
		// if we have no primary key, we might need to use another unique key as a surrogate - see PostgreSQLTableLister below
		// furthermore this key must have no NULLable columns, as they effectively make the index not unique
		string key_name = row.string_at(0);
		string column_name = row.string_at(3);
		size_t column_index = table.index_of_column(column_name);
		// FUTURE: consider representing collation, index type, partial keys etc.

		if (table.keys.empty() || table.keys.back().name != key_name) {
			KeyType key_type(KeyType::standard_key);
			if (row.string_at(1) == "t") key_type = KeyType::unique_key;
			if (row.string_at(2).find("USING gist ") != string::npos) key_type = KeyType::spatial_key;
			table.keys.push_back(Key(key_name, key_type));
		}
		table.keys.back().columns.push_back(column_index);
	}

	Table &table;
};

struct PostgreSQLTableLister {
	PostgreSQLTableLister(PostgreSQLClient &client, Database &database, TypeMap &type_map): client(client), database(database), type_map(type_map) {}

	void operator()(PostgreSQLRow &row) {
		Table table(row.string_at(0));

		PostgreSQLColumnLister column_lister(table, type_map);
		client.query(
			"SELECT attname, format_type(atttypid, atttypmod), attnotnull, atthasdef, pg_get_expr(adbin, adrelid), " + attidentity_column() + " AS attidentity "
			  "FROM pg_attribute "
			  "JOIN pg_class ON attrelid = pg_class.oid "
			  "JOIN pg_type ON atttypid = pg_type.oid "
			  "LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum "
			 "WHERE attnum > 0 AND "
			       "NOT attisdropped AND "
			       "relname = '" + client.escape_string_value(table.name) + "' "
			 "ORDER BY attnum",
			column_lister);

		PostgreSQLPrimaryKeyLister primary_key_lister(table);
		client.query(
			"SELECT column_name "
			  "FROM information_schema.table_constraints, "
			       "information_schema.key_column_usage "
			 "WHERE information_schema.table_constraints.table_name = '" + client.escape_string_value(table.name) + "' AND "
			       "information_schema.key_column_usage.table_name = information_schema.table_constraints.table_name AND "
			       "constraint_type = 'PRIMARY KEY' "
			 "ORDER BY ordinal_position",
			primary_key_lister);

		PostgreSQLKeyLister key_lister(table);
		client.query(
			"SELECT indexname, indisunique, indexdef, attname "
			  "FROM (SELECT table_class.oid AS table_oid, index_class.relname AS indexname, pg_index.indisunique, pg_get_indexdef(indexrelid) AS indexdef, generate_series(1, array_length(indkey, 1)) AS position, unnest(indkey) AS attnum "
			          "FROM pg_class table_class, pg_class index_class, pg_index "
			         "WHERE table_class.relname = '" + client.escape_string_value(table.name) + "' AND "
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

		sort(table.keys.begin(), table.keys.end()); // order is arbitrary for keys, but both ends must be consistent, so we sort the keys by name

		database.tables.push_back(table);
	}

	inline string attidentity_column() {
		return (client.supports_generated_as_identity() ? "attidentity" : "NULL");
	}

	PostgreSQLClient &client;
	Database &database;
	TypeMap &type_map;
};

struct PostgreSQLTypeMapCollector {
	PostgreSQLTypeMapCollector(TypeMap &type_map): type_map(type_map) {}

	void operator()(PostgreSQLRow &row) {
		uint32_t oid(row.uint_at(0));
		string typname(row.string_at(1));

		if (typname == "geometry" || typname == "geography") {
			type_map.spatial.insert(oid);
		}
	}

	TypeMap &type_map;
};

struct PostgreSQLEnumValuesCollector {
	PostgreSQLEnumValuesCollector(TypeMap &type_map): type_map(type_map) {}

	void operator()(PostgreSQLRow &row) {
		string typname(row.string_at(0));
		string value(row.string_at(1));

		type_map.enum_type_values[typname].push_back(value);
	}

	TypeMap &type_map;
};

void PostgreSQLClient::populate_types() {
	PostgreSQLTypeMapCollector type_collector(type_map);
	query(
		"SELECT pg_type.oid, pg_type.typname "
		  "FROM pg_type, pg_namespace "
		 "WHERE pg_type.typnamespace = pg_namespace.oid AND "
		       "pg_namespace.nspname = ANY (current_schemas(false)) AND "
		       "pg_type.typname IN ('geometry', 'geography')",
		type_collector);

	PostgreSQLEnumValuesCollector enum_values_collector(type_map);
	query(
		"SELECT typname, "
		       "enumlabel "
		  "FROM pg_type, "
		       "pg_namespace, "
		       "pg_enum "
		 "WHERE pg_type.typnamespace = pg_namespace.oid AND "
		       "pg_namespace.nspname = ANY (current_schemas(false)) AND "
		       "pg_type.oid = enumtypid "
		 "ORDER BY enumtypid, enumsortorder",
		enum_values_collector);
}

void PostgreSQLClient::populate_database_schema(Database &database) {
	PostgreSQLTableLister table_lister(*this, database, type_map);
	query(
		"SELECT pg_class.relname "
		  "FROM pg_class, pg_namespace "
		 "WHERE pg_class.relnamespace = pg_namespace.oid AND "
		       "pg_namespace.nspname = ANY (current_schemas(false)) AND "
		       "relkind = 'r' "
		 "ORDER BY pg_relation_size(pg_class.oid) DESC, relname ASC",
		table_lister);
}


int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
