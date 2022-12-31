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
#define POSTGRESQL_12 120000

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

private:
	PostgreSQLRes &_res;
	int _row_number;
};


class PostgreSQLClient: public GlobalKeys, public SequenceColumns, public SetNullability, public SupportsCustomTypes {
public:
	typedef PostgreSQLRow RowType;

	PostgreSQLClient(
		const string &database_host,
		const string &database_port,
		const string &database_username,
		const string &database_password,
		const string &database_name,
		const string &database_schema,
		const string &variables);
	~PostgreSQLClient();

	bool foreign_key_constraints_present();
	void disable_referential_integrity(bool leader);
	string export_snapshot();
	void import_snapshot(const string &snapshot);
	void unhold_snapshot();
	void start_read_transaction();
	void start_write_transaction();
	void commit_transaction();
	void rollback_transaction();

	void populate_types();
	ColumnTypeList supported_types();
	void populate_database_schema(Database &database, const ColumnTypeList &accepted_types);
	void convert_unsupported_database_schema(Database &database);
	string add_unique_relation_name_suffix(string name, set<string> used_relation_names, size_t max_allowed_length);

	string escape_string_value(const string &value);
	string &append_quoted_string_value_to(string &result, const string &value);
	string &append_quoted_bytea_value_to(string &result, const string &value);
	string &append_quoted_spatial_value_to(string &result, const string &value);
	string &append_quoted_column_value_to(string &result, const Column &column, const string &value);
	string column_type(const Column &column);
	string column_default(const Table &table, const Column &column);
	string column_definition(const Table &table, const Column &column);
	string key_definition(const Table &table, const Key &key);

	inline string quote_identifier(const string &name) { return ::quote_identifier(name, '"'); };
	inline string quote_schema_name(const string &schema_name) { return quote_identifier(schema_name.empty() ? default_schema : schema_name); } // whereas in postgresql itself not specifying a schema means use the first entry in the search_path, but in KS it always means the normal default namespace for the database server in question
	inline string quote_table_name(const Table &table) { return quote_schema_name(table.schema_name) + '.' + quote_identifier(table.name); }
	inline bool supports_jsonb_column_type() const { return (server_version >= POSTGRESQL_9_4); }
	inline bool supports_generated_as_identity() const { return (server_version >= POSTGRESQL_10); }
	inline bool supports_generated_columns() const { return (server_version >= POSTGRESQL_12); }
	inline map<string, vector<string>> enum_type_values() const { return type_map.enum_type_values; }

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

public:
	const string specified_schema;
	const string default_schema; // will be the same as specified_schema if there is one, and "public" if not

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
	const string &database_username,
	const string &database_password,
	const string &database_name,
	const string &database_schema,
	const string &variables):
	specified_schema(database_schema),
	default_schema(database_schema.empty() ? "public" : database_schema) {

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

	if (!database_schema.empty()) {
		execute("SET search_path=" + quote_identifier(database_schema));
	}

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

bool PostgreSQLClient::foreign_key_constraints_present() {
	return (atoi(select_one(
		"SELECT COUNT(*)" \
		 " FROM information_schema.table_constraints" \
		" WHERE constraint_schema = ANY(current_schemas(false)) AND" \
		      " constraint_type = 'FOREIGN KEY'").c_str()));
}

void PostgreSQLClient::disable_referential_integrity(bool leader) {
	// disable foreign key checks so that we can work on tables in multiple workers and in any
	// order.  enabling and disabling all the triggers using ALTER TABLE ... DISABLE TRIGGER ALL
	// would be problematic because if we fail, we wouldn't be able to re-enable the triggers and
	// if we re-run we wouldn't know which triggers should be enabled again at the end.  so
	// instead we implement this by changing session_replication_role - at the expense of firing
	// any triggers actually set to REPLICA (which doesn't seem inappropriate given the use case).
	// changing this setting requires superuser permissions, so don't try if we know we don't have that.
	if (select_one("SELECT current_setting('is_superuser')") == "on") {
		execute("SET session_replication_role = 'replica'");
	} else if (leader && foreign_key_constraints_present()) {
		cerr << "Warning: can't disable foreign key constraint triggers without superuser privileges.  You may experience foreign key violation errors or locking problems if the database has foreign key constraints." << endl;
	}
}

string PostgreSQLClient::escape_string_value(const string &value) {
	string result;
	result.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)result.data(), value.c_str(), value.size(), nullptr);
	result.resize(result_length);
	return result;
}

string &PostgreSQLClient::append_quoted_string_value_to(string &result, const string &value) {
	string buffer;
	buffer.resize(value.size()*2 + 1);
	size_t result_length = PQescapeStringConn(conn, (char*)buffer.data(), value.c_str(), value.size(), nullptr);
	result += '\'';
	result.append(buffer, 0, result_length);
	result += '\'';
	return result;
}

string &PostgreSQLClient::append_quoted_bytea_value_to(string &result, const string &value) {
	size_t encoded_length;
	const unsigned char *encoded = PQescapeByteaConn(conn, (const unsigned char *)value.c_str(), value.size(), &encoded_length);
	result += '\'';
	result.append(encoded, encoded + encoded_length - 1); // encoded_length includes the null terminator
	result += '\'';
	PQfreemem((void *)encoded);
	return result;
}

string &PostgreSQLClient::append_quoted_spatial_value_to(string &result, const string &value) {
	result.append("ST_GeomFromEWKB(");
	append_quoted_bytea_value_to(result, value);
	result.append(")");
	return result;
}

string &PostgreSQLClient::append_quoted_column_value_to(string &result, const Column &column, const string &value) {
	if (!column.values_need_quoting()) {
		return result += value;
	} else if (column.column_type == ColumnType::binary) {
		return append_quoted_bytea_value_to(result, value);
	} else if (column.column_type == ColumnType::spatial) {
		return append_quoted_spatial_value_to(result, value);
	} else {
		return append_quoted_string_value_to(result, value);
	}
}

void PostgreSQLClient::convert_unsupported_database_schema(Database &database) {
	map<string, set<string>> used_relation_names_by_schema;

	for (Table &table : database.tables) {
		// normally we're syncing from an entire database to an entire database, and specified_schema will be left empty.
		// if it's set to anything else, we're syncing into a single specific schema, and although in principle we could
		// still allow other schemas as well, that's probably going to be a surprise to the user, or at least they'd
		// probably want to set mappings for each named schema.
		if (!table.schema_name.empty() && !specified_schema.empty()) {
			database.errors.push_back(
				"Can't place table " + table.name + " from schema " + table.schema_name + " into schema " + default_schema + ". "
				"To sync into a single specific schema you must sync from a single specific schema only.");
			return;
		}

		// check all the columns
		for (Column &column : table.columns) {
			// the modern type system negotiates compatible integer types, so we never get given types
			// that postgresql won't support.  we have to support converting legacy schemas here though.
			// postgresql doesn't support unsigned columns; to make migration from databases that do
			// easier, we don't reject unsigned columns, we just convert them to the signed equivalent
			// and rely on it raising if we try to insert an invalid value.  postgresql also doesn't
			// have 8 or 24 bit types, so we upgrade those to 16 bit.
			if (column.column_type == ColumnType::sint_8bit || column.column_type == ColumnType::uint_8bit || column.column_type == ColumnType::uint_16bit) {
				column.column_type = ColumnType::sint_16bit;
			}
			if (column.column_type == ColumnType::sint_24bit || column.column_type == ColumnType::uint_24bit || column.column_type == ColumnType::uint_32bit) {
				column.column_type = ColumnType::sint_32bit;
			}

			// postgresql's doesn't support mysql's FLOAT(M,D)/DOUBLE(M,D) extension; they'll be ignored
			// by the type mapping code, but zero those fields here so the schema looks equal to schema_matcher
			if (column.column_type == ColumnType::float_32bit || column.column_type == ColumnType::float_64bit) {
				column.size = column.scale = 0;
			}

			// postgresql doesn't have different sized TEXT/BLOB columns, they're all equivalent to mysql's biggest type
			if (column.column_type == ColumnType::text || column.column_type == ColumnType::binary) {
				column.size = 0;
			}

			// postgresql requires that you create a material type for each enumeration, whereas mysql just lists the
			// possible values on the column itself.  when syncing from mysql, we don't currently implement creation/maintainance of these custom
			// types ourselves - users need to do that - but we need to find the name of the type they've (hopefully) created.
			if (column.column_type == ColumnType::enumeration && column.subtype.empty()) {
				for (auto it = type_map.enum_type_values.begin(); it != type_map.enum_type_values.end() && column.subtype.empty(); ++it) {
					if (it->second == column.enumeration_values) {
						column.subtype = it->first;
					}
				}
			}

			// support for the new standard SQL GENERATED ... AS IDENTITY syntax was added in postgresql 10, downgrade to sequences for earlier versions
			if (!supports_generated_as_identity() && (column.default_type == DefaultType::generated_by_default_as_identity || column.default_type == DefaultType::generated_always_as_identity)) {
				column.default_type = DefaultType::generated_by_sequence;
			}

			// we didn't previously serialise the sequence name
			if (column.default_type == DefaultType::generated_by_sequence && column.default_value.empty()) {
				// name to match what postgresql creates for serial columns
				column.default_value = table.name + "_" + column.name + "_seq";
			}

			// support for VIRTUAL generated columns was yanked from postgresql 12 as it wasn't ready
			if (column.default_type == DefaultType::generated_always_virtual) {
				column.default_type = DefaultType::generated_always_stored;
			}

			// postgresql doesn't have an equivalent to mysql's 'ON UPDATE CURRENT_TIMESTAMP', which is itself not considered current;
			// you'd have to create a trigger to get the same effect on postgresql, which is outside of scope for KS
			if (column.auto_update_type != AutoUpdateType::no_auto_update) {
				column.auto_update_type = AutoUpdateType::no_auto_update;
			}
		}

		// check all the keys
		for (Key &key : table.keys) {
			if (key.name.size() >= 63) {
				// postgresql has a hardcoded limit of 63 characters for index names
				key.name = key.name.substr(0, 63);
			}
		}

		// see below
		used_relation_names_by_schema[table.schema_name].insert(table.name);
	}

	// postgresql index names are unique across the whole schema, whereas they are unique only within a table for some databases.
	// the underlying cause is that in pg, indexes are relations, and since relation names are unique across the schema, indexes also
	// cannot have the same name as any other relations, including tables - which is why we have to do two passes over database.tables.
	// rename indexes to fix any conflicts, on the assumption that users who aren't allowed to modify the source schema would prefer
	// this to failing to sync, and everyone else who doesn't like our renames can rename their indexes themselves.
	for (Table &table : database.tables) {
		set<string> &used_relation_names(used_relation_names_by_schema[table.schema_name]);

		for (Key &key : table.keys) {
			if (used_relation_names.count(key.name)) {
				key.name = add_unique_relation_name_suffix(key.name, used_relation_names, 63);
			}

			used_relation_names.insert(key.name);
		}
	}

	// if syncing to a whole database, we check that the schemas we're told the tables belong to are all in search_path (at the
	// 'to' end) because if not, populate_database_schema will not see them and we'll incorrectly think they need to be created
	// which of course won't work since they already exist.
	if (specified_schema.empty()) {
		string current_schemas_list(select_one("SELECT current_schemas(false)"));
		current_schemas_list = current_schemas_list.substr(1, current_schemas_list.length() - 2);

		set<string> current_schemas(split_list(current_schemas_list, ","));
		set<string> missing_schemas;

		for (const auto schema_and_relation_names : used_relation_names_by_schema) {
			const string schema_name(schema_and_relation_names.first.empty() ? default_schema : schema_and_relation_names.first);
			if (!current_schemas.count(schema_name) && !current_schemas.count(quote_identifier(schema_name))) {
				missing_schemas.insert(schema_name);
			}
		}

		if (!missing_schemas.empty()) {
			string error("The search_path is currently set to ");
			error += current_schemas_list;
			error += " but there are also tables in ";
			for (const string s : missing_schemas) {
				if (s != *missing_schemas.begin()) error += ", ";
				error += s;
			}
			error += ". Either add to the 'to' end search_path using --set-to-variables or restrict the 'from' end search_path using --set-from-variables.";
			database.errors.push_back(error);
		}
	}
}

string PostgreSQLClient::add_unique_relation_name_suffix(string name, set<string> used_relation_names, size_t max_allowed_length) {
	for (size_t counter = 2; counter < numeric_limits<size_t>::max(); counter++) {
		string suffix(to_string(counter));
		if (name.length() + suffix.length() > max_allowed_length) {
			name = name.substr(0, max_allowed_length - suffix.length());
		}

		string renumbered_name(name + suffix);
		if (!used_relation_names.count(renumbered_name)) {
			return renumbered_name;
		}
	}

	// not really possible to ever get here, but return the original name and let the database explain the problem
	return name;
}

map<ColumnType, string> SimpleColumnTypes{
	{ColumnType::binary,       "bytea"},
	{ColumnType::text,         "text"},
	{ColumnType::text_varchar, "character varying"},
	{ColumnType::text_fixed,   "character"},
	{ColumnType::json,         "json"},
	{ColumnType::json_binary,  "jsonb"},
	{ColumnType::uuid,         "uuid"},
	{ColumnType::boolean,      "boolean"},
	{ColumnType::sint_16bit,   "smallint"},
	{ColumnType::sint_32bit,   "integer"},
	{ColumnType::sint_64bit,   "bigint"},
	{ColumnType::float_64bit,  "double precision"},
	{ColumnType::float_32bit,  "real"},
	{ColumnType::decimal,      "numeric"},
	{ColumnType::date,         "date"},
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

string PostgreSQLClient::column_type(const Column &column) {
	auto simple_type = SimpleColumnTypes.find(column.column_type);
	if (simple_type != SimpleColumnTypes.cend()) {
		return simple_type->second + column_type_suffix(column);
	}

	switch (column.column_type) {
		case ColumnType::time:
			return "time" + column_type_suffix(column, 6) + " without time zone";

		case ColumnType::time_tz:
			return "time" + column_type_suffix(column, 6) + " with time zone";

		case ColumnType::datetime:
			return "timestamp" + column_type_suffix(column, 6) + " without time zone";

		case ColumnType::datetime_tz:
			return "timestamp" + column_type_suffix(column, 6) + " with time zone";

		case ColumnType::spatial:
		case ColumnType::spatial_geography: {
			// note that we have made the assumption that all the mysql geometry types should be mapped to
			// PostGIS objects, rather than to the built-in geometric types such as POINT, because
			// postgresql's built-in geometric types don't support spatial reference systems (SRIDs), don't
			// have any equivalent to the multi* types, the built-in POLYGON type doesn't support 'holes' (as
			// created using the two-argument form on mysql), etc.
			// this still leaves us with a choice between geometry and geography; the difference is that
			// geography does calculations on spheroids whereas geometry calculates using simple planes.
			// geography defaults to SRID 4326 (and until recently, only supported 4326) and will even use
			// that default if you specify 0.  geometry allows you to specify any SRID and remembers it, but
			// always calculates is as if using SRID 0, so there's no real benefit to doing so.  we've added
			// separate types for the two postgresql types to allow us to round-trip the schema accurately.
			string result(column.column_type == ColumnType::spatial_geography ? "geography" : "geometry");
			if (!column.reference_system.empty()) {
				result += '(';
				result += (column.subtype.empty() ? string("geometry") : column.subtype);
				result += ',';
				result += column.reference_system;
				result += ')';
			} else if (!column.subtype.empty()) {
				result += '(';
				result += column.subtype;
				result += ')';
			}
			return result;
		}

		case ColumnType::enumeration:
			if (column.subtype.empty()) {
				throw runtime_error("Can't find an enumerated type with possible values " + values_list(*this, column.enumeration_values) + " for column " + column.name + ", please create one using CREATE TYPE");
			}
			if (type_map.enum_type_values.count(column.subtype) && // otherwise one will be created by CustomTypeMatcher
				type_map.enum_type_values[column.subtype] != column.enumeration_values) {
				throw runtime_error("The enumerated type named " + column.subtype + " has possible values " + values_list(*this, type_map.enum_type_values[column.subtype]) + " but should have possible values " + values_list(*this, column.enumeration_values) + " for column " + column.name + ", please alter the type");
			}
			return column.subtype;

		case ColumnType::postgresql_specific:
			// as long as the versions are compatible, this should 'just work'
			return column.subtype;

		case ColumnType::unknown:
			// fall back to the raw type string given by the other end, which is really only likely to
			// work if the other end is the same type of database server (and maybe even a compatible
			// version). this also implies we don't know anything about parsing/formatting values for
			// this column type, so it'll only work if the database accepts exactly the same input as
			// it gives in output.
			return column.subtype;

		default:
			throw runtime_error("Don't know how to express column type of " + column.name + " (" + to_string(static_cast<int>(column.column_type)) + ")");
	}
}

string PostgreSQLClient::column_default(const Table &table, const Column &column) {
	switch (column.default_type) {
		case DefaultType::no_default:
			return " DEFAULT NULL";

		case DefaultType::generated_by_default_as_identity:
			return " GENERATED BY DEFAULT AS IDENTITY";

		case DefaultType::generated_always_as_identity:
			return " GENERATED ALWAYS AS IDENTITY";

		case DefaultType::generated_by_sequence:
			return string(" DEFAULT nextval('" + escape_string_value(quote_schema_name(table.schema_name) + '.' + quote_identifier(column.default_value)) + "'::regclass)");

		case DefaultType::default_value: {
			string result(" DEFAULT ");
			append_quoted_column_value_to(result, column, column.default_value);
			return result;
		}

		case DefaultType::default_expression:
			return " DEFAULT " + column.default_value; // the only expression accepted prior to support for arbitrary expressions

		case DefaultType::generated_always_stored:
			return " GENERATED ALWAYS AS (" + column.default_value + ") STORED";

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
	result += quote_table_name(table);
	result += ' ';
	if (key.spatial()) result += "USING gist ";
	result += columns_tuple(*this, table.columns, key.columns);
	return result;
}

inline ColumnTypeList PostgreSQLClient::supported_types() {
	ColumnTypeList result{
		ColumnType::time,
		ColumnType::time_tz,
		ColumnType::datetime,
		ColumnType::datetime_tz,
		ColumnType::enumeration,
		ColumnType::postgresql_specific,
		ColumnType::unknown,
	};
	for (const auto &it: SimpleColumnTypes) {
		result.insert(it.first);
	}
	if (!type_map.spatial.empty()) {
		result.insert(ColumnType::spatial);
		result.insert(ColumnType::spatial_geography);
	}
	if (!supports_jsonb_column_type()) {
		result.erase(ColumnType::json_binary); // in SimpleColumnTypes, but only supported by some versions
	}
	return result;
}

struct PostgreSQLColumnLister {
	inline PostgreSQLColumnLister(PostgreSQLClient &client, Table &table, TypeMap &type_map, const ColumnTypeList &accepted_types): client(client), table(table), type_map(type_map), accepted_types(accepted_types) {}

	inline void operator()(PostgreSQLRow &row) {
		Column column;

		column.name = row.string_at(0);
		string db_type(row.string_at(1));
		column.nullable = (row.string_at(2) == "f");

		if (!row.null_at(5) && row.string_at(5) != "\0") {
			// attidentity set (will be 'a' for 'always' and 'd' for 'by default')
			if (row.string_at(5) == "a") {
				column.default_type = DefaultType::generated_always_as_identity;
			} else {
				column.default_type = DefaultType::generated_by_default_as_identity;
			}

		} else if (!row.null_at(6) && row.string_at(6) == "s") {
			// attgenerated set ('s' for 'stored', virtual not supported yet)
			column.default_type = DefaultType::generated_always_stored;
			column.default_value = row.string_at(4);

		} else if (row.string_at(3) == "t") {
			column.default_type = DefaultType::default_value;
			column.default_value = row.string_at(4);

			if (column.default_value.length() > 20 &&
				column.default_value.substr(0, 9) == "nextval('" &&
				column.default_value.substr(column.default_value.length() - 12, 12) == "'::regclass)") {
				// this is what you got back when you used the historic SERIAL pseudo-type (subsequently replaced by the new standard IDENTITY GENERATED ... AS IDENTITY)
				column.default_type = DefaultType::generated_by_sequence;
				string quoted_sequence_name(column.default_value.substr(9, column.default_value.length() - 21));

				// postgresql requires that sequences belong to the same schema as the table that uses them, so we should always find that the sequence name is prefixed
				// by the same schema name, if it's not in the 'public' schema.  chop this off here; otherwise when it's applied at the other end, it'll all get quoted
				// together with the sequence name as a single big identifier, which won't work.  we look for both quoted and unquoted forms rather than trying to guess
				// whether postgresql would've used the quoted form.  this isn't always present for tables in the 'public' schema, but it sometimes is, so look anyway.
				quoted_sequence_name = remove_specific_schema_from_identifier(quoted_sequence_name, table.schema_name.empty() ? client.default_schema : table.schema_name);

				column.default_value = unquote_identifier(quoted_sequence_name);

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
			column.column_type = ColumnType::boolean;

		} else if (db_type == "smallint") {
			column.column_type = ColumnType::sint_16bit;

		} else if (db_type == "integer") {
			column.column_type = ColumnType::sint_32bit;

		} else if (db_type == "bigint") {
			column.column_type = ColumnType::sint_64bit;

		} else if (db_type == "real") {
			column.column_type = ColumnType::float_32bit;

		} else if (db_type == "double precision") {
			column.column_type = ColumnType::float_64bit;

		} else if (db_type.substr(0, 8) == "numeric(") {
			column.column_type = ColumnType::decimal;
			column.size = extract_column_length(db_type);
			column.scale = extract_column_scale(db_type);

		} else if (db_type.substr(0, 7) == "numeric") {
			column.column_type = ColumnType::decimal;

		} else if (db_type == "bytea") {
			column.column_type = ColumnType::binary;

		} else if (db_type.substr(0, 18) == "character varying(") {
			column.column_type = ColumnType::text_varchar;
			column.size = extract_column_length(db_type);

		} else if (db_type.substr(0, 18) == "character varying") {
			column.column_type = ColumnType::text_varchar; /* no length limit */

		} else if (db_type.substr(0, 10) == "character(") {
			column.column_type = ColumnType::text_fixed;
			column.size = extract_column_length(db_type);

		} else if (db_type == "text") {
			column.column_type = ColumnType::text;

		} else if (db_type == "json") {
			column.column_type = select_supported_type(ColumnType::json, ColumnType::text);

		} else if (db_type == "jsonb") {
			column.column_type = select_supported_type(ColumnType::json_binary, ColumnType::json, ColumnType::text);

		} else if (db_type == "uuid") {
			column.column_type = select_supported_type(ColumnType::uuid, ColumnType::text_fixed);
			if (column.column_type == ColumnType::text_fixed) column.size = 36;

		} else if (db_type == "date") {
			column.column_type = ColumnType::date;

		} else if (db_type.substr(0, 5) == "time(" || db_type.substr(0, 5) == "time ") {
			if (db_type.find("with time zone") != string::npos) {
				column.column_type = select_supported_type(ColumnType::time_tz, ColumnType::time);
			} else {
				column.column_type = ColumnType::time;
			}
			if (db_type.substr(0, 5) == "time(") {
				column.size = extract_column_length(db_type);
			} else {
				column.size = 6; /* microsecond precision */
			}

		} else if (db_type.substr(0, 10) == "timestamp(" || db_type.substr(0, 10) == "timestamp ") {
			if (db_type.find("with time zone") != string::npos) {
				column.column_type = select_supported_type(ColumnType::datetime_tz, ColumnType::datetime);
			} else {
				column.column_type = ColumnType::datetime;
			}
			if (db_type.substr(0, 10) == "timestamp(") {
				column.size = extract_column_length(db_type);
			} else {
				column.size = 6; /* microsecond precision */
			}

		} else if (db_type == "geometry") {
			column.column_type = ColumnType::spatial;

		} else if (db_type == "geography") {
			column.column_type = ColumnType::spatial_geography;
			column.reference_system = "4326"; // this default SRID is baked into PostGIS (and was the only SRID supported for the geography type in early version)

		} else if (db_type.substr(0, 9) == "geometry(") {
			tie(column.subtype, column.reference_system) = extract_spatial_subtype_and_reference_system(db_type.substr(9, db_type.length() - 10));
			column.column_type = ColumnType::spatial;

		} else if (db_type.substr(0, 10) == "geography(") {
			tie(column.subtype, column.reference_system) = extract_spatial_subtype_and_reference_system(db_type.substr(10, db_type.length() - 11));
			column.column_type = ColumnType::spatial_geography;

		} else if (type_map.enum_type_values.count(db_type)) {
			column.column_type = ColumnType::enumeration;
			column.subtype = db_type;
			column.enumeration_values = type_map.enum_type_values[db_type];

		} else {
			// not supported, but leave it till sync_to's check_tables_usable to complain about it so that it can be ignored
			column.column_type = ColumnType::postgresql_specific;
		}

		// degrade to 'unknown' if the type we want isn't supported by the version at the other end
		if (!accepted_types.count(column.column_type)) {
			column.column_type = ColumnType::unknown;
			column.size = column.scale = 0;
		}

		// send the raw database-specific type for unknown or unsupported types
		if (column.column_type == ColumnType::postgresql_specific || column.column_type == ColumnType::unknown) {
			column.subtype = db_type;
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

	inline string unquote_identifier(const string &escaped) {
		if (escaped.empty() || escaped[0] != '"' || escaped[escaped.length() - 1] != '"') return escaped;
		string result;
		result.reserve(escaped.length() - 2);
		for (string::size_type n = 1; n < escaped.length() - 1; n++) {
			if (escaped[n] == '"') {
				n += 1;
			}
			result += escaped[n];
		}
		return result;
	}

	inline string remove_specific_schema_from_identifier(const string &escaped, const string &schema_name) {
		if (escaped.substr(0, schema_name.length() + 1) == schema_name + '.') {
			return escaped.substr(schema_name.length() + 1);
		}

		string quoted_schema_name(quote_identifier(schema_name, '"'));
		if (escaped.substr(0, quoted_schema_name.length() + 1) == quoted_schema_name + '.') {
			return escaped.substr(quoted_schema_name.length() + 1);
		}

		return escaped;
	}

	inline tuple<string, string> extract_spatial_subtype_and_reference_system(string subtype) {
		transform(subtype.begin(), subtype.end(), subtype.begin(), [](unsigned char c){ return tolower(c); });

		size_t comma_pos = subtype.find(',');
		if (comma_pos == string::npos) {
			return make_tuple(subtype, "");
		}

		string reference_system(subtype.substr(comma_pos + 1));
		subtype.resize(comma_pos);
		if (subtype == "geometry") subtype.clear(); // normalize; note that it still says "geometry" for geography types

		return make_tuple(subtype, reference_system);
	}

	inline ColumnType select_supported_type(ColumnType type1, ColumnType type2, ColumnType type3 = ColumnType::postgresql_specific) {
		if (accepted_types.count(type1)) return type1;
		if (accepted_types.count(type2)) return type2;
		if (accepted_types.count(type3)) return type3;
		return ColumnType::postgresql_specific;
	}

	PostgreSQLClient &client;
	Table &table;
	TypeMap &type_map;
	const ColumnTypeList &accepted_types;
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
	PostgreSQLTableLister(PostgreSQLClient &client, Database &database, TypeMap &type_map, const ColumnTypeList &accepted_types): client(client), database(database), type_map(type_map), accepted_types(accepted_types) {}

	void operator()(PostgreSQLRow &row) {
		string schema_name(row.string_at(0));
		Table table(schema_name == client.default_schema ? "" : schema_name, row.string_at(1));

		PostgreSQLColumnLister column_lister(client, table, type_map, accepted_types);
		client.query(
			"SELECT attname, format_type(atttypid, atttypmod), attnotnull, atthasdef, pg_get_expr(adbin, adrelid), " + attidentity_column() + " AS attidentity, " + attgenerated_column() + " AS attgenerated "
			  "FROM pg_attribute "
			  "JOIN pg_class ON attrelid = pg_class.oid "
			  "JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid "
			  "JOIN pg_type ON atttypid = pg_type.oid "
			  "LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum "
			 "WHERE attnum > 0 AND "
			       "NOT attisdropped AND "
			       "pg_namespace.nspname = '" + client.escape_string_value(schema_name) + "' AND "
			       "relname = '" + client.escape_string_value(table.name) + "' "
			 "ORDER BY attnum",
			column_lister);

		PostgreSQLPrimaryKeyLister primary_key_lister(table);
		client.query(
			"SELECT column_name "
			  "FROM information_schema.table_constraints, "
			       "information_schema.key_column_usage "
			 "WHERE information_schema.table_constraints.table_schema = '" + client.escape_string_value(schema_name) + "' AND "
			       "information_schema.table_constraints.table_name = '" + client.escape_string_value(table.name) + "' AND "
			       "information_schema.key_column_usage.table_schema = information_schema.table_constraints.table_schema AND "
			       "information_schema.key_column_usage.constraint_name = information_schema.table_constraints.constraint_name AND "
			       "constraint_type = 'PRIMARY KEY' "
			 "ORDER BY ordinal_position",
			primary_key_lister);

		PostgreSQLKeyLister key_lister(table);
		client.query(
			"SELECT indexname, indisunique, indexdef, attname "
			  "FROM (SELECT table_class.oid AS table_oid, index_class.relname AS indexname, pg_index.indisunique, pg_get_indexdef(indexrelid) AS indexdef, generate_series(1, array_length(indkey, 1)) AS position, unnest(indkey) AS attnum "
			          "FROM pg_namespace, pg_class table_class, pg_class index_class, pg_index "
			         "WHERE pg_namespace.nspname = '" + client.escape_string_value(schema_name) + "' AND "
			               "table_class.relnamespace = pg_namespace.oid AND "
			               "table_class.relname = '" + client.escape_string_value(table.name) + "' AND "
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

	inline string attgenerated_column() {
		return (client.supports_generated_columns() ? "attgenerated" : "NULL");
	}

	PostgreSQLClient &client;
	Database &database;
	TypeMap &type_map;
	const ColumnTypeList &accepted_types;
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

void PostgreSQLClient::populate_database_schema(Database &database, const ColumnTypeList &accepted_types) {
	PostgreSQLTableLister table_lister(*this, database, type_map, accepted_types);
	query(
		"SELECT pg_namespace.nspname, pg_class.relname "
		  "FROM pg_namespace, pg_class "
		 "WHERE pg_namespace.nspname = ANY (current_schemas(false)) AND "
		       "pg_class.relnamespace = pg_namespace.oid AND "
		       "relkind = 'r' "
		 "ORDER BY pg_relation_size(pg_class.oid) DESC, pg_namespace.nspname ASC, relname ASC",
		table_lister);
}


int main(int argc, char *argv[]) {
	return endpoint_main<PostgreSQLClient>(argc, argv);
}
