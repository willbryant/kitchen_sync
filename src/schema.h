#ifndef SCHEMA_H
#define SCHEMA_H

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include "message_pack/packed_value.h"

using namespace std;

typedef vector<size_t> ColumnIndices;
typedef vector<PackedValue> ColumnValues;

namespace ColumnTypes {
	const string BLOB = "BLOB";
	const string TEXT = "TEXT";
	const string VCHR = "VARCHAR";
	const string FCHR = "CHAR";
	const string UUID = "UUID";
	const string BOOL = "BOOL";
	const string SINT = "INT";
	const string UINT = "INT UNSIGNED";
	const string REAL = "REAL";
	const string DECI = "DECIMAL";
	const string DATE = "DATE";
	const string TIME = "TIME";
	const string DTTM = "DATETIME";
	const string SPAT = "SPATIAL";

	const string UNKN = "UNKNOWN";
}

enum DefaultType {
	no_default = 0,
	sequence = 1,
	default_value = 2,
	default_expression = 3,
};

enum ColumnFlags {
	nothing = 0,
	mysql_timestamp = 1,
	mysql_on_update_timestamp = 2,
	time_zone = 4,
};

struct Column {
	string name;
	bool nullable;
	string column_type;
	size_t size;
	size_t scale;
	DefaultType default_type;
	string default_value;
	ColumnFlags flags;
	string type_restriction;
	string reference_system;

	// serialized but not compared; used only for passing along unknown column types so you get an intelligible error, and non-portable
	string db_type_def;

	// the following member isn't serialized currently (could be, but not required):
	string filter_expression;

	inline Column(const string &name, bool nullable, DefaultType default_type, string default_value, string column_type, size_t size = 0, size_t scale = 0, ColumnFlags flags = ColumnFlags::nothing, const string &type_restriction = "", const string &reference_system = "", const string &db_type_def = ""): name(name), nullable(nullable), default_type(default_type), default_value(default_value), column_type(column_type), size(size), scale(scale), flags(flags), type_restriction(type_restriction), reference_system(reference_system), db_type_def(db_type_def) {}
	inline Column(): nullable(true), size(0), scale(0), default_type(DefaultType::no_default), flags(ColumnFlags::nothing) {}

	inline bool operator ==(const Column &other) const { return (name == other.name && nullable == other.nullable && column_type == other.column_type && size == other.size && scale == other.scale && default_type == other.default_type && default_value == other.default_value && flags == other.flags && type_restriction == other.type_restriction && reference_system == other.reference_system); }
	inline bool operator !=(const Column &other) const { return (!(*this == other)); }
};

typedef vector<Column> Columns;
typedef vector<string> ColumnNames;

struct Key {
	string name;
	bool unique;
	ColumnIndices columns;

	inline Key(const string &name, bool unique): name(name), unique(unique) {}
	inline Key() {}

	inline bool operator <(const Key &other) const { return (unique != other.unique ? unique : name < other.name); }
	inline bool operator ==(const Key &other) const { return (name == other.name && unique == other.unique && columns == other.columns); }
	inline bool operator !=(const Key &other) const { return (!(*this == other)); }
};

typedef vector<Key> Keys;

enum PrimaryKeyType {
	no_available_key = 0,
	explicit_primary_key = 1,
	suitable_unique_key = 2,
};

struct Table {
	string name;
	Columns columns;
	ColumnIndices primary_key_columns;
	PrimaryKeyType primary_key_type = PrimaryKeyType::no_available_key;
	Keys keys;

	// the following member isn't serialized currently (could be, but not required):
	string where_conditions;

	inline Table(const string &name): name(name) {}
	inline Table() {}

	inline bool operator <(const Table &other) const { return (name < other.name); }
	inline bool operator ==(const Table &other) const { return (name == other.name && columns == other.columns && same_primary_key_as(other) && keys == other.keys); }
	inline bool operator !=(const Table &other) const { return (!(*this == other)); }
	size_t index_of_column(const string &name) const;

protected:
	inline bool same_primary_key_as(const Table &other) const {
		size_t this_explicit_columns = primary_key_type == explicit_primary_key ? primary_key_columns.size() : 0;
		size_t that_explicit_columns = other.primary_key_type == explicit_primary_key ? other.primary_key_columns.size() : 0;
		return (this_explicit_columns == that_explicit_columns && equal(primary_key_columns.begin(), primary_key_columns.begin() + this_explicit_columns, other.primary_key_columns.begin()));
	}
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

#endif
