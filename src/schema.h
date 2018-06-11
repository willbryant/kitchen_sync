#ifndef SCHEMA_H
#define SCHEMA_H

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
	const string BOOL = "BOOL";
	const string SINT = "INT";
	const string UINT = "INT UNSIGNED";
	const string REAL = "REAL";
	const string DECI = "DECIMAL";
	const string DATE = "DATE";
	const string TIME = "TIME";
	const string DTTM = "DATETIME";
	const string POIN = "POINT";

	const string UNKN = "UNKNOWN";
}

enum DefaultType {
	no_default = 0,
	sequence = 1,
	default_value = 2,
	default_function = 3,
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

	// serialized but not compared; used only for passing along unknown column types so you get an intelligible error, and non-portable
	string db_type_def;

	// the following member isn't serialized currently (could be, but not required):
	string filter_expression;

	inline Column(const string &name, bool nullable, DefaultType default_type, string default_value, string column_type, size_t size = 0, size_t scale = 0, ColumnFlags flags = ColumnFlags::nothing, const string &db_type_def = ""): name(name), nullable(nullable), default_type(default_type), default_value(default_value), column_type(column_type), size(size), scale(scale), flags(flags), db_type_def(db_type_def) {}
	inline Column(): nullable(true), size(0), scale(0), default_type(DefaultType::no_default), flags(ColumnFlags::nothing) {}

	inline bool operator ==(const Column &other) const { return (name == other.name && nullable == other.nullable && column_type == other.column_type && size == other.size && scale == other.scale && default_type == other.default_type && default_value == other.default_value && flags == other.flags); }
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

struct Table {
	string name;
	Columns columns;
	ColumnIndices primary_key_columns;
	Keys keys;

	// the following member isn't serialized currently (could be, but not required):
	string where_conditions;

	inline Table(const string &name): name(name) {}
	inline Table() {}

	inline bool operator <(const Table &other) const { return (name < other.name); }
	inline bool operator ==(const Table &other) const { return (name == other.name && columns == other.columns && primary_key_columns == other.primary_key_columns && keys == other.keys); }
	inline bool operator !=(const Table &other) const { return (!(*this == other)); }
	size_t index_of_column(const string &name) const;
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

#endif
