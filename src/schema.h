#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>
#include <map>
#include "message_pack/packed_value.h"

using namespace std;

typedef vector<size_t> ColumnIndices;
typedef vector<PackedValue> ColumnValues;
typedef vector<ColumnValues> Rows;

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
}

struct Column {
	string name;
	bool nullable;
	string column_type;
	size_t size;
	size_t scale;
	vector<string> labels;

	// the following member isn't serialized currently (could be, but not required):
	string filter_expression;

	inline Column(const string &name, bool nullable, string column_type, size_t size = 0, size_t scale = 0): name(name), nullable(nullable), column_type(column_type), size(size), scale(scale) {}
	inline Column(): size(0), scale(0), nullable(true) {}
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
	size_t index_of_column(const string &name) const;
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

#endif
