#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>

using namespace std;

typedef vector<size_t> ColumnIndices;
typedef vector<string> ColumnValues;
typedef vector<ColumnValues> Rows;

struct Column {
	string name;

	inline Column(const string &name): name(name) {}
	inline Column() {}
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
