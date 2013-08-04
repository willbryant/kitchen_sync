#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <vector>

using namespace std;

struct Column {
	string name;

	inline Column(const string &name): name(name) { }
	inline Column() { }
};

typedef vector<Column> Columns;
typedef vector<string> ColumnNames;
typedef vector<size_t> ColumnIndices;

struct Table {
	string name;
	Columns columns;
	ColumnIndices primary_key_columns;

	inline Table(const string &name): name(name) { }
	inline Table() { }

	inline bool operator <(const Table &other) const { return (name < other.name); }
	size_t index_of_column(const string &name) const;
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

typedef vector<string> RowValues;
typedef vector<RowValues> Rows;

#endif
