#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>

using namespace std;

struct Column {
	string name;

	inline Column(string name): name(name) { }
	inline Column() { }
};

typedef vector<Column> Columns;

struct Table {
	string name;
	Columns columns;

	inline Table(string name): name(name) { }
	inline Table() { }

	inline bool operator <(const Table &other) const { return (name < other.name); }
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

#endif
