#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>

using namespace std;

struct Column {
	string name;

	inline Column(string name): name(name) { }
};

typedef vector<Column> Columns;

struct Table {
	string name;
	Columns columns;

	inline Table(string name): name(name) { }
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
};

#endif
