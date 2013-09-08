#include "schema_functions.h"

#include <algorithm>
#include "sql_functions.h"

template <typename T>
bool name_equal(const T &a, const T &b) {
	return (a.name == b.name);
};

template <typename T>
struct name_is {
	const string &name;
	name_is(const string &name): name(name) {}
	bool operator()(const T& obj) const {
		return (obj.name == name);
	}
};

void report_schema_mismatch(const string &error) {
	// FUTURE: can we implement some kind of non-fatal mismatch handling?
	throw schema_mismatch(error);
}

void check_column_match(const Column &from_column, const Column &to_column) {
	// FUTURE: check column type, collation etc.
}

void check_columns_match(const Table &table, const Columns &from_columns, const Columns &to_columns) {
	Columns::const_iterator to_column = to_columns.begin();
	for (Columns::const_iterator from_column = from_columns.begin(); from_column != from_columns.end(); ++from_column) {
		if (to_column != to_columns.end() && to_column->name == from_column->name) {
			check_column_match(*from_column, *to_column);
			++to_column;

		} else if (find_if(to_column, to_columns.end(), name_is<Column>(from_column->name)) == to_columns.end()) {
			report_schema_mismatch("Missing column " + from_column->name + " on table " + table.name);

		} else if (find_if(from_column, from_columns.end(), name_is<Column>(to_column->name)) == from_columns.end()) {
			report_schema_mismatch("Extra column " + to_column->name + " on table " + table.name);

		} else {
			report_schema_mismatch("Misordered column " + from_column->name + " on table " + table.name + ", should have " + to_column->name + " first");
		}
	}
	if (to_column != to_columns.end()) {
		report_schema_mismatch("Extra column " + to_column->name + " on table " + table.name);
	}
}

void check_primary_key_matches(const Table &table, const ColumnIndices &from_primary_key_columns, const ColumnIndices &to_primary_key_columns) {
	if (from_primary_key_columns.size() != to_primary_key_columns.size() ||
		!equal(from_primary_key_columns.begin(), from_primary_key_columns.end(), to_primary_key_columns.begin())) {
		report_schema_mismatch("Mismatching primary key " + columns_list(table.columns, to_primary_key_columns) + " on table " + table.name + ", should have " + columns_list(table.columns, from_primary_key_columns));
	}
}

void check_table_match(const Table &from_table, const Table &to_table) {
	check_columns_match(from_table, from_table.columns, to_table.columns);
	check_primary_key_matches(from_table, from_table.primary_key_columns, to_table.primary_key_columns);
	// FUTURE: check secondary indexes, collation etc.
}

void check_tables_match(Tables &from_tables, Tables &to_tables) {
	// databases typically return the tables in sorted order, but our algorithm requires it, so we quickly enforce it here
	sort(from_tables.begin(), from_tables.end());
	sort(  to_tables.begin(),   to_tables.end());

	Tables::const_iterator to_table = to_tables.begin();
	for (Tables::const_iterator from_table = from_tables.begin(); from_table != from_tables.end(); ++from_table) {
		if (to_table == to_tables.end() || to_table->name > from_table->name) {
			report_schema_mismatch("Missing table " + from_table->name);

		} else if (to_table->name < from_table->name) {
			report_schema_mismatch("Extra table " + to_table->name);

		} else {
			check_table_match(*from_table, *to_table);
			++to_table;
		}
	}
	if (to_table != to_tables.end()) {
		report_schema_mismatch("Extra table " + to_table->name);
	}
}

void check_schema_match(Database &from_database, Database &to_database) {
	// currently we only pay attention to tables, but in the future we might support other schema items
	check_tables_match(from_database.tables, to_database.tables);
}
