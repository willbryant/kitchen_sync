#include <iostream>
#include <unistd.h>

#include "schema.h"

const bool schema_mismatches_are_fatal = true; // will be an option in the future

struct schema_mismatch : public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

template <class T>
bool name_equal(const T &a, const T &b) {
	return (a.name == b.name);
};

template <class T>
struct name_is {
	const string &name;
	name_is(const string &name): name(name) {}
	bool operator()(const T& obj) const {
		return (obj.name == name);
	}
};

string columns_list(const ColumnNames &column_names) {
	if (column_names.empty()) {
		return "(NULL)";
	}

	string result("(");
	result.append(*column_names.begin());
	for (ColumnNames::const_iterator column_name = column_names.begin() + 1; column_name != column_names.end(); column_name++) {
		result.append(", ");
		result.append(*column_name);
	}
	result.append(")");
	return result;
}

void report_schema_mismatch(const string &error, bool fatal) {
	if (fatal) {
		throw schema_mismatch(error);
	} else {
		cerr << error << endl;
	}
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
			report_schema_mismatch("Missing column " + from_column->name + " on table " + table.name, schema_mismatches_are_fatal);

		} else if (find_if(from_column, from_columns.end(), name_is<Column>(to_column->name)) == from_columns.end()) {
			report_schema_mismatch("Extra column " + to_column->name + " on table " + table.name, schema_mismatches_are_fatal);

		} else {
			report_schema_mismatch("Misordered column " + from_column->name + " on table " + table.name + ", should have " + to_column->name + " first", schema_mismatches_are_fatal);
		}
	}
	if (to_column != to_columns.end()) {
		report_schema_mismatch("Extra column " + to_column->name + " on table " + table.name, schema_mismatches_are_fatal);
	}
}

void check_primary_key_matches(const Table &table, const ColumnNames &from_primary_key_columns, const ColumnNames &to_primary_key_columns) {
	if (from_primary_key_columns.size() != to_primary_key_columns.size() ||
		!equal(from_primary_key_columns.begin(), from_primary_key_columns.end(), to_primary_key_columns.begin())) {
		report_schema_mismatch("Mismatching primary key " + columns_list(to_primary_key_columns) + " on table " + table.name + ", should have " + columns_list(from_primary_key_columns), schema_mismatches_are_fatal);
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
			report_schema_mismatch("Missing table " + from_table->name, schema_mismatches_are_fatal);

		} else if (to_table->name < from_table->name) {
			report_schema_mismatch("Extra table " + to_table->name, schema_mismatches_are_fatal);

		} else {
			check_table_match(*from_table, *to_table);
			++to_table;
		}
	}
	if (to_table != to_tables.end()) {
		report_schema_mismatch("Extra table " + to_table->name, schema_mismatches_are_fatal);
	}
}

void check_schema_match(Database &from_database, Database &to_database) {
	// currently we only pay attention to tables, but in the future we might support other schema items
	check_tables_match(from_database.tables, to_database.tables);
}

template<class T>
void sync_to(T &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	// tell the other end what protocol we speak
	Stream stream(STDIN_FILENO);
	cout << Command("protocol", PROTOCOL_VERSION_SUPPORTED);
	int protocol;
	stream.read_and_unpack(protocol);

	// get its schema
	cout << Command("schema");
	Database from_database;
	stream.read_and_unpack(from_database);

	// get our end's schema
	Database to_database(client.database_schema());

	// check they match
	check_schema_match(from_database, to_database);

	cout << Command("quit");
	close(STDOUT_FILENO);
	close(STDIN_FILENO);
}
