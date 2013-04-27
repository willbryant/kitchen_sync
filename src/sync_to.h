#include <iostream>
#include <unistd.h>

#include "schema.h"

const bool schema_mismatches_are_fatal = true; // will be an option in the future

struct schema_mismatch : public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

void report_schema_mismatch(const string &error, bool fatal) {
	if (fatal) {
		throw schema_mismatch(error);
	} else {
		cerr << error << endl;
	}
}

void check_column_match(const Column &from_column, const Column &to_column) {
	// in the future we will check the type too
}

void check_columns_match(const Columns &from_columns, const Columns &to_columns) {
	Columns::const_iterator to_column = to_columns.begin();
	for (Columns::const_iterator from_column = from_columns.begin(); from_column != from_columns.end(); ++from_column) {
		if (to_column != to_columns.end() && to_column->name == from_column->name) {
			check_column_match(*from_column, *to_column);
			++to_column;

		} else if (to_column == to_columns.end() || to_column->name != from_column->name) {
			report_schema_mismatch("Missing column " + from_column->name, schema_mismatches_are_fatal);

		} else {
			report_schema_mismatch("Extra column " + from_column->name, schema_mismatches_are_fatal);
		}
	}
}

void check_table_match(const Table &from_table, const Table &to_table) {
	// currently we only pay attention to tables, but in the future we will check indexes too
	check_columns_match(from_table.columns, to_table.columns);
}

void check_tables_match(Tables &from_tables, Tables &to_tables) {
	// databases typically return the tables in sorted order, but our algorithm requires it, so we quickly enforce it here
	sort(from_tables.begin(), from_tables.end());
	sort(  to_tables.begin(),   to_tables.end());

	Tables::const_iterator to_table = to_tables.begin();
	for (Tables::const_iterator from_table = from_tables.begin(); from_table != from_tables.end(); ++from_table) {
		if (to_table != to_tables.end() && to_table->name == from_table->name) {
			check_table_match(*from_table, *to_table);
			++to_table;

		} else if (to_table == to_tables.end() || to_table->name != from_table->name) {
			report_schema_mismatch("Missing table " + from_table->name, schema_mismatches_are_fatal);

		} else {
			report_schema_mismatch("Extra table " + from_table->name, schema_mismatches_are_fatal);
		}
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
