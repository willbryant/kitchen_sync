#include <iostream>
#include <stdexcept>
#include <unistd.h>

#include "schema.h"

template<class T>
void sync_to(T &client) {
	Database to_database(client.database_schema());
	Database from_database;

	// if (!from_database.ParseFromFileDescriptor(STDIN_FILENO)) {
	// 	throw runtime_error("Couldn't read database");
	// }

	for (Tables::const_iterator table = from_database.tables.begin(); table != from_database.tables.end(); ++table) {
		cerr << table->name << endl;
		for (Columns::const_iterator column = table->columns.begin(); column != table->columns.end(); ++column) {
			cerr << "\t" << column->name << endl;
		}
	}

	close(STDIN_FILENO);
	close(STDOUT_FILENO);
}
