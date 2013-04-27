#include <iostream>
#include <unistd.h>

#include "schema.h"

template<class T>
void sync_to(T &client) {
	Stream stream(STDIN_FILENO);
	Database to_database(client.database_schema());
	Database from_database;

	cout << Command("protocol", 1);
	cout << Command("schema");
	stream.read_and_unpack(from_database);

	for (Tables::const_iterator table = from_database.tables.begin(); table != from_database.tables.end(); ++table) {
		cerr << table->name << endl;
		for (Columns::const_iterator column = table->columns.begin(); column != table->columns.end(); ++column) {
			cerr << "\t" << column->name << endl;
		}
	}

	cout << Command("quit");
	close(STDOUT_FILENO);
	close(STDIN_FILENO);
}
