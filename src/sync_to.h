#include <iostream>
#include <stdexcept>
#include <unistd.h>

#include "kitchen_sync.pb.h"

using namespace std;

template<class T>
void sync_to(T &client) {
	kitchen_sync::Database to_database(client.database_schema());
	kitchen_sync::Database from_database;

	if (!from_database.ParseFromFileDescriptor(STDIN_FILENO)) {
		throw runtime_error("Couldn't read database");
	}

	for (int t = 0; t < from_database.table_size(); t++) {
		const kitchen_sync::Table &table(from_database.table(t));
		cerr << table.name() << endl;
		for (int c = 0; c < table.column_size(); c++) {
			const kitchen_sync::Column &column(table.column(c));
			cerr << "\t" << column.name() << endl;
		}
	}

	cerr << test.name() << endl;

	close(STDIN_FILENO);
	close(STDOUT_FILENO);
}
