#include <iostream>
#include <unistd.h>

#include "kitchen_sync.pb.h"

using namespace std;

template<class T>
void sync_from(T &client) {
	kitchen_sync::Database database(client.database_schema());
	for (int t = 0; t < database.table_size(); t++) {
		const kitchen_sync::Table &table(database.table(t));
		cout << table.name() << endl;
		for (int c = 0; c < table.column_size(); c++) {
			const kitchen_sync::Column &column(table.column(c));
			cout << "\t" << column.name() << endl;
		}
	}

	close(STDOUT_FILENO);
}
