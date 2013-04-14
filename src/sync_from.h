#include <iostream>
#include <stdexcept>
#include <unistd.h>

#include "schema.h"

template<class T>
void sync_from(T &client) {
	Database from_database(client.database_schema());

	// if (!from_database.SerializeToOstream(&cout)) {
	// 	throw runtime_error("Couldn't serialize database");
	// }
	cout.flush();

	close(STDOUT_FILENO);
}
