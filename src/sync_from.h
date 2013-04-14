#include <iostream>
#include <unistd.h>

#include "schema_serialization.h"

template<class T>
void sync_from(T &client) {
	Database from_database(client.database_schema());

	cout << from_database;
	cout.flush();

	close(STDOUT_FILENO);
}
