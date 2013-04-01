#include <iostream>
#include <stdexcept>
#include <unistd.h>

#include "kitchen_sync.pb.h"

using namespace std;

template<class T>
void sync_from(T &client) {
	kitchen_sync::Database from_database(client.database_schema());

	if (!from_database.SerializeToOstream(&cout)) {
		throw runtime_error("Couldn't serialize database");
	}
	cout.flush();

	close(STDOUT_FILENO);
}
