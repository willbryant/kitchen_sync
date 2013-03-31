#include <iostream>

#include "kitchen_sync.pb.h"

using namespace std;

template<class T>
void sync_from(T &client) {
	kitchen_sync::Database database(client.database_schema());
	for (int i = 0; i < database.table_size(); i++) {
		cout << database.table(i).name() << endl;
	}
}
