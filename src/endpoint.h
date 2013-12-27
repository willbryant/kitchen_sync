#include <string>
#include <iostream>

#include "sync_from.h"
#include "sync_to.h"

template<class DatabaseClient>
int endpoint_main(int argc, char *argv[]) {
	if (argc < 7 || (argv[1] != string("from") && argv[1] != string("to"))) {
		cerr << "This program is a part of Kitchen Sync.  Instead of running this program directly, run 'ks'.\n";
		return 1;
	}

	bool from = (argv[1] == string("from"));

	try {
		const char *database_host(argv[2]);
		const char *database_port(argv[3]);
		const char *database_name(argv[4]);
		const char *database_username(argv[5]);
		const char *database_password(argv[6]);
		
		if (from) {
			sync_from<DatabaseClient>(database_host, database_port, database_name, database_username, database_password);
		} else {
			sync_to<DatabaseClient>(database_host, database_port, database_name, database_username, database_password);
		}

		close(STDOUT_FILENO);
	} catch (exception& e) {
		cerr << e.what() << endl;
		return 2;
	}
	return 0;
}
