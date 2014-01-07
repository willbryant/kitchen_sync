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

		if (database_port     == string("-")) database_port     = "";
		if (database_username == string("-")) database_username = "";
		if (database_password == string("-")) database_password = "";
		
		if (from) {
			sync_from<DatabaseClient>(database_host, database_port, database_name, database_username, database_password, STDIN_FILENO, STDOUT_FILENO);
		} else {
			int workers = argc > 7 ? atoi(argv[7]) : 1;
			int startfd = argc > 8 ? atoi(argv[8]) : STDIN_FILENO;
			bool verbose = argc > 9 ? atoi(argv[9]) : false;
			bool partial = argc > 10 ? atoi(argv[10]) : false;
			sync_to<DatabaseClient>(database_host, database_port, database_name, database_username, database_password, workers, startfd, verbose, partial);
		}
	} catch (const sync_error& e) {
		// the worker thread has already output the error to cerr
		return 2;
	} catch (const exception& e) {
		cerr << e.what() << endl;
		return 2;
	}
	return 0;
}
