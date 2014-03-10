#include <string>
#include <iostream>

#include "sync_from.h"
#include "sync_to.h"

set<string> split_list(const string &str) {
	set<string> result;
	boost::split(result, str, boost::is_any_of(", "));
	if (result.size() == 1 && *result.begin() == "") result.erase("");
	return result;
}

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
			const char *filters_file = argc > 7 ? argv[7] : nullptr;
			sync_from<DatabaseClient>(database_host, database_port, database_name, database_username, database_password, filters_file, STDIN_FILENO, STDOUT_FILENO);
		} else {
			set <string> ignore(split_list(argc > 7 ? argv[7] : ""));
			set <string> only(split_list(argc > 8 ? argv[8] : ""));
			int workers = argc > 9 ? atoi(argv[9]) : 1;
			int startfd = argc > 10 ? atoi(argv[10]) : STDIN_FILENO;
			int verbose = argc > 11 ? atoi(argv[11]) : false;
			bool snapshot = argc > 12 ? atoi(argv[12]) : false;
			bool partial = argc > 13 ? atoi(argv[13]) : false;
			bool rollback_after = argc > 14 ? atoi(argv[14]) : false;
			sync_to<DatabaseClient>(workers, startfd, database_host, database_port, database_name, database_username, database_password, ignore, only, verbose, snapshot, partial, rollback_after);
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
