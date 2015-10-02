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
	if (argc < 8 || (argv[1] != string("from") && argv[1] != string("to"))) {
		cerr << "This program is a part of Kitchen Sync.  Instead of running this program directly, run 'ks'.\n";
		return 1;
	}

	bool from = (argv[1] == string("from"));

	try {
		// the first set of arguments are the same for both endpoints
		string database_host(argv[2]);
		string database_port(argv[3]);
		string database_name(argv[4]);
		string database_username(argv[5]);
		string database_password(argv[6]);
		string set_variables(argv[7]);

		// unfortunately when we transport program arguments over SSH it flattens them into a string and so empty arguments get lost; we work around by using "-"
		if (database_port     == string("-")) database_port     = "";
		if (database_username == string("-")) database_username = "";
		if (database_password == string("-")) database_password = "";
		if (set_variables     == string("-")) set_variables     = "";
		
		// the remaining arguments are different for the two arguments
		if (from) {
			const string filters_file = argc > 8 ? argv[8] : "";
			char *status_area = argv[1];
			char *last_arg = argv[argc - 1];
			char *end_of_last_arg = last_arg + strlen(last_arg);
			size_t status_size = end_of_last_arg - status_area;
			HashAlgorithm hash_algorithm(HashAlgorithm::md5); // until advised otherwise by the 'to' end
			sync_from<DatabaseClient>(database_host, database_port, database_name, database_username, database_password, set_variables, filters_file, hash_algorithm, STDIN_FILENO, STDOUT_FILENO, status_area, status_size);
		} else {
			set <string> ignore(split_list(argc > 8 ? argv[8] : ""));
			set <string> only(split_list(argc > 9 ? argv[9] : ""));
			int workers = argc > 10 ? atoi(argv[10]) : 1;
			int startfd = argc > 11 ? atoi(argv[11]) : STDIN_FILENO;
			int verbose = argc > 12 ? atoi(argv[12]) : 0;
			bool snapshot = argc > 13 ? atoi(argv[13]) : false;
			bool alter = argc > 14 ? atoi(argv[14]) : true;
			CommitLevel commit_level = argc > 15 ? CommitLevel(atoi(argv[15])) : CommitLevel::success;
			HashAlgorithm hash_algorithm = argc > 16 ? HashAlgorithm(atoi(argv[16])) : HashAlgorithm::md5;
			sync_to<DatabaseClient>(workers, startfd, database_host, database_port, database_name, database_username, database_password, set_variables, ignore, only, verbose, snapshot, alter, commit_level, hash_algorithm);
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
