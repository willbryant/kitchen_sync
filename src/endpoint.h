#include <string>
#include <iostream>

#include "env.h"
#include "sync_from.h"
#include "sync_to.h"

set<string> split_list(const string &str, const string &delimiters = ", ") {
	set<string> result;
	string::size_type beg_pos, end_pos = 0;
	while ((beg_pos = str.find_first_not_of(delimiters, end_pos)) != string::npos) {
		end_pos = str.find_first_of(delimiters, beg_pos);
		if (end_pos == string::npos) end_pos = str.length();
		result.insert(str.substr(beg_pos, end_pos - beg_pos));
	}
	return result;
}

template<class DatabaseClient>
int endpoint_main(int argc, char *argv[]) {
	if (argc > 1 && argv[1] == string("do-nothing")) {
		return 0;
	}

	if (argc < 2 || (argv[1] != string("from") && argv[1] != string("to"))) {
		cerr << "This program is a part of Kitchen Sync.  Instead of running this program directly, run 'ks'.\n";
		return 1;
	}

	bool from = (argv[1] == string("from"));

	try {
		// // the first set of arguments are the same for both endpoints
		string database_host(getenv_default("ENDPOINT_DATABASE_HOST", ""));
		string database_port(getenv_default("ENDPOINT_DATABASE_PORT", ""));
		string database_name(getenv_default("ENDPOINT_DATABASE_NAME", ""));
		string database_username(getenv_default("ENDPOINT_DATABASE_USERNAME", ""));
		string database_password(getenv_default("ENDPOINT_DATABASE_PASSWORD", ""));
		string set_variables(getenv_default("ENDPOINT_SET_VARIABLES", ""));

		if (from) {
			// for backwards compatibility, we currently send and support positional arguments to the 'from'
			// endpoint (which may be on another system if the --via option is used), but we also accept
			// environment variables (which we intend to use in the future).
			if (argc > 7) {
				database_host = argv[2];
				database_port = argv[3];
				database_name = argv[4];
				database_username = argv[5];
				database_password = argv[6];
				set_variables = argv[7];

				// unfortunately when we transport program arguments over SSH it flattens them into a string and so empty arguments get lost; we work around by using "-"
				if (database_port     == string("-")) database_port     = "";
				if (database_username == string("-")) database_username = "";
				if (database_password == string("-")) database_password = "";
				if (set_variables     == string("-")) set_variables     = "";
			}

			char *status_area = argv[1];
			char *last_arg = argv[argc - 1];
			char *end_of_last_arg = last_arg + strlen(last_arg);
			size_t status_size = end_of_last_arg - status_area;

			sync_from<DatabaseClient>(database_host, database_port, database_name, database_username, database_password, set_variables, STDIN_FILENO, STDOUT_FILENO, status_area, status_size);
		} else {
			// the 'to' endpoint has already been converted to pass options using environment variables -
			// since it's always on the same system as the ks command, it doesn't need legacy support.
			// the only time these values won't be set is running the test suite.
			string filters_file(getenv_default("ENDPOINT_FILTERS_FILE", ""));
			set <string> ignore(split_list(getenv_default("ENDPOINT_IGNORE_TABLES", "")));
			set <string> only(split_list(getenv_default("ENDPOINT_ONLY_TABLES", "")));
			int workers = getenv_default("ENDPOINT_WORKERS", 1);
			int startfd = getenv_default("ENDPOINT_STARTFD", STDIN_FILENO);
			int verbose = getenv_default("ENDPOINT_VERBOSE", 0);
			bool progress = getenv_default("ENDPOINT_PROGRESS", false);
			bool snapshot = getenv_default("ENDPOINT_SNAPSHOT", false);
			bool alter = getenv_default("ENDPOINT_ALTER", true);
			CommitLevel commit_level = CommitLevel(getenv_default("ENDPOINT_COMMIT_LEVEL", CommitLevel::success));
			HashAlgorithm hash_algorithm = HashAlgorithm(getenv_default("ENDPOINT_HASH_ALGORITHM", DEFAULT_HASH_ALGORITHM));
			size_t target_minimum_block_size = getenv_default("ENDPOINT_TARGET_MINIMUM_BLOCK_SIZE", DEFAULT_MINIMUM_BLOCK_SIZE); // only set by tests
			size_t target_maximum_block_size = getenv_default("ENDPOINT_TARGET_MAXIMUM_BLOCK_SIZE", DEFAULT_MAXIMUM_BLOCK_SIZE); // not currently used except manual testing
			bool structure_only = getenv_default("ENDPOINT_STRUCTURE_ONLY", false);

			sync_to<DatabaseClient>(workers, startfd, database_host, database_port, database_name, database_username, database_password, set_variables, filters_file, ignore, only, verbose, progress, snapshot, alter, commit_level, hash_algorithm, target_minimum_block_size, target_maximum_block_size, structure_only);
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
