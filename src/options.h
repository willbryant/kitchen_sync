#ifndef OPTIONS_H
#define OPTIONS_H

#include <getopt.h>
#include <cstring>
#include <stdexcept>
#include "commit_level.h"
#include "hash_algorithm.h"
#include "db_url.h"

struct Options {
	inline Options(): workers(1), verbose(0), snapshot(true), alter(false), commit_level(CommitLevel::success), hash_algorithm(HashAlgorithm::md5) {}

	void help() {
		cerr <<
			"Allowed options:\n"
			"  --from url                 The URL of the database to copy data from.  \n"
			"                             Required.\n"
			"\n"
			"  --to url                   The URL of the database to copy data to.  \n"
			"                             Required.\n"
			"\n"
			"  --via host                 The server to run the 'from' end on (instead of \n"
			"                             accessing the database server directly).  \n"
			"                             Optional; useful whenever the network link to the \n"
			"                             'from' database server is a bottleneck, which will\n"
			"                             definitely be the case if it is at another \n"
			"                             datacentre, and may even be the case on local LANs\n"
			"                             if you have very fast disks.\n"
			"\n"
			"  --workers num              The number of concurrent workers to use at each end.\n"
			"                             Defaults to 1.\n"
			"\n"
			"  --ignore tables            Comma-separated list of tables to ignore.\n"
			"\n"
			"  --only tables              Comma-separated list of tables to process (causing \n"
			"                             all others to be ignored).\n"
			"\n"
			"  --filters file.yml         YAML file to read table/column filtering \n"
			"                             information from (if using --via, this is read at \n"
			"                             the 'from' end).\n"
			"\n"
			"  --set-from-variables var   SET variables to apply at the 'from' end (eg. \n"
			"                             --set-from-variables=\"sql_log_off=0, \n"
			"                             time_zone='UTC'\")\n"
			"\n"
			"  --set-to-variables var     SET variables to apply at the 'to' end (eg. \n"
			"                             --set-to-variables=\"sql_log_bin=0\")\n"
			"\n"
			"  --without-snapshot-export  Don't attempt to export/use a consistent snapshot \n"
			"                             across multiple workers.\n"
			"                             Snapshots are normally a good thing as they give\n"
			"                             you consistent copies of the data, but on PostgreSQL\n"
			"                             they require version 9.2+, and on MySQL they use\n"
			"                             FLUSH TABLES WITH READ LOCK which both requires the\n"
			"                             RELOAD privilege and may also have an impact on\n"
			"                             other connections (as it blocks the server till all\n"
			"                             open transactions commit).\n"
			"                             Turning on this option avoids these problems, but \n"
			"                             you may get an inconsistent copy if transactions \n"
			"                             commit in between the individual worker \n"
			"                             transactions starting.  It's still safe to use if\n"
			"                             you know that there will be no other transactions\n"
			"                             committed while the workers are starting (changes\n"
			"                             after that point won't be a problem anyway).\n"
			"\n"
			"  --commit                   When to commit the write transactions.  May be:\n"
			"                               'never' (roll back after syncing);\n"
			"                               'success' (commit if all workers complete normally);\n"
			"                               'tables' (commit after finishing each table); or\n"
			"                               'often' (periodically commit work in progress)\n"
			"                             The default is 'success'.  'never' is only useful\n"
			"                             for benchmarking and testing.  'often' is best if\n"
			"                             you are happy to run Kitchen Sync again if it fails.\n"
			"\n"
			"  --alter                    Alter the database schema if it doesn't match.\n"
			"                             (If not given, the schema will still be checked,\n"
			"                             and if it doesn't match the statements --alter\n"
			"                             would use are printed as suggestions.)"
			"\n"
			"  --hash arg                 Use the specified checksum algorithm.  The default\n"
			"                             is MD5.  You can downgrade to XXH64 if you are more\n"
			"                             interested in performance than data integrity.\n"
			"                             This is not considered appropriate for production\n"
			"                             use, but may be useful for dev/test machines.\n"
			"\n"
			"  --from-path                Directory in which to find the Kitchen Sync binaries\n"
			"                             on the source end.  Normally you should not need this\n"
			"                             but if you use the --via option and the binaries are\n"
			"                             neither installed in the same place on your local and\n"
			"                             remote systems, nor are in the PATH on both systems,\n"
			"                             you may need to use this option.\n"
			"\n"
			"  --verbose                  Log more information as the program works.\n"
			"\n"
			"  --debug                    Log debugging information as the program works.\n";
		cerr << endl;
	}

	inline bool parse(int argc, char *argv[]) {
		try {
			int urls = 0;
			while (true) {
				static struct option longopts[] = {
					{ "from",						required_argument,	NULL,	'f' },
					{ "to",							required_argument,	NULL,	't' },
					{ "via",						required_argument,	NULL,	'v' },
					{ "from-path",					required_argument,	NULL,	'P' },
					{ "workers",					required_argument,	NULL,	'w' },
					{ "ignore",						required_argument,	NULL,	'i' },
					{ "only",						required_argument,	NULL,	'o' },
					{ "filters",					required_argument,	NULL,	'l' },
					{ "set-from-variables",			required_argument,	NULL,	'F' },
					{ "set-to-variables",			required_argument,	NULL,	'T' },
					{ "without-snapshot-export",	no_argument,		NULL,	'W' },
					{ "commit",						required_argument,	NULL,	'c' },
					{ "partial",					no_argument,		NULL,	'p' }, // deprecated - use '--commit often' instead
					{ "rollback-after",				no_argument,		NULL,	'r' }, // deprecated - use '--commit never', which is equivalent
					{ "alter",						no_argument,		NULL,	'a' },
					{ "hash",					    required_argument,	NULL,	'h' },
					{ "verbose",					no_argument,		NULL,	'V' },
					{ "debug",						no_argument,		NULL,	'd' },
					{ NULL,							0,					NULL,	0 },
				};

				char ch = getopt_long_only(argc, argv, "", longopts, NULL);
				if (ch == -1) break;

				switch (ch) {
					case 'f':
						from = DbUrl(optarg);
						urls++;
						break;

					case 't':
						to = DbUrl(optarg);
						urls++;
						break;

					case 'v':
						via = optarg;
						break;

					case 'P':
						from_path = optarg;
						if (from_path.size() > 0 && from_path[from_path.size() - 1] != '/') {
							from_path += '/';
						}
						break;

					case 'w':
						workers = atoi(optarg);
						if (!workers) throw invalid_argument("Must have at least one worker");
						break;

					case 'i':
						ignore = optarg;
						break;

					case 'o':
						only = optarg;
						break;

					case 'l':
						filters = optarg;
						break;

					case 'F':
						set_from_variables = optarg;
						break;

					case 'T':
						set_to_variables = optarg;
						break;

					case 'W':
						snapshot = false;
						break;

					case 'c':
						if (!strcmp(optarg, "never")) {
							commit_level = CommitLevel::never;
						} else if (!strcmp(optarg, "success")) {
							commit_level = CommitLevel::success;
						} else if (!strcmp(optarg, "tables")) {
							commit_level = CommitLevel::tables;
						} else if (!strcmp(optarg, "often")) {
							commit_level = CommitLevel::often;
						} else {
							throw invalid_argument("Unknown commit level: " + string(optarg));
						}
						break;

					case 'p':
						commit_level = CommitLevel::always;
						break;

					case 'r':
						commit_level = CommitLevel::never;
						break;

					case 'a':
						alter = true;
						break;

					case 'h':
						if (!strcmp(optarg, "MD5")) {
							hash_algorithm = HashAlgorithm::md5;
						} else if (!strcmp(optarg, "XXH64")) {
							hash_algorithm = HashAlgorithm::xxh64;
						} else {
							throw invalid_argument("Unknown hash algorithm: " + string(optarg));
						}

					case 'V':
						verbose = 1;
						break;

					case 'd':
						verbose = 2;
						break;

					case '?':
						help();
						return false;
				}
			}

			if (urls < 2) {
				help();
				return false;
			}

			return true;
		} catch (const exception &e) {
			cerr << e.what() << endl;
			help();
			return false;
		}
	}

	DbUrl from, to;
	string via;
	string from_path;
	string filters;
	string set_from_variables;
	string set_to_variables;
	int workers;
	int verbose;
	bool snapshot;
	bool alter;
	CommitLevel commit_level;
	HashAlgorithm hash_algorithm;
	string ignore, only;
};

#endif
