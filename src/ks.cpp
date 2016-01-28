#include <iostream>
#include <vector>

#include "options.h"
#include "env.h"
#include "process.h"
#include "unidirectional_pipe.h"
#include "to_string.h"

using namespace std;

#define VERY_VERBOSE 2

const string this_program_name("ks");
const int to_descriptor_list_start = 8; // arbitrary; when the program starts we expect only 0, 1, and 2 to be in use, and we only need a couple of temporaries.  getdtablesize() is guaranteed to be at least 20, which is not much!

void be_christmassy() {
	cout << "            #" << endl
	     << "           ##o" << endl
	     << "          o#*##" << endl
	     << "         ####o##" << endl
	     << "        #*#o####o" << endl
	     << "       #####o##*##" << endl
	     << "      ####o###*####" << endl
	     << "           | |" << endl;
}

int main(int argc, char *argv[]) {
	try
	{
		string binary_path(Process::binary_path_only(argv[0], this_program_name));
		Options options;
		options.from_path = binary_path;
		if (!options.parse(argc, argv)) return 1;

		cout << "Kitchen Sync" << endl;

		string from_binary(options.from_path + "ks_" + options.from.protocol);
		string   to_binary(binary_path + "ks_" +   options.to.protocol);
		string  ssh_binary("/usr/bin/ssh");

		// currently we still pass all options to the 'from' endpoint on the command line, but we've started preparing it to support environment variables.
		// unfortunately when we transport program arguments over SSH it flattens them into a string and so empty arguments get lost; we work around by passing "-".
		if (options.from.port    .empty()) options.from.port     = "-";
		if (options.from.username.empty()) options.from.username = "-";
		if (options.from.password.empty()) options.from.password = "-";
		if (options.set_from_variables.empty()) options.set_from_variables = "-";

		const char *from_args[] = { ssh_binary.c_str(), "-C", "-c", "blowfish", options.via.c_str(),
									from_binary.c_str(), "from", options.from.host.c_str(), options.from.port.c_str(), options.from.database.c_str(), options.from.username.c_str(), options.from.password.c_str(), options.set_from_variables.c_str(), options.filters.c_str(), nullptr };
		const char **applicable_from_args = (options.via.empty() ? from_args + 5 : from_args);

		if (options.verbose >= VERY_VERBOSE) {
			cout << "from command:";
			for (const char **p = applicable_from_args; *p; p++) cout << ' ' << (**p ? *p : "''");
			cout << endl;
		}

		vector<pid_t> child_pids;
		for (int worker = 0; worker < options.workers; ++worker) {
			UnidirectionalPipe stdin_pipe;
			UnidirectionalPipe stdout_pipe;
			child_pids.push_back(Process::fork_and_exec(*applicable_from_args, applicable_from_args, stdin_pipe, stdout_pipe));
			stdout_pipe.dup_read_to(to_descriptor_list_start + worker);
			stdin_pipe.dup_write_to(to_descriptor_list_start + worker + options.workers);
		}

		// we pass all options to the 'to' end in the environment
		setenv("ENDPOINT_DATABASE_HOST", options.to.host);
		setenv("ENDPOINT_DATABASE_PORT", options.to.port);
		setenv("ENDPOINT_DATABASE_NAME", options.to.database);
		setenv("ENDPOINT_DATABASE_USERNAME", options.to.username);
		setenv("ENDPOINT_DATABASE_PASSWORD", options.to.password);
		setenv("ENDPOINT_SET_VARIABLES", options.set_to_variables);

		setenv("ENDPOINT_IGNORE_TABLES", options.ignore);
		setenv("ENDPOINT_ONLY_TABLES", options.only);
		setenv("ENDPOINT_WORKERS", to_string(options.workers));
		setenv("ENDPOINT_STARTFD", to_string(to_descriptor_list_start));
		setenv("ENDPOINT_VERBOSE", to_string(options.verbose));
		setenv("ENDPOINT_PROGRESS", options.progress ? "1" : "0", 1);
		setenv("ENDPOINT_SNAPSHOT", options.snapshot ? "1" : "0", 1);
		setenv("ENDPOINT_ALTER", options.alter ? "1" : "0", 1);
		setenv("ENDPOINT_COMMIT_LEVEL", to_string(options.commit_level));
		setenv("ENDPOINT_HASH_ALGORITHM", to_string(options.hash_algorithm));
		setenv("ENDPOINT_STRUCTURE_ONLY", to_string(options.structure_only));

		const char *to_args[] = { to_binary.c_str(), "to", nullptr };
		child_pids.push_back(Process::fork_and_exec(to_binary, to_args));

		for (int worker = 0; worker < options.workers; ++worker) {
			::close(to_descriptor_list_start + worker);
			::close(to_descriptor_list_start + worker + options.workers);
		}

		bool success = true;
		for (pid_t pid : child_pids) {
			success &= Process::wait_for_and_check(pid);
		}
		
		if (success) {
			cout << "Finished Kitchen Syncing." << endl;
			time_t t = time(NULL);
			if (options.verbose && localtime(&t)->tm_mon == 11) be_christmassy();
			return 0;
		} else {
			cout << "Kitchen Syncing failed." << endl;
			return 1;
		}
	} catch (const exception &e) {
		cerr << e.what() << endl;
	}
}
