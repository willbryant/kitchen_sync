#include <iostream>
#include <vector>

#include "options.h"
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
		Options options;
		if (!options.parse(argc, argv)) return 1;

		cout << "Kitchen Sync" << endl;

		string self_binary(argv[0]);
		string from_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" + options.from.protocol));
		string   to_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" +   options.to.protocol));
		string  ssh_binary("/usr/bin/ssh");
		string workers_str(to_string(options.workers));
		string verbose_str(to_string(options.verbose));
		string  commit_str(to_string(options.commit_level));
		string startfd_str(to_string(to_descriptor_list_start));

		// unfortunately when we transport program arguments over SSH it flattens them into a string and so empty arguments get lost; we work around by using "-"
		if (options.from.port    .empty()) options.from.port     = "-";
		if (options.from.username.empty()) options.from.username = "-";
		if (options.from.password.empty()) options.from.password = "-";
		if (options.to  .port    .empty()) options.to  .port     = "-";
		if (options.to  .username.empty()) options.to  .username = "-";
		if (options.to  .password.empty()) options.to  .password = "-";
		if (options.set_from_variables.empty()) options.set_from_variables = "-";
		if (options.set_to_variables.empty())   options.set_to_variables = "-";

		const char *from_args[] = { ssh_binary.c_str(), "-C", "-c", "blowfish", options.via.c_str(),
									from_binary.c_str(), "from", options.from.host.c_str(), options.from.port.c_str(), options.from.database.c_str(), options.from.username.c_str(), options.from.password.c_str(), options.set_from_variables.c_str(), options.filters.c_str(), nullptr };
		const char *  to_args[] = {   to_binary.c_str(),   "to",   options.to.host.c_str(),   options.to.port.c_str(),   options.to.database.c_str(),   options.to.username.c_str(),   options.to.password.c_str(), options.set_to_variables.c_str(), options.ignore.c_str(), options.only.c_str(), workers_str.c_str(), startfd_str.c_str(), verbose_str.c_str(), options.snapshot ? "1" : "0", options.alter ? "1" : "0", commit_str.c_str(), nullptr };
		const char **applicable_from_args = (options.via.empty() ? from_args + 5 : from_args);

		if (options.verbose >= VERY_VERBOSE) {
			cout << "from command:";
			for (const char **p = from_args; *p; p++) cout << ' ' << (**p ? *p : "''");
			cout << endl;

			cout << "to command:";
			for (const char **p = to_args; *p; p++) cout << ' ' << (**p ? *p : "''");
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
