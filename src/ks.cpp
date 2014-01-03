#include <iostream>

#include "db_url.h"
#include "process.h"
#include "unidirectional_pipe.h"
#include "to_string.h"

using namespace std;
using namespace boost::program_options;

const string this_program_name("ks");
const int to_descriptor_list_start = 1000; // arbitrary

int help(options_description desc) {
    cout << desc << "\n";
    return 1;
}

int main(int argc, char *argv[]) {
	try
	{
		options_description desc("Allowed options");
		int workers;
		bool verbose = false;
		desc.add_options()
			("from",    value<DbUrl>()->required(),             "The URL of the database to copy data from.  Required.\n")
			("to",      value<DbUrl>()->required(),             "The URL of the database to copy data to.  Required.\n")
			("workers", value<int>(&workers)->default_value(1), "The number of concurrent workers to use at each end.\n")
			("verbose", "Log more information as the program works.\n");
		variables_map vm;

		try {
			store(parse_command_line(argc, argv, desc), vm);
			notify(vm);
		} catch(required_option) {
			return help(desc);
		} catch(validation_error &e) {
			cerr << e.what() << endl;
			return help(desc);
		}

		if (vm.count("help")) {
			return help(desc);
		}

		if (vm.count("verbose")) {
			verbose = true;
		}

		cout << "Kitchen Sync" << endl;

		DbUrl from = vm["from"].as<DbUrl>();
		DbUrl to   = vm["to"  ].as<DbUrl>();

		string self_binary(argv[0]);
		string from_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" + from.protocol));
		string   to_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" + from.protocol));
		string workers_str(to_string(workers));
		string startfd_str(to_string(to_descriptor_list_start));

		const char *from_args[] = { from_binary.c_str(), "from", from.host.c_str(), from.port.c_str(), from.database.c_str(), from.username.c_str(), from.password.c_str(), NULL };
		const char *  to_args[] = {   to_binary.c_str(),   "to",   to.host.c_str(),   to.port.c_str(),   to.database.c_str(),   to.username.c_str(),   to.password.c_str(), workers_str.c_str(), startfd_str.c_str(), verbose ? "1" : "0", NULL };

		vector<pid_t> child_pids;
		for (int worker = 0; worker < workers; ++worker) {
			UnidirectionalPipe stdin_pipe;
			UnidirectionalPipe stdout_pipe;
			child_pids.push_back(Process::fork_and_exec(from_binary, from_args, stdin_pipe, stdout_pipe));
			stdout_pipe.dup_read_to(to_descriptor_list_start + worker);
			stdin_pipe.dup_write_to(to_descriptor_list_start + worker + workers);
		}

		child_pids.push_back(Process::fork_and_exec(to_binary, to_args));

		bool success = true;
		for (vector<pid_t>::const_iterator ppid = child_pids.begin(); ppid != child_pids.end(); ++ppid) {
			success &= Process::wait_for_and_check(*ppid);
		}
		
		if (success) {
			cout << "Finished Kitchen Syncing." << endl;
			return 0;
		} else {
			cout << "Kitchen Syncing failed." << endl;
			return 1;
		}
	} catch (const exception &e) {
		cerr << e.what() << endl;
	}
}
