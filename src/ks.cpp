#include <iostream>

#include "db_url.h"
#include "process.h"

using namespace std;
using namespace boost::program_options;

const string this_program_name("ks");

int help(options_description desc) {
    cout << desc << "\n";
    return 1;
}

int main(int argc, char *argv[]) {
	try
	{
		options_description desc("Allowed options");
		desc.add_options()
			("from", value<DbUrl>()->required(), "The URL of the database to copy data from.  Required.\n")
			("to",   value<DbUrl>()->required(), "The URL of the database to copy data to.  Required.");
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

		cout << "Kitchen Sync" << endl;

		DbUrl from = vm["from"].as<DbUrl>();
		DbUrl to   = vm["to"  ].as<DbUrl>();

		string self_binary(argv[0]);
		string from_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" + from.protocol));
		string   to_binary(Process::related_binary_path(self_binary, this_program_name, "ks_" + from.protocol));

		const char *from_args[] = { from_binary.c_str(), "from", from.host.c_str(), from.port.c_str(), from.database.c_str(), from.username.c_str(), from.password.c_str(), NULL };
		const char *  to_args[] = {   to_binary.c_str(),   "to",   to.host.c_str(),   to.port.c_str(),   to.database.c_str(),   to.username.c_str(),   to.password.c_str(), NULL };

		pid_t from_pid, to_pid;
		Process::fork_and_exec_pair(from_binary, to_binary, from_args, to_args, &from_pid, &to_pid);

		if (Process::wait_for_and_check(from_pid) && Process::wait_for_and_check(to_pid)) {
			cout << "Finished Kitchen Syncing." << endl;
			return 0;
		} else {
			cout << "Kitchen Syncing failed." << endl;
			return 1;
		}
	} catch (exception &e) {
		cerr << e.what() << endl;
	}
}
