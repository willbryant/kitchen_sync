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

bool greet_remote_server(Options &options, vector<const char*>::const_iterator connection_args_begin, vector<const char*>::const_iterator connection_args_end, const string &from_binary) {
	string from_binary_cmd(from_binary.c_str() + string(" ") + string("do-nothing"));

	vector<const char*> ssh_greeting_args(connection_args_begin, connection_args_end);
	ssh_greeting_args.push_back(from_binary_cmd.c_str());
	ssh_greeting_args.push_back(nullptr);

	if (options.verbose >= VERY_VERBOSE) {
		cout << "ssh greeting command:";
		for (const char **p = ssh_greeting_args.data(); *p; p++) cout << ' ' << (**p ? *p : "''");
		cout << endl;
	}

	pid_t pid = Process::fork_and_exec(ssh_greeting_args.front(), ssh_greeting_args.data());

	if (!Process::wait_for_and_check(pid)) {
		cerr << "Couldn't start Kitchen Sync over SSH to " << options.via << ".  Please check that you can SSH to that server yourself, and that Kitchen Sync's binary can be found on that system at " << from_binary << "." << endl;
		return false;
	}

	return true;
}

int main(int argc, char *argv[]) {
	try
	{
		string binary_path(Process::binary_path_only(argv[0], this_program_name));
		Options options;
		options.from_path = binary_path;
		if (!options.parse(argc, argv)) return 1;

		cout << "Kitchen Sync" << endl;

		// the ks binary is ks_postgresql, but often urls will be of the form 'postgres://'
		if (options.from.protocol == "postgres") options.from.protocol = "postgresql";
		if (options.to.protocol == "postgres") options.to.protocol = "postgresql";

		string from_binary(options.from_path + "ks_" + options.from.protocol);
		string   to_binary(binary_path + "ks_" +   options.to.protocol);
		string  ssh_binary("/usr/bin/ssh");

		if (options.cipher.empty()) options.cipher = DEFAULT_CIPHER;

		vector<const char*> from_args;
		if (!options.via.empty()) {
			from_args.push_back(ssh_binary.c_str());
			from_args.push_back("-C");
			from_args.push_back("-c");
			from_args.push_back(options.cipher.c_str());
			if (!options.via_port.empty()) {
				from_args.push_back("-p");
				from_args.push_back(options.via_port.c_str());
			}
			from_args.push_back(options.via.c_str());
		}
		size_t connection_args(from_args.size());

		// pass all options to the 'from' env in the environment; we could use SSH to SetEnv these, but then the
		// server configuration would have to explicitly allow them. instead, we invoke the env program, which
		// works both locally and over SSH.
		string host_arg("ENDPOINT_DATABASE_HOST=" + options.from.host);
		string port_arg("ENDPOINT_DATABASE_PORT=" + options.from.port);
		string username_arg("ENDPOINT_DATABASE_USERNAME=" + options.from.username);
		string password_arg("ENDPOINT_DATABASE_PASSWORD=" + options.from.password);
		string database_arg("ENDPOINT_DATABASE_NAME=" + options.from.database);
		string schema_arg("ENDPOINT_DATABASE_SCHEMA=" + options.from.schema);
		string set_from_variables_arg("ENDPOINT_SET_VARIABLES=" + options.set_from_variables);

		from_args.push_back("env");
		from_args.push_back(host_arg.c_str());
		from_args.push_back(port_arg.c_str());
		from_args.push_back(username_arg.c_str());
		from_args.push_back(password_arg.c_str());
		from_args.push_back(database_arg.c_str());
		from_args.push_back(schema_arg.c_str());
		from_args.push_back(set_from_variables_arg.c_str());
		from_args.push_back(from_binary.c_str());
		from_args.push_back("from");
		from_args.push_back(nullptr);

		if (options.verbose >= VERY_VERBOSE) {
			cout << "from command:";
			for (const char *p : from_args) if (p) cout << ' ' << (*p ? p : "''");
			cout << endl;
		}

		if (!options.via.empty()) {
			if (!greet_remote_server(options, from_args.begin(), from_args.begin() + connection_args, from_binary)) {
				return 1;
			}
		}

		vector<pid_t> child_pids;
		for (int worker = 0; worker < options.workers; ++worker) {
			UnidirectionalPipe stdin_pipe;
			UnidirectionalPipe stdout_pipe;
			child_pids.push_back(Process::fork_and_exec(from_args.front(), from_args.data(), stdin_pipe, stdout_pipe));
			stdout_pipe.dup_read_to(to_descriptor_list_start + worker);
			stdin_pipe.dup_write_to(to_descriptor_list_start + worker + options.workers);
		}

		// and we pass all options to the 'to' end in the environment
		setenv("ENDPOINT_DATABASE_HOST", options.to.host);
		setenv("ENDPOINT_DATABASE_PORT", options.to.port);
		setenv("ENDPOINT_DATABASE_USERNAME", options.to.username);
		setenv("ENDPOINT_DATABASE_PASSWORD", options.to.password);
		setenv("ENDPOINT_DATABASE_NAME", options.to.database);
		setenv("ENDPOINT_DATABASE_SCHEMA", options.to.schema);
		setenv("ENDPOINT_SET_VARIABLES", options.set_to_variables);

		setenv("ENDPOINT_FILTERS_FILE", options.filters);
		setenv("ENDPOINT_IGNORE_TABLES", options.ignore);
		setenv("ENDPOINT_ONLY_TABLES", options.only);
		setenv("ENDPOINT_WORKERS", to_string(options.workers));
		setenv("ENDPOINT_STARTFD", to_string(to_descriptor_list_start));
		setenv("ENDPOINT_VERBOSE", to_string(options.verbose));
		setenv("ENDPOINT_PROGRESS", options.progress ? "1" : "0", 1);
		setenv("ENDPOINT_SNAPSHOT", options.snapshot ? "1" : "0", 1);
		setenv("ENDPOINT_ALTER", options.alter ? "1" : "0", 1);
		setenv("ENDPOINT_COMMIT_LEVEL", to_string(options.commit_level));
		setenv("ENDPOINT_HASH_ALGORITHM", to_string(static_cast<int>(options.hash_algorithm)));
		setenv("ENDPOINT_STRUCTURE_ONLY", to_string(options.structure_only));
		setenv("ENDPOINT_INSERT_ONLY", to_string(options.insert_only));

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
			if (options.verbose && isatty(STDOUT_FILENO) && localtime(&t)->tm_mon == 11) be_christmassy();
			return 0;
		} else {
			cout << "Kitchen Syncing failed." << endl;
			return 1;
		}
	} catch (const exception &e) {
		cerr << e.what() << endl;
	}
}
