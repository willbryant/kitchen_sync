#include <iostream>

#include "db_url.h"

using namespace std;
using namespace boost;
using namespace boost::program_options;

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

		DbUrl from = vm["from"].as<DbUrl>();
		cout << from.protocol << endl;
		cout << from.username << endl;
		cout << from.password << endl;
		cout << from.host << endl;
		cout << from.port << endl;
		cout << from.database << endl;

		DbUrl to = vm["to"].as<DbUrl>();
		cout << to.protocol << endl;
		cout << to.username << endl;
		cout << to.password << endl;
		cout << to.host << endl;
		cout << to.port << endl;
		cout << to.database << endl;

		cout << "Kitchen Sync\n";
		return 0;
	}
	catch(std::exception &e) {
		cerr << e.what() << endl;
	}
}
