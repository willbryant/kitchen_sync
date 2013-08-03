#include <string>
#include <iostream>

#include "sync_from.h"
#include "sync_to.h"

template<class T>
int endpoint_main(int argc, char *argv[]) {
	if (argc < 7 || (argv[1] != string("from") && argv[1] != string("to"))) {
		cerr << "This program is a part of Kitchen Sync.  Instead of running this program directly, run 'ks'.\n";
		return 1;
	}

	bool from = (argv[1] == string("from"));

	try {
		T client(
			argv[2], /* database_host */
			argv[3], /* database_port */
			argv[4], /* database_name */
			argv[5], /* database_username */
			argv[6], /* database_password */
			from     /* readonly */);

		if (from) {
			sync_from(client);
		} else {
			sync_to(client);
		}

		cout.flush();
		close(STDOUT_FILENO);
	} catch (exception& e) {
		cerr << e.what() << endl;
		return 2;
	}
	return 0;
}
