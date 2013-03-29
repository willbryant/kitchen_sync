#include <string>
#include <iostream>

#include "sync_from.h"
#include "sync_to.h"

using namespace std;

template<class T>
int endpoint_main(int argc, char *argv[]) {
	if (argc < 7 || (argv[1] != string("from") && argv[1] != string("to"))) {
		cerr << "This program is a part of Kitchen Sync.  Instead of running this program directly, run 'ks'.\n";
		return 1;
	}

	try {
		T client(argv[2], argv[3], argv[4], argv[5], argv[6]);

		if (argv[1] == string("from")) {
			sync_from(client);
		} else {
			sync_to(client);
		}
	} catch (exception e) {
		cerr << e.what() << "\n";
		return 2;
	}
	return 0;
}
