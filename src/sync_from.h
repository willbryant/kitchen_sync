#include <iostream>
#include <unistd.h>

#include "command.h"
#include "schema_serialization.h"

template<class T>
void sync_from(T &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	Stream stream(STDIN_FILENO);
	Command command;
	int protocol = 0;

	while (true) {
		stream.read_and_unpack(command);

		if (command.name == "protocol") {
			protocol = min(PROTOCOL_VERSION_SUPPORTED, command.arguments[0].as<typeof(protocol)>());
			cout << protocol;
			cout.flush();

		} else if (command.name == "schema") {
			Database from_database(client.database_schema());
			cout << from_database;
			cout.flush();
			break;

		} else if (command.name == "quit") {
			break;

		} else {
			cerr << "Unknown command: " << command.name << endl;
			break;
		}
	}

	close(STDOUT_FILENO);
	close(STDIN_FILENO);
}
