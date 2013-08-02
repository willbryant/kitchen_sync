#include <iostream>
#include <unistd.h>

#include "command.h"
#include "schema_serialization.h"

template<class T>
void sync_from(T &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	Stream stream(STDIN_FILENO);
	msgpack::packer<ostream> packer(cout); // we could overload for ostreams automatically, but then any primitive types send to cout would get printed without encoding
	Command command;
	int protocol = 0;

	while (true) {
		stream.read_and_unpack(command);

		if (command.name == "protocol") {
			protocol = min(PROTOCOL_VERSION_SUPPORTED, command.arguments[0].as<typeof(protocol)>());
			packer << protocol;

		} else if (command.name == "schema") {
			Database from_database(client.database_schema());
			packer << from_database;

		} else if (command.name == "quit") {
			break;

		} else {
			cerr << "Unknown command: " << command.name << endl;
			break;
		}

		cout.flush();
	}

	close(STDOUT_FILENO);
	close(STDIN_FILENO);
}
