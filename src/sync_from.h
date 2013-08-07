#include <iostream>
#include <unistd.h>

#include "command.h"
#include "schema_serialization.h"
#include "row_serialization.h"

struct command_error: public runtime_error {
	command_error(const string &error): runtime_error(error) { }
};

template<class DatabaseClient>
void sync_from(DatabaseClient &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	Stream stream(STDIN_FILENO);
	msgpack::packer<ostream> packer(cout); // we could overload for ostreams automatically, but then any primitive types send to cout would get printed without encoding
	Command command;

	// all conversations must start with a "protocol" command to establish the language to be used
	stream >> command;
	if (command.name != "protocol") {
		throw command_error("Expected a protocol command before " + command.name);
	}

	// the usable protocol is the highest out of those supported by the two ends
	int protocol = min(PROTOCOL_VERSION_SUPPORTED, command.arguments[0].as<typeof(protocol)>());

	// tell the other end what version was selected
	packer << protocol;
	cout.flush();

	while (true) {
		stream >> command;

		if (command.name == "schema") {
			Database from_database(client.database_schema());
			packer << from_database;

		} else if (command.name == "rows") {
			string  table_name(command.arguments[0].as<string>());
			RowValues prev_key(command.arguments[1].as<RowValues>());
			RowValues last_key(command.arguments[2].as<RowValues>());
			const Table &table(client.table_by_name(table_name));
			RowPacker<typename DatabaseClient::RowType> row_packer(packer);
			client.retrieve_rows(table, prev_key, last_key, row_packer);

		} else if (command.name == "hash") {
			string  table_name(command.arguments[0].as<string>());
			RowValues prev_key(command.arguments[1].as<RowValues>());
			uint64_t row_count(command.arguments[2].as<uint64_t>());
			const Table &table(client.table_by_name(table_name));
			RowHasherAndPacker<typename DatabaseClient::RowType> row_hasher_and_packer(packer, table.primary_key_columns);
			client.retrieve_rows(table, prev_key, row_count, row_hasher_and_packer);

		} else if (command.name == "quit") {
			break;

		} else {
			throw command_error("Unknown command " + command.name);
		}

		cout.flush();
	}
}
