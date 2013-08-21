#include <iostream>
#include <unistd.h>

#include "command.h"
#include "schema_serialization.h"
#include "row_serialization.h"

struct command_error: public runtime_error {
	command_error(const string &error): runtime_error(error) { }
};
#include "sql_functions.h"
template<class DatabaseClient>
void sync_from(DatabaseClient &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	Unpacker input(STDIN_FILENO);
	Packer<ostream> output(cout);
	Command command;

	// all conversations must start with a "protocol" command to establish the language to be used
	input >> command;
	if (command.name != "protocol") {
		throw command_error("Expected a protocol command before " + command.name);
	}

	// the usable protocol is the highest out of those supported by the two ends
	int protocol = min(PROTOCOL_VERSION_SUPPORTED, (int)command.argument<int64_t>(0));

	// tell the other end what version was selected
	output << protocol;
	cout.flush();

	while (true) {
		input >> command;

		if (command.name == "schema") {
			Database from_database(client.database_schema());
			output << from_database;

		} else if (command.name == "rows") {
			string     table_name(command.argument<string>(0));
			ColumnValues prev_key(command.argument<ColumnValues>(1));
			ColumnValues last_key(command.argument<ColumnValues>(2));
			const Table &table(client.table_by_name(table_name));
			RowPacker<typename DatabaseClient::RowType> row_packer(output);
			client.retrieve_rows(table, prev_key, last_key, row_packer);

		} else if (command.name == "hash") {
			string     table_name(command.argument<string>(0));
			ColumnValues prev_key(command.argument<ColumnValues>(1));
			ColumnValues last_key(command.argument<ColumnValues>(2));
			const Table &table(client.table_by_name(table_name));
			RowHasherAndPacker<typename DatabaseClient::RowType> row_hasher_and_packer(output);
			client.retrieve_rows(table, prev_key, last_key, row_hasher_and_packer);

		} else if (command.name == "quit") {
			break;

		} else {
			throw command_error("Unknown command " + command.name);
		}

		cout.flush();
	}
}
