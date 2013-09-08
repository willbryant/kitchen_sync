#include <iostream>

#include "command.h"
#include "schema_serialization.h"
#include "row_serialization.h"
#include "sync_algorithm.h"

struct command_error: public runtime_error {
	command_error(const string &error): runtime_error(error) { }
};

template <typename DatabaseClient, typename OutputStream>
void handle_rows_command(DatabaseClient &client, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, Packer<OutputStream> &output) {
	const Table &table(client.table_by_name(table_name));

	send_command(output, "rows", table_name, prev_key, last_key);
	RowPacker<typename DatabaseClient::RowType, OutputStream> row_packer(output);
	client.retrieve_rows(table, prev_key, last_key, row_packer);
}

template <typename DatabaseClient, typename OutputStream>
void handle_hash_command(DatabaseClient &client, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash, Packer<OutputStream> &output) {
	const Table &table(client.table_by_name(table_name));

	ColumnValues matched_up_to_key;
	size_t rows_to_hash = check_hash_and_choose_next_range(client, table, prev_key, last_key, hash, matched_up_to_key);

	// calculate our hash of the next rows_to_hash rows
	RowHasherAndLastKey<typename DatabaseClient::RowType> hasher_for_our_rows(table.primary_key_columns);
	if (rows_to_hash) {
		client.retrieve_rows(table, matched_up_to_key, rows_to_hash, hasher_for_our_rows);
	}

	if (hasher_for_our_rows.row_count == 0) {
		// rows don't match, and there's only one or no rows left, so send it straight across, as if they had given the rows command
		handle_rows_command(client, table_name, prev_key, last_key, output);
		
	} else {
		// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
		send_command(output, "hash", table_name, matched_up_to_key, hasher_for_our_rows.last_key, hasher_for_our_rows.finish());
	}
}

template<class DatabaseClient>
void sync_from(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	DatabaseClient client(database_host, database_port, database_name, database_username, database_password, true /* readonly */);
	Unpacker<istream> input(cin);
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
			handle_rows_command(client, table_name, prev_key, last_key, output);

		} else if (command.name == "hash") {
			string     table_name(command.argument<string>(0));
			ColumnValues prev_key(command.argument<ColumnValues>(1));
			ColumnValues last_key(command.argument<ColumnValues>(2));
			string           hash(command.argument<string>(3));
			handle_hash_command(client, table_name, prev_key, last_key, hash, output);

		} else if (command.name == "quit") {
			break;

		} else {
			throw command_error("Unknown command " + command.name);
		}

		cout.flush();
	}
}
