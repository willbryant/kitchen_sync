#include "command.h"
#include "schema_serialization.h"
#include "sync_algorithm.h"
#include "fdstream.h"

template <typename DatabaseClient, typename OutputStream>
void handle_rows_command(DatabaseClient &client, Packer<OutputStream> &output, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key) {
	const Table &table(client.table_by_name(table_name));

	send_command(output, "rows", table_name, prev_key, last_key);
	RowPacker<typename DatabaseClient::RowType, OutputStream> row_packer(output);
	client.retrieve_rows(table, prev_key, last_key, row_packer);
}

template <typename DatabaseClient, typename OutputStream>
void handle_hash_command(DatabaseClient &client, Packer<OutputStream> &output, const string &table_name, ColumnValues &prev_key, ColumnValues &last_key, string &hash) { // mutable as we allow check_hash_and_choose_next_range to update the values; caller has no use for the original values once passed
	const Table &table(client.table_by_name(table_name));

	check_hash_and_choose_next_range(client, table, prev_key, last_key, hash);

	if (hash.empty()) {
		// rows don't match, and there's only one or no rows left, so send it straight across, as if they had given the rows command
		handle_rows_command(client, output, table_name, prev_key, last_key /* empty, meaning to the end of the table */);
		
	} else {
		// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
		send_command(output, "hash", table_name, prev_key, last_key, hash);
	}
}

template <typename InputStream, typename OutputStream>
int negotiate_protocol_version(Unpacker<InputStream> &input, Packer<OutputStream> &output, int protocol_version_supported) {
	// all conversations must start with a "protocol" command to establish the language to be used
	Command command;
	input >> command;
	if (command.name != "protocol") {
		throw command_error("Expected a protocol command before " + command.name);
	}

	// the usable protocol is the highest out of those supported by the two ends
	int protocol = min(protocol_version_supported, (int)command.argument<int64_t>(0));

	// tell the other end what version was selected
	output << protocol;
	output.flush();
}

template<class DatabaseClient>
void sync_from(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	DatabaseClient client(database_host, database_port, database_name, database_username, database_password);
	FDReadStream in(read_from_descriptor);
	Unpacker<FDReadStream> input(in);
	FDWriteStream out(write_to_descriptor);
	Packer<FDWriteStream> output(out);

	int protocol = negotiate_protocol_version(input, output, PROTOCOL_VERSION_SUPPORTED);

	try {
		Command command;

		while (true) {
			input >> command;

			if (command.name == "hash") {
				string     table_name(command.argument<string>(0));
				ColumnValues prev_key(command.argument<ColumnValues>(1));
				ColumnValues last_key(command.argument<ColumnValues>(2));
				string           hash(command.argument<string>(3));
				handle_hash_command(client, output, table_name, prev_key, last_key, hash);

			} else if (command.name == "rows") {
				string     table_name(command.argument<string>(0));
				ColumnValues prev_key(command.argument<ColumnValues>(1));
				ColumnValues last_key(command.argument<ColumnValues>(2));
				handle_rows_command(client, output, table_name, prev_key, last_key);

			} else if (command.name == "export_snapshot") {
				output << client.export_snapshot();

			} else if (command.name == "import_snapshot") {
				string snapshot(command.argument<string>(0));
				client.import_snapshot(snapshot);
				output.pack_nil(); // arbitrary, sent to indicate we've started our transaction

			} else if (command.name == "unhold_snapshot") {
				client.unhold_snapshot();
				output.pack_nil(); // similarly arbitrary

			} else if (command.name == "without_snapshot") {
				client.start_read_transaction();
				output.pack_nil(); // similarly arbitrary

			} else if (command.name == "schema") {
				output << client.database_schema();

			} else if (command.name == "quit") {
				break;

			} else {
				throw command_error("Unknown command " + command.name);
			}

			output.flush();
		}
	} catch (const exception &e) {
		// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
		cerr << e.what() << endl;
		throw sync_error();
	}
}
