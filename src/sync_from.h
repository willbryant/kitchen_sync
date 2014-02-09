#include "command.h"
#include "schema_serialization.h"
#include "sync_algorithm.h"
#include "fdstream.h"

template <typename OutputStream>
inline void send_hash_response(Packer<OutputStream> &output, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
	send_command(output, Commands::HASH, table_name, prev_key, last_key, hash);
}

template <typename DatabaseClient, typename OutputStream>
inline void send_rows_response(DatabaseClient &client, Packer<OutputStream> &output, const Table &table, ColumnValues &prev_key, ColumnValues &last_key) {
	send_command(output, Commands::ROWS, table.name, prev_key, last_key);
	RowPacker<typename DatabaseClient::RowType, OutputStream> row_packer(output);
	client.retrieve_rows(table, prev_key, last_key, row_packer);
}

template <typename DatabaseClient, typename OutputStream>
void handle_rows_command(DatabaseClient &client, Packer<OutputStream> &output, const string &table_name, ColumnValues &prev_key, ColumnValues &last_key) { // mutable as we allow find_hash_of_next_range to update the values; caller has no use for the original values once passed
	const Table &table(client.table_by_name(table_name));

	if (last_key.empty()) {
		// send all remaining rows and we're done
		send_rows_response(client, output, table, prev_key, last_key);
	} else {
		// find out what comes after the requested range
		ColumnValues following_last_key;
		string hash;
		find_hash_of_next_range(client, table, 1, last_key, following_last_key, hash);

		if (following_last_key.empty()) {
			// if there's no more rows, we can simply extend the requested range to the end of the table and return it
			send_rows_response(client, output, table, prev_key, following_last_key);
		} else {
			// send the rows in the given range, and then immediately follow on with the hash of the subsequent range
			send_rows_response(client, output, table, prev_key, last_key);
			send_hash_response(output, table_name, last_key, following_last_key, hash);
		}
	}
}

template <typename DatabaseClient, typename OutputStream>
void handle_hash_command(DatabaseClient &client, Packer<OutputStream> &output, const string &table_name, ColumnValues &prev_key, ColumnValues &last_key, string &hash) { // mutable as we allow check_hash_and_choose_next_range to update the values; caller has no use for the original values once passed
	const Table &table(client.table_by_name(table_name));

	size_t row_count = check_hash_and_choose_next_range(client, table, prev_key, last_key, hash);

	if (hash.empty()) {
		// rows don't match, and there's only one or no rows left, so send it straight across, as if they had given the rows command
		handle_rows_command(client, output, table_name, prev_key, last_key);
		
	} else {
		// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
		send_hash_response(output, table_name, prev_key, last_key, hash);
	}
}

template <typename InputStream, typename OutputStream>
int negotiate_protocol_version(Unpacker<InputStream> &input, Packer<OutputStream> &output, int protocol_version_supported) {
	// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
	Command command;
	input >> command;
	if (command.verb != Commands::PROTOCOL) {
		throw command_error("Expected a protocol command before " + command.verb);
	}

	// the usable protocol is the highest out of those supported by the two ends
	int protocol = min(protocol_version_supported, (int)command.argument<int64_t>(0));

	// tell the other end what version was selected
	output << protocol;
	output.flush();
	
	return protocol;
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

			if (command.verb == Commands::HASH) {
				string     table_name(command.argument<string>(0));
				ColumnValues prev_key(command.argument<ColumnValues>(1));
				ColumnValues last_key(command.argument<ColumnValues>(2));
				string           hash(command.argument<string>(3));
				handle_hash_command(client, output, table_name, prev_key, last_key, hash);

			} else if (command.verb == Commands::ROWS) {
				string     table_name(command.argument<string>(0));
				ColumnValues prev_key(command.argument<ColumnValues>(1));
				ColumnValues last_key(command.argument<ColumnValues>(2));
				handle_rows_command(client, output, table_name, prev_key, last_key);

			} else if (command.verb == Commands::EXPORT_SNAPSHOT) {
				output << client.export_snapshot();

			} else if (command.verb == Commands::IMPORT_SNAPSHOT) {
				string snapshot(command.argument<string>(0));
				client.import_snapshot(snapshot);
				output.pack_nil(); // arbitrary, sent to indicate we've started our transaction

			} else if (command.verb == Commands::UNHOLD_SNAPSHOT) {
				client.unhold_snapshot();
				output.pack_nil(); // similarly arbitrary

			} else if (command.verb == Commands::WITHOUT_SNAPSHOT) {
				client.start_read_transaction();
				output.pack_nil(); // similarly arbitrary

			} else if (command.verb == Commands::SCHEMA) {
				output << client.database_schema();

			} else if (command.verb == Commands::QUIT) {
				break;

			} else {
				throw command_error("Unknown command " + command.verb);
			}

			output.flush();
		}
	} catch (const exception &e) {
		// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
		cerr << e.what() << endl;
		throw sync_error();
	}
}
