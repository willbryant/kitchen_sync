#include "command.h"
#include "schema_serialization.h"
#include "sync_algorithm.h"
#include "fdstream.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor):
		client(database_host, database_port, database_name, database_username, database_password),
		in(read_from_descriptor),
		input(in),
		out(write_to_descriptor),
		output(out),
		row_packer(output) {
	}

	void operator()() {
		negotiate_protocol_version();

		string current_table_name;

		try {
			Command command;

			while (true) {
				input >> command;

				if (command.verb == Commands::OPEN) {
					current_table_name = command.argument<string>(0);
					handle_open_command(current_table_name);

				} else if (command.verb == Commands::HASH) {
					if (current_table_name.empty()) throw command_error("Expected a table command before hash command");
					ColumnValues prev_key(command.argument<ColumnValues>(0));
					ColumnValues last_key(command.argument<ColumnValues>(1));
					string           hash(command.argument<string>(2));
					handle_hash_command(current_table_name, prev_key, last_key, hash);

				} else if (command.verb == Commands::ROWS) {
					if (current_table_name.empty()) throw command_error("Expected a table command before rows command");
					ColumnValues prev_key(command.argument<ColumnValues>(0));
					ColumnValues last_key(command.argument<ColumnValues>(1));
					handle_rows_command(current_table_name, prev_key, last_key);

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
					throw command_error("Unknown command " + to_string(command.verb));
				}

				output.flush();
			}
		} catch (const exception &e) {
			// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
			cerr << e.what() << endl;
			throw sync_error();
		}
	}

	inline void send_hash_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		send_command(output, Commands::HASH, prev_key, last_key, hash);
	}

	inline void send_rows_command(const Table &table, ColumnValues &prev_key, ColumnValues &last_key) {
		send_command(output, Commands::ROWS, prev_key, last_key);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		row_packer.pack_end();

		// if that range extended to the end of the table, we're done
		if (last_key.empty()) return;

		// and then follow up straight away with the next command
		prev_key = last_key;
		find_hash_of_next_range(*this, client, table, 1, prev_key, last_key);
	}

	void handle_rows_command(const string &table_name, ColumnValues &prev_key, ColumnValues &last_key) { // mutable as we allow find_hash_of_next_range to update the values; caller has no use for the original values once passed
		const Table &table(client.table_by_name(table_name));

		// send the requested rows
		send_rows_command(table, prev_key, last_key);
	}

	void handle_open_command(const string &table_name) {
		const Table &table(client.table_by_name(table_name));

		ColumnValues prev_key;
		ColumnValues last_key;
		find_hash_of_next_range(*this, client, table, 1, prev_key, last_key);
	}

	void handle_hash_command(const string &table_name, ColumnValues &prev_key, ColumnValues &last_key, string &hash) { // mutable as we allow check_hash_and_choose_next_range to update the values; caller has no use for the original values once passed
		const Table &table(client.table_by_name(table_name));

		check_hash_and_choose_next_range(*this, client, table, prev_key, last_key, hash);
	}

	void negotiate_protocol_version() {
		const int PROTOCOL_VERSION_SUPPORTED = 1;

		// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
		Command command;
		input >> command;
		if (command.verb != Commands::PROTOCOL) {
			throw command_error("Expected a protocol command before " + to_string(command.verb));
		}

		// the usable protocol is the highest out of those supported by the two ends
		protocol = min(PROTOCOL_VERSION_SUPPORTED, (int)command.argument<int64_t>(0));

		// tell the other end what version was selected
		output << protocol;
		output.flush();
	}

	DatabaseClient client;
	FDReadStream in;
	Unpacker<FDReadStream> input;
	FDWriteStream out;
	Packer<FDWriteStream> output;
	RowPacker<typename DatabaseClient::RowType, FDWriteStream> row_packer;

	int protocol;
};

template<class DatabaseClient>
void sync_from(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor) {
	SyncFromWorker<DatabaseClient> worker(database_host, database_port, database_name, database_username, database_password, read_from_descriptor, write_to_descriptor);
	worker();
}
