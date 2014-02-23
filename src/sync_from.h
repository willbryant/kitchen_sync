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
		row_packer(output),
		target_block_size(1) {
	}

	void operator()() {
		negotiate_protocol_version();

		const Table *table;

		try {
			Command command;

			while (true) {
				input >> command;

				if (command.verb == Commands::OPEN) {
					string table_name = command.argument<string>(0);
					table = &client.table_by_name(table_name);
					hash_first_range(*this, *table, target_block_size);

				} else if (command.verb == Commands::HASH_NEXT) {
					if (!table) throw command_error("Expected a table command before hash command");
					ColumnValues   prev_key(command.argument<ColumnValues>(0));
					ColumnValues   last_key(command.argument<ColumnValues>(1));
					string             hash(command.argument<string>(2));
					check_hash_and_choose_next_range(*this, *table, NULL, prev_key, last_key, NULL, hash, target_block_size);

				} else if (command.verb == Commands::HASH_FAIL) {
					if (!table) throw command_error("Expected a table command before hash command");
					ColumnValues        prev_key(command.argument<ColumnValues>(0));
					ColumnValues        last_key(command.argument<ColumnValues>(1));
					ColumnValues failed_last_key(command.argument<ColumnValues>(2));
					string                  hash(command.argument<string>(3));
					check_hash_and_choose_next_range(*this, *table, NULL, prev_key, last_key, &failed_last_key, hash, target_block_size);

				} else if (command.verb == Commands::ROWS) {
					if (!table) throw command_error("Expected a table command before rows command");
					ColumnValues prev_key(command.argument<ColumnValues>(0));
					ColumnValues last_key(command.argument<ColumnValues>(1));
					send_rows_command(*table, prev_key, last_key);

				} else if (command.verb == Commands::ROWS_AND_HASH_NEXT) {
					if (!table) throw command_error("Expected a table command before rows+hash command");
					ColumnValues prev_key(command.argument<ColumnValues>(0));
					ColumnValues last_key(command.argument<ColumnValues>(1));
					ColumnValues next_key(command.argument<ColumnValues>(2));
					string           hash(command.argument<string>(3));
					check_hash_and_choose_next_range(*this, *table, &prev_key, last_key, next_key, NULL, hash, target_block_size);

				} else if (command.verb == Commands::ROWS_AND_HASH_FAIL) {
					if (!table) throw command_error("Expected a table command before rows+hash command");
					ColumnValues        prev_key(command.argument<ColumnValues>(0));
					ColumnValues        last_key(command.argument<ColumnValues>(1));
					ColumnValues        next_key(command.argument<ColumnValues>(2));
					ColumnValues failed_last_key(command.argument<ColumnValues>(3));
					string                  hash(command.argument<string>(4));
					check_hash_and_choose_next_range(*this, *table, &prev_key, last_key, next_key, &failed_last_key, hash, target_block_size);

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

				} else if (command.verb == Commands::TARGET_BLOCK_SIZE) {
					target_block_size = command.argument<int64_t>(0); // strictly speaking this should use size_t, but unpack_any doesn't know about different numeric types
					output << target_block_size; // we always accept the requested size, but the test suite doesn't

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

	inline void send_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		send_command(output, Commands::HASH_NEXT, prev_key, last_key, hash);
	}

	inline void send_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &failed_last_key, const string &hash) {
		send_command(output, Commands::HASH_FAIL, prev_key, last_key, failed_last_key, hash);
	}

	inline void send_rows_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		send_command(output, Commands::ROWS, prev_key, last_key);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		row_packer.pack_end();
	}

	inline void send_rows_and_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const string &hash) {
		send_command(output, Commands::ROWS_AND_HASH_NEXT, prev_key, last_key, next_key, hash);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		row_packer.pack_end();
	}

	inline void send_rows_and_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const ColumnValues &failed_last_key, const string &hash) {
		send_command(output, Commands::ROWS_AND_HASH_FAIL, prev_key, last_key, next_key, failed_last_key, hash);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		row_packer.pack_end();
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
		protocol_version = min(PROTOCOL_VERSION_SUPPORTED, (int)command.argument<int64_t>(0));

		// tell the other end what version was selected
		output << protocol_version;
		output.flush();
	}

	DatabaseClient client;
	FDReadStream in;
	Unpacker<FDReadStream> input;
	FDWriteStream out;
	Packer<FDWriteStream> output;
	RowPacker<FDWriteStream> row_packer;

	int protocol_version;
	size_t target_block_size;
};

template<class DatabaseClient>
void sync_from(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int read_from_descriptor, int write_to_descriptor) {
	SyncFromWorker<DatabaseClient> worker(database_host, database_port, database_name, database_username, database_password, read_from_descriptor, write_to_descriptor);
	worker();
}
