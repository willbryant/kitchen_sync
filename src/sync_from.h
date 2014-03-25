#include "command.h"
#include "schema_serialization.h"
#include "filters.h"
#include "fdstream.h"
#include "sync_algorithm.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, const char *filter_file, int read_from_descriptor, int write_to_descriptor):
		client(database_host, database_port, database_name, database_username, database_password),
		filter_file(filter_file),
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
			while (true) {
				verb_t verb;
				input >> verb;

				switch (verb) {
					case Commands::OPEN:
						table = handle_open_command();
						break;

					case Commands::HASH_NEXT:
						handle_hash_next_command(table);
						break;

					case Commands::HASH_FAIL:
						handle_hash_fail_command(table);
						break;

					case Commands::ROWS:
						handle_rows_command(table);
						break;

					case Commands::ROWS_AND_HASH_NEXT:
						handle_rows_and_hash_next_command(table);
						break;

					case Commands::ROWS_AND_HASH_FAIL:
						handle_rows_and_hash_fail_command(table);
						break;

					case Commands::EXPORT_SNAPSHOT:
						handle_export_snapshot_command();
						break;

					case Commands::IMPORT_SNAPSHOT:
						handle_import_snapshot_command();
						break;

					case Commands::UNHOLD_SNAPSHOT:
						handle_unhold_snapshot_command();
						break;

					case Commands::WITHOUT_SNAPSHOT:
						handle_without_snapshot_command();
						break;

					case Commands::SCHEMA:
						handle_schema_command();
						break;

					case Commands::TARGET_BLOCK_SIZE:
						handle_target_block_size_command();
						break;

					case Commands::QUIT:
						read_all_arguments(input);
						return;

					default:
						throw command_error("Unknown command " + to_string(verb));
				}

				output.flush();
			}
		} catch (const exception &e) {
			// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
			cerr << e.what() << endl;
			throw sync_error();
		}
	}

	const Table *handle_open_command() {
		string table_name;
		read_all_arguments(input, table_name);
		const Table *table = tables_by_name.at(table_name); // throws out_of_range if not present in the map
		hash_first_range(*this, *table, target_block_size);
		return table;
	}

	void handle_hash_next_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before hash command");
		ColumnValues prev_key, last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, hash);
		check_hash_and_choose_next_range(*this, *table, nullptr, prev_key, last_key, nullptr, hash, target_block_size);
	}

	void handle_hash_fail_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before hash command");
		ColumnValues prev_key, last_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, failed_last_key, hash);
		check_hash_and_choose_next_range(*this, *table, nullptr, prev_key, last_key, &failed_last_key, hash, target_block_size);
	}

	void handle_rows_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before rows command");
		ColumnValues prev_key, last_key;
		read_all_arguments(input, prev_key, last_key);
		send_rows_command(*table, prev_key, last_key);
	}

	void handle_rows_and_hash_next_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before rows+hash next command");
		ColumnValues prev_key, last_key, next_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, next_key, hash);
		check_hash_and_choose_next_range(*this, *table, &prev_key, last_key, next_key, nullptr, hash, target_block_size);
	}

	void handle_rows_and_hash_fail_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before rows+hash fail command");
		ColumnValues prev_key, last_key, next_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, next_key, failed_last_key, hash);
		check_hash_and_choose_next_range(*this, *table, &prev_key, last_key, next_key, &failed_last_key, hash, target_block_size);
	}

	void handle_export_snapshot_command() {
		read_all_arguments(input);
		send_command(output, Commands::EXPORT_SNAPSHOT, client.export_snapshot());
		populate_database_schema();
	}

	void handle_import_snapshot_command() {
		string snapshot;
		read_all_arguments(input, snapshot);
		client.import_snapshot(snapshot);
		send_command(output, Commands::IMPORT_SNAPSHOT); // just to indicate that we have completed the command
		populate_database_schema();
	}

	void handle_unhold_snapshot_command() {
		read_all_arguments(input);
		client.unhold_snapshot();
		send_command(output, Commands::UNHOLD_SNAPSHOT); // just to indicate that we have completed the command
	}

	void handle_without_snapshot_command() {
		read_all_arguments(input);
		client.start_read_transaction();
		send_command(output, Commands::WITHOUT_SNAPSHOT); // just to indicate that we have completed the command
		populate_database_schema();
	}

	void handle_schema_command() {
		read_all_arguments(input);
		send_command(output, Commands::SCHEMA, database);
	}

	void handle_target_block_size_command() {
		read_all_arguments(input, target_block_size);
		send_command(output, Commands::TARGET_BLOCK_SIZE, target_block_size); // we always accept the requested size and send it back (but the test suite doesn't)
	}

	inline void send_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		send_command(output, Commands::HASH_NEXT, prev_key, last_key, hash);
	}

	inline void send_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &failed_last_key, const string &hash) {
		send_command(output, Commands::HASH_FAIL, prev_key, last_key, failed_last_key, hash);
	}

	inline void send_rows_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		send_command_begin(output, Commands::ROWS, prev_key, last_key);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		send_command_end(output);
	}

	inline void send_rows_and_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const string &hash) {
		send_command_begin(output, Commands::ROWS_AND_HASH_NEXT, prev_key, last_key, next_key, hash);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		send_command_end(output);
	}

	inline void send_rows_and_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const ColumnValues &failed_last_key, const string &hash) {
		send_command_begin(output, Commands::ROWS_AND_HASH_FAIL, prev_key, last_key, next_key, failed_last_key, hash);
		client.retrieve_rows(table, prev_key, last_key, row_packer);
		send_command_end(output);
	}

	void negotiate_protocol_version() {
		const int PROTOCOL_VERSION_SUPPORTED = 2;

		// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
		int their_protocol_version;
		read_expected_command(input, Commands::PROTOCOL, their_protocol_version);

		// the usable protocol is the highest out of those supported by the two ends
		protocol_version = min(PROTOCOL_VERSION_SUPPORTED, their_protocol_version);

		// tell the other end what version was selected
		send_command(output, Commands::PROTOCOL, protocol_version);
	}

	void populate_database_schema() {
		client.populate_database_schema(database);

		for (Table &table : database.tables) {
			tables_by_name[table.name] = &table;
		}

		if (filter_file && *filter_file) {
			load_filters(filter_file, tables_by_name);
		}
	}

	DatabaseClient client;
	Database database;
	map<string, Table*> tables_by_name;
	const char *filter_file;
	FDReadStream in;
	Unpacker<FDReadStream> input;
	FDWriteStream out;
	Packer<FDWriteStream> output;
	RowPacker<FDWriteStream> row_packer;

	int protocol_version;
	size_t target_block_size;
};

template<class DatabaseClient, typename... Options>
void sync_from(const Options &...options) {
	SyncFromWorker<DatabaseClient> worker(options...);
	worker();
}
