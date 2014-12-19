#include "command.h"
#include "schema_serialization.h"
#include "filters.h"
#include "fdstream.h"
#include "sync_algorithm.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(
		const string &database_host, const string &database_port, const string &database_name, const string &database_username, const string &database_password,
		const string &set_variables, const string &filter_file,
		int read_from_descriptor, int write_to_descriptor, char *status_area, size_t status_size):
			client(database_host, database_port, database_name, database_username, database_password),
			filter_file(filter_file),
			in(read_from_descriptor),
			input(in),
			out(write_to_descriptor),
			output(out),
			status_area(status_area),
			status_size(status_size),
			target_block_size(1) {
		if (!set_variables.empty()) {
			client.execute("SET " + set_variables);
		}
	}

	void operator()() {
		show_status("negotiating");
		negotiate_protocol_version();

		show_status("ready");
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
		show_status(table_name);
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
		send_rows(table, prev_key, last_key);
		send_command_end(output);
	}

	inline void send_rows_and_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const string &hash) {
		send_command_begin(output, Commands::ROWS_AND_HASH_NEXT, prev_key, last_key, next_key, hash);
		send_rows(table, prev_key, last_key);
		send_command_end(output);
	}

	inline void send_rows_and_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const ColumnValues &failed_last_key, const string &hash) {
		send_command_begin(output, Commands::ROWS_AND_HASH_FAIL, prev_key, last_key, next_key, failed_last_key, hash);
		send_rows(table, prev_key, last_key);
		send_command_end(output);
	}

	void send_rows(const Table &table, ColumnValues prev_key, const ColumnValues &last_key) {
		// we limit individual queries to an arbitrary limit of 10000 rows, to reduce annoying slow
		// queries that would otherwise be logged on the server and reduce buffering.
		const int BATCH_SIZE = 10000;
		RowPackerAndLastKey<FDWriteStream> row_packer(output, table.primary_key_columns);

		while (true) {
			client.retrieve_rows(row_packer, table, prev_key, last_key, BATCH_SIZE);
			if (row_packer.row_count < BATCH_SIZE) break;
			prev_key = row_packer.last_key;
			row_packer.reset_row_count();
		}
	}

	void negotiate_protocol_version() {
		const int EARLIEST_PROTOCOL_VERSION_SUPPORTED = 5;
		const int LATEST_PROTOCOL_VERSION_SUPPORTED = 5;

		// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
		int their_protocol_version;
		read_expected_command(input, Commands::PROTOCOL, their_protocol_version);

		// the usable protocol is the highest out of those supported by the two ends, unless lower than the minimum in which case no version is usable
		protocol_version = max(EARLIEST_PROTOCOL_VERSION_SUPPORTED, min(LATEST_PROTOCOL_VERSION_SUPPORTED, their_protocol_version));

		// tell the other end what version was selected
		send_command(output, Commands::PROTOCOL, protocol_version);
	}

	void populate_database_schema() {
		client.populate_database_schema(database);

		for (Table &table : database.tables) {
			tables_by_name[table.name] = &table;
		}

		if (!filter_file.empty()) {
			load_filters(filter_file, tables_by_name);
		}
	}

	void show_status(string message) {
		strncpy(status_area, message.c_str(), status_size);
		status_area[status_size] = 0;
	}

	DatabaseClient client;
	Database database;
	map<string, Table*> tables_by_name;
	string filter_file;
	FDReadStream in;
	Unpacker<FDReadStream> input;
	FDWriteStream out;
	Packer<FDWriteStream> output;
	char *status_area;
	size_t status_size;

	int protocol_version;
	size_t target_block_size;
};

template<class DatabaseClient, typename... Options>
void sync_from(const Options &...options) {
	SyncFromWorker<DatabaseClient> worker(options...);
	worker();
}
