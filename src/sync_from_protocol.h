#include "sync_algorithm.h"
#include "defaults.h"

template <class Worker, class DatabaseClient>
struct SyncFromProtocol {
	SyncFromProtocol(Worker &worker):
		worker(worker),
		input(worker.input),
		output(worker.output),
		sync_algorithm(*this, worker.client, DEFAULT_HASH_ALGORITHM), // until advised to use a different hash algorithm by the 'to' end
		target_minimum_block_size(1),
		target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE) {
	}

	void handle_commands() {
		const Table *table;

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
					worker.handle_export_snapshot_command();
					break;

				case Commands::IMPORT_SNAPSHOT:
					worker.handle_import_snapshot_command();
					break;

				case Commands::UNHOLD_SNAPSHOT:
					worker.handle_unhold_snapshot_command();
					break;

				case Commands::WITHOUT_SNAPSHOT:
					worker.handle_without_snapshot_command();
					break;

				case Commands::SCHEMA:
					worker.handle_schema_command();
					break;

				case Commands::TARGET_BLOCK_SIZE:
					handle_target_block_size_command();
					break;

				case Commands::HASH_ALGORITHM:
					handle_hash_algorithm_command();
					break;

				case Commands::QUIT:
					read_all_arguments(input);
					return;

				default:
					throw command_error("Unknown command " + to_string(verb));
			}

			output.flush();
		}
	}

	const Table *handle_open_command() {
		string table_name;
		read_all_arguments(input, table_name);
		const Table *table = worker.tables_by_name.at(table_name); // throws out_of_range if not present in the map
		worker.show_status("syncing " + table_name);
		sync_algorithm.hash_first_range(*table, target_minimum_block_size);
		return table;
	}

	void handle_hash_next_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before hash command");
		ColumnValues prev_key, last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, hash);
		sync_algorithm.check_hash_and_choose_next_range(*table, nullptr, prev_key, last_key, nullptr, hash, target_minimum_block_size, target_maximum_block_size);
	}

	void handle_hash_fail_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before hash command");
		ColumnValues prev_key, last_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, failed_last_key, hash);
		sync_algorithm.check_hash_and_choose_next_range(*table, nullptr, prev_key, last_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
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
		sync_algorithm.check_hash_and_choose_next_range(*table, &prev_key, last_key, next_key, nullptr, hash, target_minimum_block_size, target_maximum_block_size);
	}

	void handle_rows_and_hash_fail_command(const Table *table) {
		if (!table) throw command_error("Expected a table command before rows+hash fail command");
		ColumnValues prev_key, last_key, next_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, next_key, failed_last_key, hash);
		sync_algorithm.check_hash_and_choose_next_range(*table, &prev_key, last_key, next_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
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
			sync_algorithm.client.retrieve_rows(row_packer, table, prev_key, last_key, BATCH_SIZE);
			if (row_packer.row_count < BATCH_SIZE) break;
			prev_key = row_packer.last_key;
			row_packer.reset_row_count();
		}
	}

	void handle_hash_algorithm_command() {
		read_all_arguments(input, sync_algorithm.hash_algorithm);
		send_command(output, Commands::HASH_ALGORITHM, sync_algorithm.hash_algorithm); // we always accept the requested algorithm and send it back (but maybe one day we won't)
	}

	void handle_target_block_size_command() {
		read_all_arguments(input, target_minimum_block_size);
		send_command(output, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size); // we always accept the requested size and send it back (but the test suite doesn't)
	}

	Worker &worker;
	Unpacker<FDReadStream> &input;
	Packer<FDWriteStream> &output;
	SyncAlgorithm<SyncFromProtocol<Worker, DatabaseClient>, DatabaseClient> sync_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
};
