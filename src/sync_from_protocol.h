#include "schema.h"
#include "row_serialization.h"
#include "command.h"
#include "hash_algorithm.h"
#include "sync_error.h"
#include "defaults.h"

template <class Worker, class DatabaseClient>
struct SyncFromProtocol {
	SyncFromProtocol(Worker &worker):
		worker(worker),
		input(worker.input),
		output(worker.output),
		hash_algorithm(DEFAULT_HASH_ALGORITHM) { // until advised to use a different hash algorithm by the 'to' end
	}

	void handle_commands() {
		const Table *table;

		while (true) {
			switch (verb_t verb = input.next<verb_t>()) {
				case Commands::RANGE:
					handle_range_command();
					break;

				case Commands::HASH:
					handle_hash_command();
					break;

				case Commands::ROWS:
					handle_rows_command();
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

				case Commands::FILTERS:
					worker.handle_filters_command();
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

	void handle_range_command() {
		string table_name;
		read_all_arguments(input, table_name);
		worker.show_status("syncing " + table_name);

		const Table &table(*worker.tables_by_name.at(table_name));
		send_command(output, Commands::RANGE, table_name, first_key(worker.client, table), last_key(worker.client, table));
	}

	void handle_hash_command() {
		string table_name;
		ColumnValues prev_key, last_key;
		size_t rows_to_hash;
		read_all_arguments(input, table_name, prev_key, last_key, rows_to_hash);
		worker.show_status("syncing " + table_name);

		RowHasher hasher(hash_algorithm);
		size_t row_count = retrieve_rows(worker.client, hasher, *worker.tables_by_name.at(table_name), prev_key, last_key, rows_to_hash);

		send_command(output, Commands::HASH, table_name, prev_key, last_key, rows_to_hash, row_count, hasher.finish());
	}

	void handle_rows_command() {
		string table_name;
		ColumnValues prev_key, last_key;
		read_all_arguments(input, table_name, prev_key, last_key);
		worker.show_status("syncing " + table_name);

		send_command_begin(output, Commands::ROWS, table_name, prev_key, last_key);
		send_rows(*worker.tables_by_name.at(table_name), prev_key, last_key);
		send_command_end(output);
	}

	void send_rows(const Table &table, ColumnValues prev_key, const ColumnValues &last_key) {
		// we limit individual queries to an arbitrary limit of 10000 rows, to reduce annoying slow
		// queries that would otherwise be logged on the server and reduce buffering.
		const int BATCH_SIZE = 10000;
		RowPackerAndLastKey<FDWriteStream> row_packer(output, table.primary_key_columns);

		while (true) {
			size_t row_count = retrieve_rows(worker.client, row_packer, table, prev_key, last_key, BATCH_SIZE);
			if (row_count < BATCH_SIZE) break;
			prev_key = row_packer.last_key;
		}
	}

	void handle_hash_algorithm_command() {
		HashAlgorithm requested_hash_algorithm;
		read_all_arguments(input, requested_hash_algorithm);

		if (hash_algorithm == HashAlgorithm::md5 || hash_algorithm == HashAlgorithm::xxh64) {
			hash_algorithm = requested_hash_algorithm;
		}

		send_command(output, Commands::HASH_ALGORITHM, static_cast<int>(hash_algorithm));
	}

	// deprecated as actually not relevant under current protocol versions, but still supported for backwards compatibility
	void handle_target_block_size_command() {
		size_t target_minimum_block_size;
		read_all_arguments(input, target_minimum_block_size);
		send_command(output, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size); // older versions require that we always accept the requested size and send it back
	}

	Worker &worker;
	Unpacker<FDReadStream> &input;
	Packer<FDWriteStream> &output;
	HashAlgorithm hash_algorithm;
};
