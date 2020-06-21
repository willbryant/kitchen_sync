#include "defaults.h"
#include "protocol_versions.h"
#include "command.h"
#include "versioned_stream.h"
#include "schema.h"
#include "schema_serialization.h"
#include "row_serialization.h"
#include "filter_serialization.h"
#include "filters.h"
#include "query_functions.h"
#include "hash_algorithm.h"
#include "sync_error.h"
#include "substitute_primary_key.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(
		const string &database_host, const string &database_port, const string &database_name, const string &database_username, const string &database_password,
		const string &set_variables,
		int read_from_descriptor, int write_to_descriptor, char *status_area, size_t status_size):
			client(database_host, database_port, database_name, database_username, database_password, set_variables),
			input_stream(read_from_descriptor),
			input(input_stream),
			output_stream(write_to_descriptor),
			output(output_stream),
			hash_algorithm(HashAlgorithm::blake3), // really only the default for tests, as for real runs the hash algorithm is sent by a command from the 'to' end
			status_area(status_area),
			status_size(status_size) {
	}

	void operator()() {
		show_status("negotiating");
		negotiate_protocol_version();

		show_status("ready");

		try {
			handle_commands();
		} catch (const exception &e) {
			// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
			cerr << "Error in the 'from' worker: " << e.what() << endl;
			throw sync_error();
		}
	}

	void handle_commands() {
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

				case Commands::IDLE:
					handle_idle_command();
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

				case Commands::HASH_ALGORITHM:
					handle_hash_algorithm_command();
					break;

				case Commands::FILTERS:
					handle_filters_command();
					break;

				case Commands::TYPES:
					handle_types_command();
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

	void negotiate_protocol_version() {
		// all conversations must start with a Commands::PROTOCOL command to establish the language to be used
		int their_protocol_version;
		read_expected_command(input, Commands::PROTOCOL, their_protocol_version);

		// the usable protocol is the highest out of those supported by the two ends, unless lower than the minimum in which case no version is usable
		input_stream.protocol_version = output_stream.protocol_version = max(EARLIEST_PROTOCOL_VERSION_SUPPORTED, min(LATEST_PROTOCOL_VERSION_SUPPORTED, their_protocol_version));

		// tell the other end what version was selected
		send_command(output, Commands::PROTOCOL, output_stream.protocol_version);
	}

	void handle_filters_command() {
		read_all_arguments(input, table_filters);

		// versions 7 and below of the protocol sent the FILTERS command after retrieving the schema,
		// so we applied it to the already-loaded schema immediately.  this meant however that we
		// couldn't consider the filters when doing things like choosing a substitute primary key,
		// so from version 8 and on we are sent the FILTERS command early and apply them later.
		// note that if a filter_definition_error occurs for v7, we let it bubble up and error out
		// immediately, in contrast to v8 which rescues and serializes them.
		if (output_stream.protocol_version <= LAST_FILTERS_AFTER_SNAPSHOT_PROTOCOL_VERSION) {
			apply_filters(table_filters, database.tables);
		}

		send_command(output, Commands::FILTERS);
	}

	void handle_types_command() {
		set<string> accepted_type_names;
		read_all_arguments(input, accepted_type_names);
		for (const string &name: accepted_type_names) {
			auto it = ColumnTypesByName.find(name);
			if (it != ColumnTypesByName.cend()) accepted_types.insert(it->second);
		}
		send_command(output, Commands::TYPES);
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

	void populate_database_schema() {
		if (output_stream.protocol_version <= LAST_LEGACY_SCHEMA_FORMAT_VERSION) {
			accepted_types = LegacySupportedColumnTypes;
		}
		client.populate_database_schema(database, accepted_types);

		if (!table_filters.empty()) { // optimisation, but also will always skip for protocol 7 and earlier, per above comment
			try {
				apply_filters(table_filters, database.tables);
			} catch (const filter_definition_error &e) {
				database.errors.push_back(e.what());
				return; // bail out with tables_by_id still empty so tests that don't pay attention to the error and terminate can't mess about making other requests and giving confusing output
			}
		}

		for (Table &table : database.tables) {
			tables_by_id[table.id_from_name()] = &table;

			// if the table doesn't have an actual primary key, choose one
			choose_primary_key_for(table);
		}
	}

	void handle_schema_command() {
		read_all_arguments(input);
		send_command(output, Commands::SCHEMA, database);
	}

	void handle_range_command() {
		string table_id;
		read_all_arguments(input, table_id);
		show_status("syncing " + table_id);

		const Table &table(*tables_by_id.at(table_id));
		send_command(output, Commands::RANGE, table_id, first_key(client, table), last_key(client, table));
	}

	void handle_hash_command() {
		string table_id;
		ColumnValues prev_key, last_key;
		size_t rows_to_hash;
		read_all_arguments(input, table_id, prev_key, last_key, rows_to_hash);
		show_status("syncing " + table_id);

		RowHasher hasher(hash_algorithm);
		size_t row_count = retrieve_rows(client, hasher, *tables_by_id.at(table_id), prev_key, last_key, rows_to_hash);

		send_command(output, Commands::HASH, table_id, prev_key, last_key, rows_to_hash, row_count, hasher.finish());
	}

	void handle_rows_command() {
		string table_id;
		ColumnValues prev_key, last_key;
		read_all_arguments(input, table_id, prev_key, last_key);
		show_status("syncing " + table_id);

		send_command_begin(output, Commands::ROWS, table_id, prev_key, last_key);
		send_rows(*tables_by_id.at(table_id), prev_key, last_key);
		send_command_end(output);
	}

	void send_rows(const Table &table, ColumnValues prev_key, const ColumnValues &last_key) {
		// we limit individual queries to an arbitrary limit of 10000 rows, to reduce annoying slow
		// queries that would otherwise be logged on the server and reduce buffering.
		const int BATCH_SIZE = 10000;
		RowPackerAndLastKey<VersionedFDWriteStream> row_packer(output, table.primary_key_columns);

		while (true) {
			size_t row_count = retrieve_rows(client, row_packer, table, prev_key, last_key, BATCH_SIZE);
			if (row_count < BATCH_SIZE) break;
			prev_key = row_packer.last_key;
		}
	}

	void handle_idle_command() {
		show_status("idle");
		read_all_arguments(input);
		send_command(output, Commands::IDLE);
	}

	void handle_hash_algorithm_command() {
		HashAlgorithm requested_hash_algorithm;
		read_all_arguments(input, requested_hash_algorithm);

		if (hash_algorithm == HashAlgorithm::md5 || hash_algorithm == HashAlgorithm::xxh64 || hash_algorithm == HashAlgorithm::blake3) {
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

	void show_status(string message) {
		strncpy(status_area, message.c_str(), status_size);
		status_area[status_size] = 0;
	}

	DatabaseClient client;
	Database database;
	map<string, Table*> tables_by_id;
	VersionedFDReadStream input_stream;
	Unpacker<VersionedFDReadStream> input;
	VersionedFDWriteStream output_stream;
	Packer<VersionedFDWriteStream> output;
	HashAlgorithm hash_algorithm;
	TableFilters table_filters;
	ColumnTypeList accepted_types;
	char *status_area;
	size_t status_size;
};

template<class DatabaseClient, typename... Options>
void sync_from(const Options &...options) {
	SyncFromWorker<DatabaseClient> worker(options...);
	worker();
}
