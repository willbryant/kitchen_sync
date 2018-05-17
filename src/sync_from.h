#include "command.h"
#include "schema_serialization.h"
#include "filter_serialization.h"
#include "filters.h"
#include "fdstream.h"
#include "sync_from_protocol.h"

template<class DatabaseClient>
struct SyncFromWorker {
	SyncFromWorker(
		const string &database_host, const string &database_port, const string &database_name, const string &database_username, const string &database_password,
		const string &set_variables, const string &filter_file,
		int read_from_descriptor, int write_to_descriptor, char *status_area, size_t status_size):
			client(database_host, database_port, database_name, database_username, database_password, set_variables),
			filter_file(filter_file),
			in(read_from_descriptor),
			input(in),
			out(write_to_descriptor),
			output(out),
			status_area(status_area),
			status_size(status_size),
			protocol_version(0) {
	}

	void operator()() {
		show_status("negotiating");
		negotiate_protocol_version();

		show_status("ready");

		try {
			SyncFromProtocol<SyncFromWorker<DatabaseClient>, DatabaseClient> sync_from_protocol(*this);
			sync_from_protocol.handle_commands();
		} catch (const exception &e) {
			// in fact we just output these errors much the same way that our caller does, but we do it here (before the stream gets closed) to help tests
			cerr << "Error in the 'from' worker: " << e.what() << endl;
			throw sync_error();
		}
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

	void negotiate_protocol_version() {
		const int EARLIEST_PROTOCOL_VERSION_SUPPORTED = 7;
		const int LATEST_PROTOCOL_VERSION_SUPPORTED = 7;

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

		// older versions pass the file to be read to the 'from' endpoint as a startup option
		if (!filter_file.empty()) {
			apply_filters(load_filters(filter_file), database.tables);
		}
	}

	void handle_filters_command() {
		// newer versions pass the parsed contents of the file from the 'to' endpoint over the connection
		TableFilters table_filters;
		read_all_arguments(input, table_filters);

		apply_filters(table_filters, database.tables);

		send_command(output, Commands::FILTERS);
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
};

template<class DatabaseClient, typename... Options>
void sync_from(const Options &...options) {
	SyncFromWorker<DatabaseClient> worker(options...);
	worker();
}
