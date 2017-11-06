#include "command.h"
#include "schema_serialization.h"
#include "filters.h"
#include "fdstream.h"
#include "sync_from_protocol.h"
#include "sync_from_protocol_6.h"

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
			protocol_version(0) {
		if (!set_variables.empty()) {
			client.execute("SET " + set_variables);
		}
	}

	void operator()() {
		show_status("negotiating");
		negotiate_protocol_version();

		show_status("ready");

		try {
			if (protocol_version <= 6) {
				SyncFromProtocol6<SyncFromWorker<DatabaseClient>, DatabaseClient> sync_from_protocol(*this);
				sync_from_protocol.handle_commands();
			} else {
				SyncFromProtocol<SyncFromWorker<DatabaseClient>, DatabaseClient> sync_from_protocol(*this);
				sync_from_protocol.handle_commands();
			}
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
		const int EARLIEST_PROTOCOL_VERSION_SUPPORTED = 6;
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

		if (!filter_file.empty()) {
			apply_filters(load_filters(filter_file));
		}
	}

	void apply_filters(const TableFilters &table_filters) {
		for (auto const &it : table_filters) {
			const string &table_name(it.first);
			const TableFilter &table_filter(it.second);

			Table *table = tables_by_name[table_name];
			if (!table) throw runtime_error("Filtered table '" + table_name + "' not found");

			apply_filter(*table, table_filter);
		}
	}

	void apply_filter(Table &table, const TableFilter &table_filter) {
		table.where_conditions = table_filter.where_conditions;

		map<string, Column*> columns_by_name;
		for (Column &column : table.columns) {
			columns_by_name[column.name] = &column;
		}

		for (auto const &it : table_filter.filter_expressions) {
			const string &column_name(it.first);
			const string &filter_expression(it.second);

			Column *column = columns_by_name[column_name];
			if (!column) throw runtime_error("Can't find column '" + column_name + "' to filter in table '" + table.name + "'");

			column->filter_expression = filter_expression;
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
};

template<class DatabaseClient, typename... Options>
void sync_from(const Options &...options) {
	SyncFromWorker<DatabaseClient> worker(options...);
	worker();
}
