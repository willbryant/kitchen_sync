#include <thread>

#include "command.h"
#include "commit_level.h"
#include "schema_functions.h"
#include "schema_matcher.h"
#include "sync_queue.h"
#include "row_range_applier.h"
#include "reset_table_sequences.h"
#include "versioned_stream.h"
#include "sync_to_protocol.h"
#include "defaults.h"

using namespace std;

template <typename DatabaseClient>
struct SyncToWorker {
	SyncToWorker(
		Database &database, SyncQueue<DatabaseClient> &sync_queue, bool leader, int worker_number, int read_from_descriptor, int write_to_descriptor,
		const string &database_host, const string &database_port, const string &database_name, const string &database_username, const string &database_password,
		const string &set_variables, const string &filter_file, const set<string> &ignore_tables, const set<string> &only_tables,
		int verbose, bool progress, bool snapshot, bool alter, CommitLevel commit_level,
		HashAlgorithm hash_algorithm, size_t target_minimum_block_size, size_t target_maximum_block_size,
		bool structure_only):
			database(database),
			sync_queue(sync_queue),
			leader(leader),
			worker_number(worker_number),
			input_stream(read_from_descriptor),
			output_stream(write_to_descriptor),
			input(input_stream),
			output(output_stream),
			client(database_host, database_port, database_name, database_username, database_password, set_variables),
			table_filters(load_filters(filter_file)),
			ignore_tables(ignore_tables),
			only_tables(only_tables),
			verbose(verbose),
			progress(progress),
			snapshot(snapshot),
			alter(alter),
			commit_level(commit_level),
			hash_algorithm(hash_algorithm),
			target_minimum_block_size(target_minimum_block_size),
			target_maximum_block_size(target_maximum_block_size),
			structure_only(structure_only),
			worker_thread(std::ref(*this)) {
	}

	~SyncToWorker() {
		worker_thread.join();
	}

	void operator()() {
		if (prepare() && !structure_only) {
			// schema matches, sync the data
			sync();
		} else {
			// send a quit command so the other end terminates gracefully rather than outputting an error itself
			send_quit_command();
		}

		// eagerly close the streams so that the SSH session terminates promptly on aborts
		output_stream.close();
	}

	bool prepare() {
		try {
			negotiate_protocol_version();
			share_snapshot();
			retrieve_database_schema();
			compare_schema();
			send_filters();
			sync_queue.wait_at_barrier();

			return true;
		} catch (const exception &e) {
			// make sure all other workers terminate promptly, and if we are the first to fail, output the error
			if (sync_queue.abort()) {
				cerr << e.what() << endl;
			}

			return false;
		}
	}

	void sync() {
		try {
			enqueue_tables();

			client.start_write_transaction();
			client.disable_referential_integrity();

			SyncToProtocol<SyncToWorker<DatabaseClient>, DatabaseClient> sync_to_protocol(*this);
			sync_to_protocol.sync_tables();

			wait_for_finish();

			client.enable_referential_integrity();
			if (commit_level >= CommitLevel::success) {
				commit();
			} else {
				rollback();
			}
		} catch (const exception &e) {
			// make sure all other workers terminate promptly, and if we are the first to fail, output the error
			if (sync_queue.abort()) {
				cerr << "Error in the 'to' worker: " << e.what() << endl;
			}

			// optionally, try to commit the changes we've made, but ignore any errors, and don't bother outputting timings
			if (commit_level == CommitLevel::always || commit_level == CommitLevel::often) {
				try { client.commit_transaction(); } catch (...) {}
			}
		}
	}

	void negotiate_protocol_version() {
		const int EARLIEST_PROTOCOL_VERSION_SUPPORTED = 7;
		const int LATEST_PROTOCOL_VERSION_SUPPORTED = 7;

		// tell the other end what version of the protocol we can speak, and have them tell us which version we're able to converse in
		send_command(output, Commands::PROTOCOL, LATEST_PROTOCOL_VERSION_SUPPORTED);

		// read the response to the protocol_version command that the output thread sends when it starts
		// this is currently unused, but the command's semantics need to be in place for it to be useful in the future...
		read_expected_command(input, Commands::PROTOCOL, output_stream.protocol_version);

		if (output_stream.protocol_version < EARLIEST_PROTOCOL_VERSION_SUPPORTED || output_stream.protocol_version > LATEST_PROTOCOL_VERSION_SUPPORTED) {
			throw runtime_error("Sorry, the other end doesn't support a compatible protocol version");
		}
	}

	void share_snapshot() {
		if (sync_queue.workers > 1 && snapshot) {
			// although some databases (such as postgresql) can share & adopt snapshots with no penalty
			// to other transactions, those that don't have an actual snapshot adoption mechanism (mysql)
			// need us to use blocking locks to prevent other transactions changing the data while they
			// start simultaneous transactions.  it's therefore important to minimize the time that we
			// hold the locks, so we wait for all workers to be up, running, and connected before
			// starting; this is also nicer (on all databases) in that it means no changes will be made
			// if some of the workers fail to start.
			sync_queue.wait_at_barrier();

			// now, request the lock or snapshot from the leader's peer.
			if (leader) {
				send_command(output, Commands::EXPORT_SNAPSHOT);
				read_expected_command(input, Commands::EXPORT_SNAPSHOT, sync_queue.snapshot);
			}
			sync_queue.wait_at_barrier();

			// as soon as it has responded, adopt the snapshot/start the transaction in each of the other workers.
			if (!leader) {
				send_command(output, Commands::IMPORT_SNAPSHOT, sync_queue.snapshot);
				read_expected_command(input, Commands::IMPORT_SNAPSHOT);
			}
			sync_queue.wait_at_barrier();

			// those databases that use locking instead of snapshot adoption can release the locks once
			// all the workers have started their transactions.
			if (leader) {
				send_command(output, Commands::UNHOLD_SNAPSHOT);
				read_expected_command(input, Commands::UNHOLD_SNAPSHOT);
			}
		} else {
			send_command(output, Commands::WITHOUT_SNAPSHOT);
			read_expected_command(input, Commands::WITHOUT_SNAPSHOT);
		}
	}

	void retrieve_database_schema() {
		// we could do this in all workers, but there's no need, and it'd waste a bit of traffic/time
		if (leader) {
			send_command(output, Commands::SCHEMA);
			read_expected_command(input, Commands::SCHEMA, database);
			client.convert_unsupported_database_schema(database);
			restrict_tables(database.tables);
			check_tables_usable();
		}
	}

	void compare_schema() {
		if (leader) {
			// get our schema
			Database to_database;
			client.populate_database_schema(to_database);
			restrict_tables(to_database.tables);

			// check they match, and if not, figure out what DDL we would need to run to fix the 'to' end's schema
			SchemaMatcher<DatabaseClient> matcher(client);

			matcher.match_schemas(database, to_database);

			if (matcher.statements.empty()) return;

			if (alter) {
				for (const string &statement : matcher.statements) {
					if (verbose) cout << statement << endl;
					if (statement.substr(0, 2) != "--") { // stop postgresql printing the comments to stderr
						client.execute(statement);
					}
				}
			} else {
				cerr << "The database schema doesn't match.  Use the --alter option if you would like to automatically apply the following schema changes:" << endl << endl;
				for (const string &statement : matcher.statements) {
					cerr << statement << ';' << endl;
				}
				cerr << endl;
				throw runtime_error("Database schema needs migration.");
			}
		}
	}

	void restrict_tables(Tables &tables) {
		Tables::iterator table = tables.begin();
		while (table != tables.end()) {
			if (ignore_tables.count(table->name) ||
				(!only_tables.empty() && !only_tables.count(table->name))) {
				table = tables.erase(table);
			} else {
				++table;
			}
		}
	}

	void check_tables_usable() {
		for (const Table &table : database.tables) {
			if (table.primary_key_columns.empty()) {
				throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
			}
			for (const Column &column : table.columns) {
				if (column.column_type == ColumnTypes::UNKN) {
					throw runtime_error("Don't know how to interpret type of " + table.name + '.' + column.name + " (" + column.db_type_def + ").  Please check https://github.com/willbryant/kitchen_sync/blob/master/SCHEMA.md.");
				}
			}
		}
	}

	void restrict_table_filters() {
		TableFilters::iterator it = table_filters.begin();
		while (it != table_filters.end()) {
			if (ignore_tables.count(it->first) ||
				(!only_tables.empty() && !only_tables.count(it->first))) {
				it = table_filters.erase(it);
			} else {
				++it;
			}
		}
	}

	void send_filters() {
		restrict_table_filters();

		if (!table_filters.empty()) {
			if (leader) {
				// we can give better error feedback to the user if we check the filters before we send them
				// to the N workers at the other end.  we work on a copy of the table schema object (taken
				// before calling restrict_tables) here because don't want to actually apply the filters to
				// our end (since then we'd ignore rows that we really need to change or clear).
				Tables copy(database.tables);
				apply_filters(table_filters, copy);
			}

			send_command(output, Commands::FILTERS, table_filters);
			read_expected_command(input, Commands::FILTERS);
		}
	}

	void enqueue_tables() {
		// queue up all the tables
		if (leader) {
			sync_queue.enqueue_tables_to_process(database.tables);
		}

		// wait for the leader to do that (a barrier here is slightly excessive as we don't care if the other
		// workers are ready to start work, but it's not worth having another synchronisation mechanism for this)
		sync_queue.wait_at_barrier();
	}

	void wait_for_finish() {
		// send a quit so the other end closes its output and terminates gracefully
		send_quit_command();

		// wait for all workers to finish their tables
		sync_queue.wait_at_barrier();
	}

	void commit() {
		time_t started = time(nullptr);

		client.commit_transaction();

		if (verbose && commit_level < CommitLevel::tables) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "committed in " << (now - started) << "s" << endl << flush;
		}
	}

	void rollback() {
		time_t started = time(nullptr);

		client.rollback_transaction();

		if (verbose) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "rolled back in " << (now - started) << "s" << endl << flush;
		}
	}

	void send_quit_command() {
		try {
			send_command(output, Commands::QUIT);
		} catch (const exception &e) {
			// we don't care if sending this command fails itself, we're already past the point where we could abort anyway
		}
	}

	Database &database;
	SyncQueue<DatabaseClient> &sync_queue;
	bool leader;
	int worker_number;
	VersionedFDWriteStream output_stream;
	FDReadStream input_stream;
	Unpacker<FDReadStream> input;
	Packer<VersionedFDWriteStream> output;
	DatabaseClient client;
	
	TableFilters table_filters;
	const set<string> ignore_tables;
	const set<string> only_tables;
	int verbose;
	bool progress;
	bool snapshot;
	bool alter;
	CommitLevel commit_level;
	bool structure_only;

	HashAlgorithm hash_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
	std::thread worker_thread;
};

template <typename DatabaseClient, typename... Options>
void sync_to(int num_workers, int startfd, const Options &...options) {
	Database database;
	SyncQueue<DatabaseClient> sync_queue(num_workers);
	vector<SyncToWorker<DatabaseClient>*> workers;

	workers.resize(num_workers);

	for (int worker = 0; worker < num_workers; worker++) {
		bool leader = (worker == 0);
		int read_from_descriptor = startfd + worker;
		int write_to_descriptor = startfd + worker + num_workers;
		workers[worker] = new SyncToWorker<DatabaseClient>(database, sync_queue, leader, worker, read_from_descriptor, write_to_descriptor, options...);
	}

	for (SyncToWorker<DatabaseClient>* worker : workers) delete worker;

	if (sync_queue.aborted) throw sync_error();
}
