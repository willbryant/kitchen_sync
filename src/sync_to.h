#include <boost/algorithm/string.hpp>
#include <thread>

#include "command.h"
#include "commit_level.h"
#include "hash_algorithm.h"
#include "sync_algorithm.h"
#include "schema_functions.h"
#include "schema_matcher.h"
#include "sync_queue.h"
#include "row_range_applier.h"
#include "reset_table_sequences.h"
#include "fdstream.h"
#include "defaults.h"

using namespace std;

#define VERY_VERBOSE 2

template <typename DatabaseClient>
struct SyncToWorker {
	SyncToWorker(
		Database &database, SyncQueue &sync_queue, bool leader, int read_from_descriptor, int write_to_descriptor,
		const string &database_host, const string &database_port, const string &database_name, const string &database_username, const string &database_password,
		const string &set_variables, const set<string> &ignore_tables, const set<string> &only_tables,
		int verbose, bool progress, bool snapshot, bool alter, CommitLevel commit_level, HashAlgorithm hash_algorithm,
		bool structure_only) :
			database(database),
			sync_queue(sync_queue),
			leader(leader),
			input_stream(read_from_descriptor),
			output_stream(write_to_descriptor),
			input(input_stream),
			output(output_stream),
			client(database_host, database_port, database_name, database_username, database_password),
			ignore_tables(ignore_tables),
			only_tables(only_tables),
			verbose(verbose),
			progress(progress),
			snapshot(snapshot),
			alter(alter),
			commit_level(commit_level),
			configured_hash_algorithm(hash_algorithm),
			structure_only(structure_only),
			protocol_version(0),
			target_minimum_block_size(1),
			target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE),
			sync_algorithm(*this, client, hash_algorithm),
			worker_thread(std::ref(*this)) {
		if (!set_variables.empty()) {
			client.execute("SET " + set_variables);
		}
	}

	~SyncToWorker() {
		worker_thread.join();
	}

	void operator()() {
		try {
			negotiate_protocol();
			negotiate_target_minimum_block_size();
			negotiate_hash_algorithm();

			share_snapshot();
			retrieve_database_schema();
			compare_schema();

			client.start_write_transaction();

			enqueue_tables();

			client.disable_referential_integrity();
			if (!structure_only) {
				sync_tables();
			}

			// send a quit so the other end closes its output and terminates gracefully
			send_quit_command();

			// wait for all workers to finish their tables
			sync_queue.wait_at_barrier();
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

		// eagerly close the streams so that the SSH session terminates promptly on aborts
		output_stream.close();
	}

	void negotiate_protocol() {
		const int EARLIEST_PROTOCOL_VERSION_SUPPORTED = 6;
		const int LATEST_PROTOCOL_VERSION_SUPPORTED = 6;

		// tell the other end what version of the protocol we can speak, and have them tell us which version we're able to converse in
		send_command(output, Commands::PROTOCOL, LATEST_PROTOCOL_VERSION_SUPPORTED);

		// read the response to the protocol_version command that the output thread sends when it starts
		// this is currently unused, but the command's semantics need to be in place for it to be useful in the future...
		read_expected_command(input, Commands::PROTOCOL, protocol_version);

		if (protocol_version < EARLIEST_PROTOCOL_VERSION_SUPPORTED || protocol_version > LATEST_PROTOCOL_VERSION_SUPPORTED) {
			throw runtime_error("Sorry, the other end doesn't support a compatible protocol version");
		}
	}

	void negotiate_target_minimum_block_size() {
		send_command(output, Commands::TARGET_BLOCK_SIZE, DEFAULT_MINIMUM_BLOCK_SIZE);

		// the real app always accepts the block size we request, but the test suite uses smaller block sizes to make it easier to set up different scenarios
		read_expected_command(input, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size);
	}

	void negotiate_hash_algorithm() {
		send_command(output, Commands::HASH_ALGORITHM, sync_algorithm.hash_algorithm);
		read_expected_command(input, Commands::HASH_ALGORITHM, sync_algorithm.hash_algorithm);
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
			filter_tables(database.tables);
			check_tables_usable(database.tables);
		}
	}

	void compare_schema() {
		if (leader) {
			// get our schema
			Database to_database;
			client.populate_database_schema(to_database);
			filter_tables(to_database.tables);

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
					cerr << statement << endl;
				}
				cerr << endl;
				throw runtime_error("Database schema needs migration");
			}
		}
	}

	void filter_tables(Tables &tables) {
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

	void check_tables_usable(Tables &tables) {
		for (Table &table : tables) {
			if (table.primary_key_columns.empty()) {
				throw runtime_error("Couldn't find a primary or non-nullable unique key on table " + table.name);
			}
		}
	}

	void enqueue_tables() {
		// queue up all the tables
		if (leader) {
			sync_queue.enqueue(database.tables);
		}

		// wait for the leader to do that (a barrier here is slightly excessive as we don't care if the other
		// workers are ready to start work, but it's not worth having another synchronisation mechanism for this)
		sync_queue.wait_at_barrier();
	}

	void sync_tables() {
		while (true) {
			// grab the next table to work on from the queue (blocking if it's empty)
			const Table *table = sync_queue.pop();

			// quit if there's no more tables to process
			if (!table) break;

			// synchronize that table (unfortunately we can't share this job with other workers because next-key
			// locking is used for unique key indexes to enforce the uniqueness constraint, so we can't share
			// write traffic to the database across connections, which makes it somewhat futile to try and farm the
			// read work out since that needs to see changes made to satisfy unique indexes earlier in the table)
			sync_table(*table);
		}
	}

	void sync_table(const Table &table) {
		RowReplacer<DatabaseClient> row_replacer(client, table, commit_level >= CommitLevel::often,
			[&] { if (progress) { cout << "." << flush; } });
		size_t hash_commands = 0;
		size_t rows_commands = 0;
		time_t started = time(nullptr);
		bool finished = false;

		if (verbose) {
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "starting " << table.name << endl << flush;
		}

		send_command(output, Commands::OPEN, table.name);

		while (!finished) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table; this is a good place to do it since it's likely we'll have no work to do for a short while

			if (progress) {
				cout << "." << flush; // simple progress meter
			}

			verb_t verb;
			input >> verb;

			switch (verb) {
				case Commands::HASH_NEXT:
					handle_hash_next_command(table);
					hash_commands++;
					break;

				case Commands::HASH_FAIL:
					handle_hash_fail_command(table);
					hash_commands++;
					break;

				case Commands::ROWS:
					finished = handle_rows_command(table, row_replacer);
					rows_commands++;
					break;

				case Commands::ROWS_AND_HASH_NEXT:
					handle_rows_and_hash_next_command(table, row_replacer);
					hash_commands++;
					rows_commands++;
					break;

				case Commands::ROWS_AND_HASH_FAIL:
					handle_rows_and_hash_fail_command(table, row_replacer);
					hash_commands++;
					rows_commands++;
					break;

				default:
					throw command_error("Unknown command " + to_string(verb));
			}
		}

		// make sure all pending updates have been applied
		row_replacer.apply();

		// reset sequences on those databases that don't automatically bump the high-water mark for inserts
		ResetTableSequences<DatabaseClient>::execute(client, table);

		if (verbose) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "finished " << table.name << " in " << (now - started) << "s using " << hash_commands << " hash commands and " << rows_commands << " rows commands changing " << row_replacer.rows_changed << " rows" << endl << flush;
		}

		if (commit_level >= CommitLevel::tables) {
			commit();
			client.start_write_transaction();
		}
	}

	void handle_hash_next_command(const Table &table) {
		// the last hash we sent them matched, and so they've moved on to the next set of rows and sent us the hash
		ColumnValues prev_key, last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, hash);
		if (verbose >= VERY_VERBOSE) cout << "-> hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		// after each hash command received it's our turn to send the next command
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, prev_key, last_key, nullptr, hash, target_minimum_block_size, target_maximum_block_size);
	}

	void handle_hash_fail_command(const Table &table) {
		// the last hash we sent them didn't match, so they've reduced the key range and sent us back
		// the hash for a smaller set of rows (but not so small that they sent back the data instead)
		ColumnValues prev_key, last_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, failed_last_key, hash);
		if (verbose >= VERY_VERBOSE) cout << "-> hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;

		// after each hash command received it's our turn to send the next command
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, prev_key, last_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
	}

	bool handle_rows_command(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		ColumnValues prev_key, last_key;
		read_array(input, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (verbose >= VERY_VERBOSE) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);

		// if the range extends to the end of their table, that means we're done with this table;
		// otherwise, rows commands are immediately followed by another command
		return (last_key.empty());
	}

	void handle_rows_and_hash_next_command(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// combo of the above ROWS and HASH_NEXT commands
		ColumnValues prev_key, last_key, next_key;
		string hash;
		read_array(input, prev_key, last_key, next_key, hash); // the first array gives the range arguments and hash, which is followed by one array for each row
		if (verbose >= VERY_VERBOSE) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (verbose >= VERY_VERBOSE) cout << "-> hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << endl;

		// after each hash command received it's our turn to send the next command; we check
		// the hash and send the command *before* we stream in the rows that we're being sent
		// with this command as a simple form of pipelining - our next hash is going back
		// over the network at the same time as we are receiving rows.  we need to be able to
		// fit the command we send back in the kernel send buffer to guarantee there is no
		// deadlock; it's never been smaller than a page on any supported OS, and has been
		// defaulted to much larger values for some years.
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, last_key, next_key, nullptr, hash, target_minimum_block_size, target_maximum_block_size);
		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
		// nb. it's implied last_key is not [], as we would have been sent back a plain rows command for the combined range if that was needed
	}

	void handle_rows_and_hash_fail_command(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// combo of the above ROWS and HASH_FAIL commands
		ColumnValues prev_key, last_key, next_key, failed_last_key;
		string hash;
		read_array(input, prev_key, last_key, next_key, failed_last_key, hash); // the first array gives the range arguments, which is followed by one array for each row
		if (verbose >= VERY_VERBOSE) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (verbose >= VERY_VERBOSE) cout << "-> hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;

		// same pipelining as the previous case
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, last_key, next_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
	}

	inline void send_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		if (verbose >= VERY_VERBOSE) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;
		send_command(output, Commands::HASH_NEXT, prev_key, last_key, hash);
	}

	inline void send_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &failed_last_key, const string &hash) {
		if (verbose >= VERY_VERBOSE) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;
		send_command(output, Commands::HASH_FAIL, prev_key, last_key, failed_last_key, hash);
	}

	inline void send_rows_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		if (verbose >= VERY_VERBOSE) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;
		send_command(output, Commands::ROWS, prev_key, last_key);
	}

	inline void send_rows_and_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const string &hash) {
		if (verbose >= VERY_VERBOSE) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (verbose >= VERY_VERBOSE) cout << "<- hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << endl;
		send_command(output, Commands::ROWS_AND_HASH_NEXT, prev_key, last_key, next_key, hash);
	}

	inline void send_rows_and_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const ColumnValues &failed_last_key, const string &hash) {
		if (verbose >= VERY_VERBOSE) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (verbose >= VERY_VERBOSE) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;
		send_command(output, Commands::ROWS_AND_HASH_FAIL, prev_key, last_key, next_key, failed_last_key, hash);
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
	SyncQueue &sync_queue;
	bool leader;
	FDWriteStream output_stream;
	FDReadStream input_stream;
	Unpacker<FDReadStream> input;
	Packer<FDWriteStream> output;
	DatabaseClient client;
	
	const set<string> ignore_tables;
	const set<string> only_tables;
	int verbose;
	bool progress;
	bool snapshot;
	bool alter;
	CommitLevel commit_level;
	bool structure_only;

	int protocol_version;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
	HashAlgorithm configured_hash_algorithm;
	SyncAlgorithm<SyncToWorker, DatabaseClient> sync_algorithm;
	std::thread worker_thread;
};

template <typename DatabaseClient, typename... Options>
void sync_to(int num_workers, int startfd, const Options &...options) {
	Database database;
	SyncQueue sync_queue(num_workers);
	vector<SyncToWorker<DatabaseClient>*> workers;

	workers.resize(num_workers);

	for (int worker = 0; worker < num_workers; worker++) {
		bool leader = (worker == 0);
		int read_from_descriptor = startfd + worker;
		int write_to_descriptor = startfd + worker + num_workers;
		workers[worker] = new SyncToWorker<DatabaseClient>(database, sync_queue, leader, read_from_descriptor, write_to_descriptor, options...);
	}

	for (SyncToWorker<DatabaseClient>* worker : workers) delete worker;

	if (sync_queue.aborted) throw sync_error();
}
