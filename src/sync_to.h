#include "command.h"
#include "sync_algorithm.h"
#include "schema_functions.h"
#include "sync_queue.h"
#include "table_row_applier.h"
#include "fdstream.h"
#include <boost/algorithm/string.hpp>

using namespace std;

#define VERY_VERBOSE 2

set<string> split_list(const string &str) {
	set<string> result;
	boost::split(result, str, boost::is_any_of(", "));
	if (result.size() == 1 && *result.begin() == "") result.erase("");
	return result;
}

template <typename DatabaseClient>
struct SyncToWorker {
	SyncToWorker(
		SyncQueue &sync_queue,
		const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password,
		const char *ignore, const char *only, int read_from_descriptor, int write_to_descriptor, bool leader, int verbose, bool snapshot, bool partial, bool rollback_after):
			sync_queue(sync_queue),
			client(database_host, database_port, database_name, database_username, database_password),
			ignore_tables(split_list(ignore)),
			only_tables(split_list(only)),
			input_stream(read_from_descriptor),
			output_stream(write_to_descriptor),
			input(input_stream),
			output(output_stream),
			leader(leader),
			verbose(verbose),
			snapshot(snapshot),
			rollback_after(rollback_after),
			partial(partial),
			protocol_version(0),
			worker_thread(boost::ref(*this)) {
	}

	~SyncToWorker() {
		worker_thread.join();
	}

	void operator()() {
		try {
			negotiate_protocol();
			share_snapshot();

			client.start_write_transaction();

			compare_schema();
			enqueue_tables();
			sync_tables();

			if (rollback_after) {
				client.rollback_transaction();
			} else {
				client.commit_transaction();
			}

			// send a quit so the other end closes its output and terminates gracefully
			send_quit();
		} catch (const exception &e) {
			// make sure all other workers terminate promptly, and if we are the first to fail, output the error
			if (sync_queue.abort()) {
				cerr << e.what() << endl;
			}

			// if the --partial option was used, try to commit the changes we've made, but ignore any errors
			if (partial) {
				try { client.commit_transaction(); } catch (...) {}
			}
		}

		// eagerly close the streams so that the SSH session terminates promptly on aborts
		output_stream.close();
	}

	void negotiate_protocol() {
		const int PROTOCOL_VERSION_SUPPORTED = 1;

		// tell the other end what version of the protocol we can speak, and have them tell us which version we're able to converse in
		send_command(output, "protocol", PROTOCOL_VERSION_SUPPORTED);

		// read the response to the protocol_version command that the output thread sends when it starts
		// this is currently unused, but the command's semantics need to be in place for it to be useful in the future...
		input >> protocol_version;
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
				send_command(output, "export_snapshot");
				sync_queue.snapshot = input.next<string>();
			}
			sync_queue.wait_at_barrier();

			// as soon as it has responded, adopt the snapshot/start the transaction in each of the other workers.
			if (!leader) {
				send_command(output, "import_snapshot", sync_queue.snapshot);
				input.next_nil(); // arbitrary; sent by the other end once they've started their transaction
			}
			sync_queue.wait_at_barrier();

			// those databases that use locking instead of snapshot adoption can release the locks once
			// all the workers have started their transactions.
			if (leader) {
				send_command(output, "unhold_snapshot");
				input.next_nil(); // similarly arbitrary
			}
		} else {
			send_command(output, "without_snapshot");
			input.next_nil(); // similarly arbitrary
		}
	}

	void compare_schema() {
		// we could do this in all workers, but there's no need, and it'd waste a bit of traffic/time
		if (leader) {
			// get its schema
			send_command(output, "schema");

			// read the response to the schema command that the output thread sends when it starts
			Database from_database;
			input >> from_database;

			// check they match
			check_schema_match(from_database, client.database_schema(), ignore_tables, only_tables);
		}
	}

	void enqueue_tables() {
		// queue up all the tables
		if (leader) {
			sync_queue.enqueue(client.database_schema().tables, ignore_tables, only_tables);
		}

		// wait for the leader to do that (a barrier here is slightly excessive as we don't care if the other
		// workers are ready to start work, but it's not worth having another synchronisation mechanism for this)
		sync_queue.wait_at_barrier();
	}

	void sync_tables() {
		client.disable_referential_integrity();

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

		// wait for all workers to finish their tables
		sync_queue.wait_at_barrier();
		client.enable_referential_integrity();
	}

	void sync_table(const Table &table) {
		TableRowApplier<DatabaseClient> row_applier(client, table);
		ColumnValues prev_key;
		ColumnValues last_key;
		string hash;
		size_t hash_commands = 0;
		size_t rows_commands = 0;
		time_t started = time(NULL);

		if (verbose) {
			boost::unique_lock<boost::mutex> lock(sync_queue.mutex);
			cout << "starting " << table.name << endl << flush;
		}

		// start with 1 row of the table; find its hash, and set its key as the key range
		// if there are no rows, this will return a blank hash, which is handled in the loop
		find_hash_of_next_range(client, table, 1, prev_key, last_key, hash);

		while (true) {
			ColumnValues matched_up_to_key;

			if (hash.empty()) {
				// ask the other end to send their rows in this range.
				if (verbose >= VERY_VERBOSE) cout << "-> rows " << table.name << ' ' << non_binary_string_values_list(prev_key) << ' ' << non_binary_string_values_list(last_key) << endl;
				send_command(output, "rows", table.name, prev_key, last_key);

			} else {
				// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
				if (verbose >= VERY_VERBOSE) cout << "-> hash " << table.name << ' ' << non_binary_string_values_list(prev_key) << ' ' << non_binary_string_values_list(last_key) << endl;
				send_command(output, "hash", table.name, prev_key, last_key, hash);

				// unlike 'rows', this is an independent command (which implies the last hash command was successfully matched), so count it
				hash_commands++;
			}

			sync_queue.check_aborted(); // rather than wait until the end of the current table; we do it here since it's likely we'll have no work to do for a short while

			Command command;
			input >> command;

			if (command.name == "hash") {
				if (command.argument<string>(0) != table.name) throw command_error("Received response on table " + command.argument<string>(0) + " but request was for " + table.name);

				// they've sent us back a hash for a set of rows, which will happen if:
				// - the last hash we sent them matched, and so they've moved on to the next set of rows; or
				// - the last hash we sent them didn't match, so they've reduced the key range and sent us back
				//   the hash for a smaller set of rows (but not so small that they sent back the data instead)
				// we don't need to know which case it is; simply loop around and carry on
				prev_key = command.argument<ColumnValues>(1);
				last_key = command.argument<ColumnValues>(2);
				hash     = command.argument<string>(3);
				if (verbose >= VERY_VERBOSE) cout << "<- hash " << table.name << ' ' << non_binary_string_values_list(prev_key) << ' ' << non_binary_string_values_list(last_key) << endl;

				hash_commands++;

				check_hash_and_choose_next_range(client, table, prev_key, last_key, hash);

			} else if (command.name == "rows") {
				if (command.argument<string>(0) != table.name) throw command_error("Received response on table " + command.argument<string>(0) + " but request was for " + table.name);

				// we're being sent a range of rows; apply them to our end.  we do this in-context to
				// provide flow control - if we buffered and used a separate apply thread, we would
				// bloat up if this end couldn't write to disk as quickly as the other end sent data.
				prev_key = command.argument<ColumnValues>(1);
				last_key = command.argument<ColumnValues>(2);
				if (verbose >= VERY_VERBOSE) cout << "<- rows " << table.name << ' ' << non_binary_string_values_list(prev_key) << ' ' << non_binary_string_values_list(last_key) << endl;

				rows_commands++;

				size_t rows_in_range = row_applier.stream_from_input(input, prev_key, last_key);

				// if the range extends to the end of their table, that means we're done with this table
				if (last_key.empty()) {
					// apply any pending updates
					row_applier.apply();
					if (verbose) {
						time_t now = time(NULL);
						boost::unique_lock<boost::mutex> lock(sync_queue.mutex);
						cout << "finished " << table.name << " in " << (now - started) << "s using " << hash_commands << " hash commands and " << rows_commands << " rows commands changing " << row_applier.rows_changed << " rows" << endl << flush;
					}
					return;
				}

				// if it doesn't, that means they have more rows after these ones, so more work to do;
				// since we failed to match last time, don't increase the row count.
				prev_key = last_key;
				if (!rows_in_range) rows_in_range = 1;
				find_hash_of_next_range(client, table, rows_in_range, prev_key, last_key, hash);

			} else {
				throw command_error("Unknown command " + command.name);
			}
		}
	}

	void send_quit() {
		try {
			send_command(output, "quit");
		} catch (const exception &e) {
			// we don't care if sending this command fails itself, we're already past the point where we could abort anyway
		}
	}

	SyncQueue &sync_queue;
	DatabaseClient client;
	set<string> ignore_tables;
	set<string> only_tables;
	FDWriteStream output_stream;
	FDReadStream input_stream;
	Unpacker<FDReadStream> input;
	Packer<FDWriteStream> output;
	bool leader;
	int verbose;
	bool snapshot;
	bool partial;
	bool rollback_after;
	int protocol_version;
	boost::thread worker_thread;
};

template <typename DatabaseClient>
void sync_to(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, const char *ignore, const char *only, int num_workers, int startfd, int verbose, bool snapshot, bool partial, bool rollback_after) {
	SyncQueue sync_queue(num_workers);
	vector<SyncToWorker<DatabaseClient>*> workers;

	workers.resize(num_workers);

	for (int worker = 0; worker < num_workers; worker++) {
		bool leader = (worker == 0);
		int read_from_descriptor = startfd + worker;
		int write_to_descriptor = startfd + worker + num_workers;
		workers[worker] = new SyncToWorker<DatabaseClient>(sync_queue, database_host, database_port, database_name, database_username, database_password, ignore, only, read_from_descriptor, write_to_descriptor, leader, verbose, snapshot, partial, rollback_after);
	}

	for (typename vector<SyncToWorker<DatabaseClient>*>::const_iterator it = workers.begin(); it != workers.end(); ++it) delete *it;

	if (sync_queue.aborted) throw sync_error();
}
