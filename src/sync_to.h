#include <stdexcept>
#include <unistd.h>

#include "schema.h"
#include "schema_functions.h"
#include "command.h"
#include "sync_table_queue.h"
#include "sync_algorithm.h"
#include "table_row_applier.h"
#include "fdstream.h"

using namespace std;

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename DatabaseClient>
struct SyncToWorker {
	SyncToWorker(
		SyncTableQueue &table_queue,
		const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password,
		int read_from_descriptor, int write_to_descriptor, bool leader):
			table_queue(table_queue),
			client(database_host, database_port, database_name, database_username, database_password, false /* not readonly */, false /* no snapshot - don't take gap locks */),
			input_stream(read_from_descriptor), input(input_stream), 
			output_stream(write_to_descriptor), output(output_stream),
			leader(leader),
			protocol_version(0),
			worker_thread(boost::ref(*this)) {
	}

	~SyncToWorker() {
		worker_thread.join();
	}

	void operator()() {
		try {
			negotiate_protocol();
			// FUTURE: export the remote snapshot from the leader to the other workers
			compare_schema();
			enqueue_tables();

			client.disable_referential_integrity();

			while (true) {
				// grab the next table to work on from the queue (blocking if it's empty)
				const Table *table = table_queue.pop();

				// quit if there's no more tables to process
				if (!table) break;

				// synchronize that table (unfortunately we can't share this job with other threads because next-key
				// locking is used for unique key indexes to enforce the uniqueness constraint, so we can't share
				// write traffic to the database, which makes it somewhat futile to try and farm the read work out)
				sync_table(*table);

				// update our count of completed tables so we know when we've finished syncing
				table_queue.finished_table();
			}

			// if any of the workers aborted, exit without committing our changes, otherwise commit and return
			if (table_queue.check_if_finished_all_tables()) {
				client.enable_referential_integrity();
				client.commit_transaction();
			}
		} catch (const exception &e) {
			cerr << e.what() << endl;
			table_queue.abort(); // abort all other output threads
		}

		// send a quit so the other end closes its output and terminates gracefully; note we do this both for normal completion and also errors
		send_quit();
	}

	void negotiate_protocol() {
		const int PROTOCOL_VERSION_SUPPORTED = 1;

		// tell the other end what version of the protocol we can speak, and have them tell us which version we're able to converse in
		send_command(output, "protocol", PROTOCOL_VERSION_SUPPORTED);

		// read the response to the protocol_version command that the output thread sends when it starts
		// this is currently unused, but the command's semantics need to be in place for it to be useful in the future...
		input >> protocol_version;
	}

	void compare_schema() {
		// we could do this in all workers, and it wouldn't be a bad idea to, but (especially with mysql) getting schema information
		// is actually relatively expensive (particularly on older servers without SSDs, or on servers with many databases or tables)
		if (leader) {
			// get its schema
			send_command(output, "schema");

			// read the response to the schema command that the output thread sends when it starts
			Database from_database;
			input >> from_database;

			// check they match
			check_schema_match(from_database, client.database_schema());
		}
	}

	void enqueue_tables() {
		if (leader) {
			// queue up all the tables, and signal the non-leaders that it's time to start
			table_queue.enqueue(client.database_schema().tables);
		} else {
			// wait for the leader to queue the tables
			table_queue.wait_until_started();
		}
	}

	void sync_table(const Table &table) {
		ColumnValues prev_key;
		ColumnValues last_key;
		string hash;

		while (true) {
			ColumnValues matched_up_to_key;
			size_t rows_to_hash = check_hash_and_choose_next_range(client, table, prev_key, last_key, hash, matched_up_to_key);

			// calculate our hash of the next rows_to_hash rows
			RowHasherAndLastKey<typename DatabaseClient::RowType> hasher_for_our_rows(table.primary_key_columns);
			if (rows_to_hash) {
				client.retrieve_rows(table, matched_up_to_key, rows_to_hash, hasher_for_our_rows);
			}

			if (hasher_for_our_rows.row_count == 0) {
				// rows don't match, and there's only one or no rows in that range at our end, so ask the other end to send theirs
				send_command(output, "rows", table.name, matched_up_to_key, hasher_for_our_rows.last_key /* empty, meaning to the end of the table */);

			} else {
				// tell the other end to check its hash of the same rows, using key ranges rather than a count to improve the chances of a match.
				send_command(output, "hash", table.name, matched_up_to_key, hasher_for_our_rows.last_key, hasher_for_our_rows.finish());
			}

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

			} else if (command.name == "rows") {
				if (command.argument<string>(0) != table.name) throw command_error("Received response on table " + command.argument<string>(0) + " but request was for " + table.name);

				// we're being sent a range of rows; apply them to our end.  we do this in-context to
				// provide flow control - if we buffered and used a separate apply thread, we would
				// bloat up if this end couldn't write to disk as quickly as the other end sent data.
				prev_key = command.argument<ColumnValues>(1);
				last_key = command.argument<ColumnValues>(2);

				TableRowApplier<DatabaseClient, FDReadStream> applier(client, input, table, prev_key, last_key);

				// if the range extends to the end of their table, that means we're done with that table
				if (last_key.empty()) return;

				// if it doesn't, that means they have more rows after these ones, so more work to do;
				// since we failed to match last time, don't increase the row count.
				prev_key = last_key;
				last_key = ColumnValues();
				hash     = string();

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

	SyncTableQueue &table_queue;
	DatabaseClient client;
	FDWriteStream output_stream;
	FDReadStream input_stream;
	Unpacker<FDReadStream> input;
	Packer<FDWriteStream> output;
	bool leader;
	int protocol_version;
	boost::thread worker_thread;
};

template <typename DatabaseClient>
void sync_to(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int num_workers, int startfd) {
	SyncTableQueue table_queue;
	vector<SyncToWorker<DatabaseClient>*> workers;

	workers.resize(num_workers);

	for (int worker = 0; worker < num_workers; worker++) {
		bool leader = (worker == 0);
		int read_from_descriptor = startfd + worker;
		int write_to_descriptor = startfd + worker + num_workers;
		workers[worker] = new SyncToWorker<DatabaseClient>(table_queue, database_host, database_port, database_name, database_username, database_password, read_from_descriptor, write_to_descriptor, leader);
	}

	for (typename vector<SyncToWorker<DatabaseClient>*>::const_iterator it = workers.begin(); it != workers.end(); ++it) delete *it;

	if (table_queue.aborted) throw sync_error();
}
