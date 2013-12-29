#include <stdexcept>
#include <unistd.h>

#include "schema.h"
#include "schema_functions.h"
#include "command.h"
#include "sync_work_queue.h"
#include "sync_algorithm.h"
#include "table_row_applier.h"
#include "fdstream.h"

using namespace std;

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename DatabaseClient, typename OutputStream>
struct SyncToOutputWorker {
	SyncToOutputWorker(
		SyncWorkQueue &work_queue,
		const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password,
		int write_to_descriptor, bool leader):
			work_queue(work_queue),
			client(database_host, database_port, database_name, database_username, database_password, true /* readonly */, false /* no snapshot - don't take gap locks */),
			output_stream(write_to_descriptor), output(output_stream), leader(leader),
			worker_thread(boost::ref(*this)) {
	}

	~SyncToOutputWorker() {
		worker_thread.join();
	}

	void operator()() {
		const int PROTOCOL_VERSION_SUPPORTED = 1;

		try {
			// tell the other end what version of the protocol we can speak, and have them tell us which version we're able to converse in
			send_command(output, "protocol", PROTOCOL_VERSION_SUPPORTED);

			// start syncing table data
			if (leader) {
				// FUTURE: export the remote snapshot

				// get its schema
				send_command(output, "schema");
			} else {
				// FUTURE: adopt the exported remote snapshot
			}

			WorkTask task;

			while (true) {
				// grab the next task off the queue (blocking if it's empty, quitting if there's no more tables to work on)
				task = work_queue.pop();
				if (!task.table) break;

				const Table &table(*task.table);
				ColumnValues matched_up_to_key;
				size_t rows_to_hash = check_hash_and_choose_next_range(client, table, task.prev_key, task.last_key, task.hash, matched_up_to_key);

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
			}
		} catch (const exception &e) {
			cerr << e.what() << endl;
			work_queue.abort(); // abort all other output threads
		}

		try {
			// send a quit so the other end closes its output, needed so our end's input thread terminates; note we do this both for normal completion and also errors
			send_command(output, "quit");
		} catch (const exception &e) {
			if (!work_queue.aborted) cerr << e.what() << endl;
			work_queue.abort();
		}
	}

	SyncWorkQueue &work_queue;
	DatabaseClient client;
	OutputStream output_stream;
	Packer<OutputStream> output;
	bool leader;
	boost::thread worker_thread;
};

template <typename DatabaseClient, typename InputStream>
struct SyncToInputWorker {
	SyncToInputWorker(
		SyncWorkQueue &work_queue,
		const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password,
		int read_from_descriptor, bool leader):
			work_queue(work_queue),
			client(database_host, database_port, database_name, database_username, database_password, false /* not readonly */, false /* no snapshot - don't take gap locks */),
			input_stream(read_from_descriptor), input(input_stream), leader(leader),
			worker_thread(boost::ref(*this)) {
	}

	~SyncToInputWorker() {
		worker_thread.join();
	}

	void operator()() {
		try {
			// read the response to the protocol_version command that the output thread sends when it starts
			// this is currently unused, but the command's semantics need to be in place for it to be useful in the future...
			input >> protocol_version;

			if (leader) {
				// FUTURE: read the remote snapshot

				// read the response to the schema command that the output thread sends when it starts
				Database from_database;
				input >> from_database;

				// check they match
				check_schema_match(from_database, client.database_schema());

				// queue up all the tables, and signal the non-leaders that it's time to start
				work_queue.enqueue(client.database_schema().tables);
			} else {
				work_queue.wait_until_started();
				// FUTURE: adopt the exported remote snapshot
			}

			client.disable_referential_integrity();

			while (true) {
				Command command;

				try {
					input >> command;
				} catch (const stream_closed_error &e) {
					if (!work_queue.check_if_finished_all_tables()) throw;
					client.enable_referential_integrity();
					client.commit_transaction();
					break;
				}

				if (command.name == "rows") {
					string     table_name(command.argument<string>(0));
					ColumnValues prev_key(command.argument<ColumnValues>(1));
					ColumnValues last_key(command.argument<ColumnValues>(2));
					handle_rows_response(table_name, prev_key, last_key);

				} else if (command.name == "hash") {
					string     table_name(command.argument<string>(0));
					ColumnValues prev_key(command.argument<ColumnValues>(1));
					ColumnValues last_key(command.argument<ColumnValues>(2));
					string           hash(command.argument<string>(3));
					handle_hash_response(table_name, prev_key, last_key, hash);

				} else {
					cerr << "received unknown response: " << command.name << endl;
					throw command_error("Unknown command " + command.name);
				}
			}
		} catch (const exception &e) {
			cerr << e.what() << endl;
			work_queue.abort(); // abort all other output threads
		}
	}

	void handle_rows_response(const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		const Table &table(client.table_by_name(table_name));
		TableRowApplier<DatabaseClient, InputStream> applier(client, input, table, prev_key, last_key);

		if (last_key.empty()) {
			// if the range extends to the end of the table, that means we're done with that table;
			// update our count of completed tables so we know when we've finished syncing.
			work_queue.finished_table();

		} else {
			// if it doesn't, that means we have more work to do; since we failed to match last time, don't
			// increase the row count.  queue up (at the front of the queue so that we go deep before broad).
			work_queue.enqueue(table, last_key, ColumnValues(), string());
		}
	}

	void handle_hash_response(const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		const Table &table(client.table_by_name(table_name));
		work_queue.enqueue(table, prev_key, last_key, hash);
	}

	SyncWorkQueue &work_queue;
	DatabaseClient client;
	InputStream input_stream;
	Unpacker<InputStream> input;
	bool leader;
	int protocol_version;
	boost::thread worker_thread;
};

template <typename DatabaseClient>
void sync_to(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password, int num_workers, int startfd) {
	SyncWorkQueue work_queue;
	vector< SyncToInputWorker<DatabaseClient, FDReadStream>* > input_workers;
	vector< SyncToOutputWorker<DatabaseClient, FDWriteStream>* > output_workers;

	output_workers.resize(num_workers);
	input_workers.resize(num_workers);

	for (int worker = 0; worker < num_workers; worker++) {
		bool leader = (worker == 0);
		int read_from_descriptor = startfd + worker;
		int write_to_descriptor = startfd + worker + num_workers;
		input_workers[worker] = new SyncToInputWorker<DatabaseClient, FDReadStream>(work_queue, database_host, database_port, database_name, database_username, database_password, read_from_descriptor, leader);
		output_workers[worker] = new SyncToOutputWorker<DatabaseClient, FDWriteStream>(work_queue, database_host, database_port, database_name, database_username, database_password, write_to_descriptor, leader);
	}

	for (typename vector< SyncToInputWorker<DatabaseClient, FDReadStream>* >::const_iterator it = input_workers.begin(); it != input_workers.end(); ++it) delete *it;
	for (typename vector< SyncToOutputWorker<DatabaseClient, FDWriteStream>* >::const_iterator it = output_workers.begin(); it != output_workers.end(); ++it) delete *it;

	if (work_queue.aborted) throw sync_error();
}
