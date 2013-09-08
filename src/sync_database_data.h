#include "sync_to_worker.h"
#include "table_row_applier.h"

template <typename DatabaseClient, typename InputStream>
void handle_rows_response(SyncWorkQueue &work_queue, DatabaseClient &client, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, Unpacker<InputStream> &input) {
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

template <typename DatabaseClient>
void handle_hash_response(SyncWorkQueue &work_queue, DatabaseClient &client, const string &table_name, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
	const Table &table(client.table_by_name(table_name));
	work_queue.enqueue(table, prev_key, last_key, hash);
}

template <typename DatabaseClient, typename InputStream, typename OutputStream>
void sync_database_data(
	DatabaseClient &client, DatabaseClient &read_client, Unpacker<InputStream> &input, Packer<OutputStream> &output, const Database &database) {

	SyncWorkQueue work_queue(database.tables);

	// single worker (but pipelined, with one writer thread and us as reader thread) for now; we'll need to be able to start up additional database clients and remote endpoints with the same snapshot to fully parallelize
	SyncToWorker<DatabaseClient, OutputStream> writer_worker(work_queue, read_client, output);

	client.disable_referential_integrity();

	while (work_queue.tables_left > 0) { // our thread is the only one that mutates tables_left at the mo, so we don't need to lock
		Command command;
		input >> command;

		if (command.name == "rows") {
			string     table_name(command.argument<string>(0));
			ColumnValues prev_key(command.argument<ColumnValues>(1));
			ColumnValues last_key(command.argument<ColumnValues>(2));
			handle_rows_response(work_queue, client, table_name, prev_key, last_key, input);

		} else if (command.name == "hash") {
			string     table_name(command.argument<string>(0));
			ColumnValues prev_key(command.argument<ColumnValues>(1));
			ColumnValues last_key(command.argument<ColumnValues>(2));
			string           hash(command.argument<string>(3));
			handle_hash_response(work_queue, client, table_name, prev_key, last_key, hash);

		} else {
			cerr << "received unknown response: " << command.name << endl;
			throw command_error("Unknown command " + command.name);
		}
	}

	client.enable_referential_integrity();
}
