#include "command.h"
#include "sync_work_queue.h"
#include "sync_algorithm.h"

template <typename DatabaseClient>
struct SyncToWorker {
	SyncToWorker(SyncWorkQueue &work_queue, DatabaseClient &client, Packer<ostream> &output): work_queue(work_queue), client(client), output(output), worker_thread(boost::ref(*this)) {}

	~SyncToWorker() {
		worker_thread.join();
	}

	void operator()() {
		WorkTask task;

		while (true) {
			// grab the next task off the queue (blocking if it's empty), and quit if we've been told all tables are done
			task = work_queue.pop();
			if (!task.table) return;
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
	}

	SyncWorkQueue &work_queue;
	DatabaseClient &client;
	Packer<ostream> &output;
	boost::thread worker_thread;
};
