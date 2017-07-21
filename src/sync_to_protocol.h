#include "timestamp.h"

struct HashResult {
	HashResult(ColumnValues prev_key, ColumnValues last_key, size_t estimated_rows_in_range, size_t our_row_count, size_t our_size, string our_hash, ColumnValues our_last_key):
		prev_key(prev_key), last_key(last_key), estimated_rows_in_range(estimated_rows_in_range), our_row_count(our_row_count), our_size(our_size), our_hash(our_hash), our_last_key(our_last_key) {}

	ColumnValues prev_key;
	ColumnValues last_key;
	size_t estimated_rows_in_range;

	size_t our_row_count;
	size_t our_size;
	string our_hash;
	ColumnValues our_last_key;
};

typedef tuple<ColumnValues, ColumnValues> KeyRange;
typedef tuple<ColumnValues, ColumnValues, size_t, size_t> KeyRangeWithRowCount;
const size_t UNKNOWN_ROW_COUNT = numeric_limits<size_t>::max();

struct TableJob {
	TableJob(const Table &table): table(table) {}

	const Table &table;
	deque<KeyRange> ranges_to_retrieve;
	deque<KeyRangeWithRowCount> ranges_to_check;
	deque<HashResult> ranges_hashed;
};

template <class Worker, class DatabaseClient>
struct SyncToProtocol {
	SyncToProtocol(Worker &worker):
		worker(worker),
		client(worker.client),
		sync_queue(worker.sync_queue),
		input(worker.input),
		output(worker.output),
		hash_algorithm(worker.configured_hash_algorithm),
		target_minimum_block_size(1),
		target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE) {
	}

	void negotiate_target_minimum_block_size() {
		send_command(output, Commands::TARGET_BLOCK_SIZE, DEFAULT_MINIMUM_BLOCK_SIZE);

		// the real app always accepts the block size we request, but the test suite uses smaller block sizes to make it easier to set up different scenarios
		read_expected_command(input, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size);
	}

	void negotiate_hash_algorithm() {
		send_command(output, Commands::HASH_ALGORITHM, hash_algorithm);
		read_expected_command(input, Commands::HASH_ALGORITHM, hash_algorithm);
	}

	void sync_tables() {
		negotiate_target_minimum_block_size();
		negotiate_hash_algorithm();

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
		RowReplacer<DatabaseClient> row_replacer(client, table, worker.commit_level >= CommitLevel::often,
			[&] { if (worker.progress) { cout << "." << flush; } });

		size_t hash_commands = 0;
		size_t rows_commands = 0;
		time_t started = time(nullptr);

		if (worker.verbose) {
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << fixed << setw(5);
			cout << timestamp() << " starting " << table.name << endl << flush;
		}

		TableJob table_job(table);

		if (worker.verbose > 1) cout << timestamp() << " <- range " << table_job.table.name << endl;
		send_command(output, Commands::RANGE, table_job.table.name);

		while (true) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table

			if (worker.progress) {
				cout << "." << flush; // simple progress meter
			}

			// now read and act on their response to the last command
			handle_response(table_job, row_replacer);

			if (!table_job.ranges_to_retrieve.empty()) {
				ColumnValues prev_key, last_key;

				tie(prev_key, last_key) = table_job.ranges_to_retrieve.front();
				table_job.ranges_to_retrieve.pop_front();

				if (worker.verbose > 1) cout << timestamp() << " <- rows " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << endl;
				send_command(output, Commands::ROWS, table_job.table.name, prev_key, last_key);
				rows_commands++;

			} else if (!table_job.ranges_to_check.empty()) {
				ColumnValues prev_key, last_key;
				size_t estimated_rows_in_range, rows_to_hash;

				tie(prev_key, last_key, estimated_rows_in_range, rows_to_hash) = table_job.ranges_to_check.front();
				table_job.ranges_to_check.pop_front();

				// tell the other end to hash this range
				if (worker.verbose > 1) cout << timestamp() << " <- hash " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << ' ' << rows_to_hash << endl;
				send_command(output, Commands::HASH, table_job.table.name, prev_key, last_key, rows_to_hash);
				hash_commands++;

				// while that end is working, do the same at our end
				RowHasherAndLastKey hasher(hash_algorithm, table.primary_key_columns);
				worker.client.retrieve_rows(hasher, table, prev_key, last_key, rows_to_hash);
				table_job.ranges_hashed.push_back(HashResult(prev_key, last_key, estimated_rows_in_range, hasher.row_count, hasher.size, hasher.finish().to_string(), hasher.last_key));

			} else {
				break;
			}
		}

		// make sure all pending updates have been applied
		row_replacer.apply();

		// reset sequences on those databases that don't automatically bump the high-water mark for inserts
		ResetTableSequences<DatabaseClient>::execute(client, table);

		if (worker.verbose) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << timestamp() << " finished " << table.name << " in " << (now - started) << "s using " << hash_commands << " hash commands and " << rows_commands << " rows commands changing " << row_replacer.rows_changed << " rows" << endl << flush;
		}

		if (worker.commit_level >= CommitLevel::tables) {
			worker.commit();
			client.start_write_transaction();
		}
	}

	inline void handle_response(TableJob &table_job, RowReplacer<DatabaseClient> &row_replacer) {
		verb_t verb;
		input >> verb;

		switch (verb) {
			case Commands::HASH:
				handle_hash_response(table_job);
				break;

			case Commands::ROWS:
				handle_rows_response(table_job.table, row_replacer);
				break;

			case Commands::RANGE:
				handle_range_response(table_job);
				break;

			default:
				throw command_error("Unexpected command " + to_string(verb));
		}
	}

	void handle_range_response(TableJob &table_job) {
		string _table_name;
		ColumnValues their_first_key, their_last_key;
		read_all_arguments(input, _table_name, their_first_key, their_last_key);
		if (worker.verbose > 1) cout << timestamp() << " -> range " << table_job.table.name << ' ' << values_list(client, table_job.table, their_first_key) << ' ' << values_list(client, table_job.table, their_last_key) << endl;

		if (their_first_key.empty()) {
			client.execute("DELETE FROM " + table_job.table.name);
			return;
		}

		// we immediately know that we need to clear everything < their_first_key or > their_last_key; do that now
		string key_columns(columns_list(client, table_job.table.columns, table_job.table.primary_key_columns));
		string delete_from("DELETE FROM " + table_job.table.name + " WHERE " + key_columns);
		client.execute(delete_from + " < " + values_list(client, table_job.table, their_first_key));
		client.execute(delete_from + " > " + values_list(client, table_job.table, their_last_key));

		// having done that, find our last key, which must now be no greater than their_last_key
		ColumnValues our_last_key(worker.client.last_key(table_job.table));

		// we immediately know that we need to retrieve any new rows > our_last_key, unless their last key is the same;
		// queue this up
		if (our_last_key != their_last_key) {
			table_job.ranges_to_retrieve.push_back(make_tuple(our_last_key, their_last_key));
		}

		// queue up a sync of everything up to our_last_key; the way we have defined key ranges to work, we have no way
		// to express start-inclusive ranges to the sync methods, so we queue a sync from the start (empty key value)
		// up to the last key, but this results in no actual inefficiency because they'd see the same rows anyway.
		if (!our_last_key.empty()) {
			table_job.ranges_to_check.push_back(make_tuple(ColumnValues(), our_last_key, UNKNOWN_ROW_COUNT, 1 /* start with 1 row and build up */));
		}
	}

	void handle_rows_response(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		string table_name;
		ColumnValues prev_key, last_key;
		read_array(input, table_name, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (worker.verbose > 1) cout << timestamp() << " -> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
	}

	HashResult find_hash_result_for(TableJob &table_job, const ColumnValues &prev_key, const ColumnValues &last_key) {
		for (auto it = table_job.ranges_hashed.begin(); it != table_job.ranges_hashed.end(); ++it) {
			HashResult result(*it);
			table_job.ranges_hashed.erase(it);
			return result;
		}
		throw command_error("Haven't issued a hash command for " + table_job.table.name + " " + values_list(client, table_job.table, prev_key) + " " + values_list(client, table_job.table, last_key));
	}

	void handle_hash_response(TableJob &table_job) {
		size_t rows_to_hash, their_row_count;
		string their_hash;
		string table_name;
		ColumnValues prev_key, last_key;
		read_all_arguments(input, table_name, prev_key, last_key, rows_to_hash, their_row_count, their_hash);
		
		HashResult hash_result(find_hash_result_for(table_job, prev_key, last_key));

		bool match = (hash_result.our_hash == their_hash && hash_result.our_row_count == their_row_count);
		if (worker.verbose > 1) cout << timestamp() << " -> hash " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << ' ' << their_row_count << (match ? " matches" : " doesn't match") << endl;

		if (hash_result.our_last_key != last_key) {
			// whether or not we found an error in the range we just did, we don't know whether
			// there is an error in the remaining part of the original range (which could be simply
			// the rest of the table); queue it to be scanned
			if (hash_result.estimated_rows_in_range == UNKNOWN_ROW_COUNT) {
				// we're scanning forward, do that last
				size_t rows_to_hash = rows_to_scan_forward_next(rows_to_hash, match, hash_result.our_row_count, hash_result.our_size);
				table_job.ranges_to_check.push_back(make_tuple(hash_result.our_last_key, last_key, UNKNOWN_ROW_COUNT, rows_to_hash));
			} else {
				// we're hunting errors, do that first.  if the part just checked matched, then the
				// error must be in the remaining part, so consider subdividing it; if it didn't match,
				// then we don't know if the remaining part has error or not, so don't subdivide it yet.
				size_t rows_remaining = hash_result.estimated_rows_in_range - hash_result.our_row_count;
				size_t rows_to_hash = match && rows_remaining > 1 && hash_result.our_size > target_minimum_block_size ? rows_remaining/2 : rows_remaining;
				table_job.ranges_to_check.push_front(make_tuple(hash_result.our_last_key, last_key, rows_remaining, rows_to_hash));
			}
		}

		if (!match) {
			// the part that we checked has an error; decide whether it's large enough to subdivide
			if (hash_result.our_row_count > 1 && hash_result.our_size > target_minimum_block_size) {
				// yup, queue it up for another iteration of hashing, subdividing the range
				table_job.ranges_to_check.push_front(make_tuple(prev_key, hash_result.our_last_key, hash_result.our_row_count, hash_result.our_row_count/2));
			} else {
				// not worth subdividing the range any further, queue it to be retrieved
				table_job.ranges_to_retrieve.push_back(make_tuple(prev_key, hash_result.our_last_key));
			}
		}
	}

	inline size_t rows_to_scan_forward_next(size_t rows_scanned, bool match, size_t our_row_count, size_t our_size) {
		if (match) {
			// on the next iteration, scan more rows per iteration, to reduce the impact of latency between the ends -
			// up to a point, after which the cost of re-work when we finally run into a mismatch outweights the
			// benefit of the latency savings
			if (our_size <= target_maximum_block_size/2) {
				return our_row_count*2;
			} else {
				return max<size_t>(our_row_count*target_maximum_block_size/our_size, 1);
			}
		} else {
			// on the next iteration, scan fewer rows per iteration, to reduce the cost of re-work (down to a point)
			if (our_size >= target_minimum_block_size*2) {
				return max<size_t>(our_row_count/2, 1);
			} else {
				return max<size_t>(our_row_count*target_minimum_block_size/our_size, 1);
			}
		}
	}

	Worker &worker;
	DatabaseClient &client;
	SyncQueue &sync_queue;
	Unpacker<FDReadStream> &input;
	Packer<FDWriteStream> &output;
	HashAlgorithm hash_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
};
