#include "timestamp.h"

typedef tuple<ColumnValues, ColumnValues> KeyRange;
typedef tuple<ColumnValues, ColumnValues, size_t> KeyRangeWithRowCount;
const size_t UNKNOWN_ROW_COUNT = numeric_limits<size_t>::max();

struct TableJob {
	TableJob(const Table &table): table(table) {}

	const Table &table;
	deque<KeyRange> ranges_to_retrieve;
	deque<KeyRangeWithRowCount> ranges_to_check;
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
		target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE),
		rows_to_scan_forward_next(1) {
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

		establish_range(table_job);

		while (true) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table

			if (worker.progress) {
				cout << "." << flush; // simple progress meter
			}

			if (!table_job.ranges_to_retrieve.empty()) {
				ColumnValues prev_key, last_key;

				tie(prev_key, last_key) = table_job.ranges_to_retrieve.front();
				table_job.ranges_to_retrieve.pop_front();

				if (worker.verbose > 1) cout << timestamp() << " <- rows " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << endl;
				send_command(output, Commands::ROWS, table_job.table.name, prev_key, last_key);
				rows_commands++;

				expect_verb(Commands::ROWS);
				handle_rows_command(table, row_replacer);

			} else if (!table_job.ranges_to_check.empty()) {
				ColumnValues prev_key, last_key;
				size_t estimated_rows_in_range;

				tie(prev_key, last_key, estimated_rows_in_range) = table_job.ranges_to_check.front();
				table_job.ranges_to_check.pop_front();

				size_t rows_to_hash = decide_rows_to_hash(estimated_rows_in_range);

				// tell the other end to hash this range
				if (worker.verbose > 1) cout << timestamp() << " <- hash " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << ' ' << rows_to_hash << endl;
				send_command(output, Commands::HASH, table_job.table.name, prev_key, last_key, rows_to_hash);
				hash_commands++;

				// while that end is working, do the same at our end
				RowHasherAndLastKey hasher(hash_algorithm, table.primary_key_columns);
				worker.client.retrieve_rows(hasher, table, prev_key, last_key, rows_to_hash);
				const Hash &our_hash(hasher.finish());

				// now read their response
				expect_verb(Commands::HASH);
				size_t _rows_to_hash, their_row_count;
				string their_hash;
				string _table_name;
				ColumnValues _prev_key, _last_key;
				read_all_arguments(input, _table_name, _prev_key, _last_key, _rows_to_hash, their_row_count, their_hash);

				if (_table_name != table_job.table.name || _prev_key != prev_key || _last_key != last_key || _rows_to_hash != rows_to_hash) {
					throw command_error("Expected arguments " + table_job.table.name + ", " + values_list(client, table_job.table, prev_key) + ", " + values_list(client, table_job.table, last_key) + ", " + to_string(rows_to_hash) +
							"but received " + _table_name + ", " + values_list(client, table_job.table, _prev_key) + ", " + values_list(client, table_job.table, _last_key) + ", " + to_string(_rows_to_hash));
				}

				bool match = (our_hash == their_hash && hasher.row_count == their_row_count);
				if (worker.verbose > 1) cout << timestamp() << " -> hash " << table_job.table.name << ' ' << values_list(client, table_job.table, prev_key) << ' ' << values_list(client, table_job.table, last_key) << ' ' << their_row_count << (match ? " matches" : " doesn't match") << endl;

				if (hasher.last_key != last_key) {
					// whether or not we found an error in the range we just did, we don't know whether
					// there is an error in the remaining part of the original range (which could be simply
					// the rest of the table); queue it to be scanned
					if (estimated_rows_in_range == UNKNOWN_ROW_COUNT) {
						// we're scanning forward, do that last
						table_job.ranges_to_check.push_back(make_tuple(hasher.last_key, last_key, UNKNOWN_ROW_COUNT));

						if (match) {
							// on the next iteration, scan more rows per iteration, to reduce the impact of latency between the ends -
							// up to a point, after which the cost of re-work when we finally run into a mismatch outweights the
							// benefit of the latency savings
							increase_scan_size(hasher);
						} else {
							// on the next iteration, scan fewer rows per iteration, to reduce the cost of re-work (down to a point)
							decrease_scan_size(hasher);
						}
					} else {
						// we're hunting errors, do that first
						size_t rows_remaining = estimated_rows_in_range - hasher.row_count;
						table_job.ranges_to_check.push_front(make_tuple(hasher.last_key, last_key, rows_remaining));
					}
				}

				if (!match) {
					// the part that we checked has an error; decide whether it's large enough to subdivide
					handle_hash_not_matching(table_job, prev_key, hasher);
				}

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

	void expect_verb(verb_t expected) {
		verb_t verb;
		input >> verb;
		if (verb != expected) {
			throw command_error("Expected command " + to_string(verb) + " but received " + to_string(verb));
		}
	}

	void establish_range(TableJob &table_job) {
		send_command(output, Commands::RANGE, table_job.table.name);
		if (worker.verbose > 1) cout << timestamp() << " <- range " << table_job.table.name << endl;
		expect_verb(Commands::RANGE);

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
			table_job.ranges_to_check.push_back(make_tuple(ColumnValues(), our_last_key, UNKNOWN_ROW_COUNT));
		}
	}

	void handle_rows_command(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		string _table_name;
		ColumnValues prev_key, last_key;
		read_array(input, _table_name, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (worker.verbose > 1) cout << timestamp() << " -> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
	}

	void handle_hash_not_matching(TableJob &table_job, const ColumnValues &prev_key, const RowHasherAndLastKey &hasher) {
		// the part that we checked has an error; decide whether it's large enough to subdivide
		if (hasher.row_count > 1 && hasher.size > target_minimum_block_size) {
			// yup, queue it up for another iteration of hashing
			table_job.ranges_to_check.push_front(make_tuple(prev_key, hasher.last_key, hasher.row_count));
		} else {
			// not worth subdividing the range any further, queue it to be retrieved
			table_job.ranges_to_retrieve.push_back(make_tuple(prev_key, hasher.last_key));
		}
	}

	inline size_t decide_rows_to_hash(size_t estimated_rows_in_range) {
		if (estimated_rows_in_range == UNKNOWN_ROW_COUNT) {
			// scan forward
			return rows_to_scan_forward_next;
		} else if (estimated_rows_in_range > 3) {
			// break the range into two
			return  estimated_rows_in_range/2;
		} else {
			// down to single-row territory already
			return 1;
		}
	}

	inline void increase_scan_size(const RowHasher &hasher) {
		if (hasher.size <= target_maximum_block_size/2) {
			rows_to_scan_forward_next = hasher.row_count*2;
		} else {
			rows_to_scan_forward_next = max<size_t>(hasher.row_count*target_maximum_block_size/hasher.size, 1);
		}
	}

	inline void decrease_scan_size(const RowHasher &hasher) {
		// on the next iteration, scan fewer rows per iteration, to reduce the cost of re-work (down to a point)
		if (hasher.size >= target_minimum_block_size*2) {
			rows_to_scan_forward_next = max<size_t>(hasher.row_count/2, 1);
		} else {
			rows_to_scan_forward_next = max<size_t>(hasher.row_count*target_minimum_block_size/hasher.size, 1);
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
	size_t rows_to_scan_forward_next;
};
