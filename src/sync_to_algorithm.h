#include "timestamp.h"

struct HashResult {
	HashResult(const ColumnValues &prev_key, const ColumnValues &last_key, size_t estimated_rows_in_range, size_t priority, size_t our_row_count, size_t our_size, string our_hash, const ColumnValues &our_last_key, const ColumnValues &next_midpoint):
		prev_key(prev_key), last_key(last_key), estimated_rows_in_range(estimated_rows_in_range), priority(priority), our_row_count(our_row_count), our_size(our_size), our_hash(our_hash), our_last_key(our_last_key), next_midpoint(next_midpoint) {}

	ColumnValues prev_key;
	ColumnValues last_key;
	size_t estimated_rows_in_range;
	size_t priority;

	size_t our_row_count;
	size_t our_size;
	string our_hash;
	ColumnValues our_last_key;
	ColumnValues next_midpoint;
};

template <class Worker, class DatabaseClient>
struct SyncToAlgorithm {
	SyncToAlgorithm(Worker &worker):
		worker(worker),
		client(worker.client),
		sync_queue(worker.sync_queue),
		input(worker.input),
		output(worker.output),
		hash_algorithm(worker.hash_algorithm),
		target_minimum_block_size(worker.target_minimum_block_size),
		target_maximum_block_size(worker.target_maximum_block_size) {
	}

	void sync_tables() {
		while (true) {
			// grab the next table to work on from the queue, blocking if there's nothing to do right now, quitting if the whole sync is finished
			shared_ptr<TableJob> table_job = sync_queue.find_table_job();
			if (!table_job) break;
			sync_table(table_job);
		}
	}

	void start_sync_table(const shared_ptr<TableJob> &table_job, RowReplacer<DatabaseClient> &row_replacer) {
		table_job->time_started = time(nullptr);

		if (worker.verbose) {
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << fixed << setw(5);
			if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << ' ';
			cout << "starting " << table_job->table.name << endl << flush;
		}

		if (table_job->table.primary_key_type != PrimaryKeyType::no_available_key) {
			// start by scoping out the table
			if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " <- range " << table_job->table.name << endl;
			send_command(output, Commands::RANGE, table_job->table_id);
			if (input.next<verb_t>() != Commands::RANGE) throw command_error("Didn't receive response to RANGE command");
			handle_range_response(table_job, row_replacer);
		} else {
			// if the table has no usable keys, all we can do is retrieve and apply the rows
			if (worker.verbose) cout << "Clearing and reloading " << table_job->table.name << ", can't efficiently detect differences because it has no primary key and no other suitable keys." << endl;
			if (!worker.insert_only) row_replacer.clear_range(ColumnValues(), ColumnValues());
			request_rows_without_pipelining(table_job, row_replacer, KeyRange());
		}
	}

	void finish_sync_table(const shared_ptr<TableJob> &table_job, size_t rows_changed) {
		// reset sequences on those databases that don't automatically bump the high-water mark for inserts
		ResetTableSequences<DatabaseClient>::execute(client, table_job->table);

		if (worker.verbose) {
			table_job->time_finished = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << ' ';
			cout << "finished " << table_job->table.name << " in " << (table_job->time_finished - table_job->time_started) << "s using " << table_job->hash_commands << " hash commands and " << table_job->rows_commands << " rows commands changing " << rows_changed << " rows" << endl << flush;
		}
	}

	inline void sync_table(const shared_ptr<TableJob> &table_job) {
		const Table &table(table_job->table);
		RowReplacer<DatabaseClient> row_replacer(client, table, worker.commit_level >= CommitLevel::often,
			[&] { if (worker.progress) { cout << "." << flush; } });

		// if the table hasn't been started, become the writer worker for it; otherwise just help out with range checks
		bool writer = !table_job->time_started;
		if (writer) start_sync_table(table_job, row_replacer);

		size_t outstanding_commands = 0;
		size_t max_outstanding_commands = DEFAULT_MAX_COMMANDS_TO_PIPELINE;

		list<HashResult> ranges_hashed;

		while (true) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table

			std::unique_lock<std::mutex> lock(table_job->mutex);

			if (writer && outstanding_commands < max_outstanding_commands && !table_job->ranges_to_retrieve.empty()) {
				KeyRange range_to_retrieve(move(table_job->ranges_to_retrieve.front()));
				table_job->ranges_to_retrieve.pop_front();
				table_job->rows_commands++;
				lock.unlock(); // don't hold the mutex while doing IO

				outstanding_commands++;
				send_rows_command(table_job, range_to_retrieve);

			} else if (outstanding_commands < max_outstanding_commands && !table_job->ranges_to_check.empty()) {
				KeyRangeToCheck range_to_check(move(table_job->ranges_to_check.top()));
				table_job->ranges_to_check.pop();
				table_job->hash_commands++;
				lock.unlock(); // don't hold the mutex while doing IO

				outstanding_commands++;
				send_hash_command(table_job, range_to_check, ranges_hashed);

			} else if (outstanding_commands > 0) {
				lock.unlock(); // don't hold the mutex while doing IO; note we still had to lock the mutex in order to check the emptiness of those lists
				handle_response(table_job, ranges_hashed, row_replacer);
				outstanding_commands--;

			} else if (writer && table_job->hash_commands_completed < table_job->hash_commands) {
				// wait for the other worker(s) to complete their task, then wake up to see if there is anything for us to do
				// note that they have to send back any mutation tasks (ie. ranges_to_retrieve) since only one database
				// connection may mutate a table, to avoid fighting for locks; we can also compete for ranges_to_check ourselves
				table_job->borrowed_task_completed.wait(lock);

			} else if (writer) {
				// nothing left to do on this table
				lock.unlock(); // don't hold the mutex while doing IO

				// make sure all pending updates have been applied
				row_replacer.apply();

				// wrap up, log it, and potentially commit it
				finish_sync_table(table_job, row_replacer.rows_changed);

				// remove it from the list of tables being worked on
				sync_queue.completed_table(table_job);
				return;

			} else {
				// nothing left to help with on this table at the moment, look for other tables with work to share
				lock.unlock(); // don't hold the mutex while doing IO
				send_idle_command(); // clear the status at the other end so admins aren't confused
				return;
			}

			if (worker.progress) {
				cout << "." << flush; // simple progress meter
			}
		}
	}

	inline void send_rows_command(const shared_ptr<TableJob> &table_job, const KeyRange &range_to_retrieve) {
		const ColumnValues &prev_key(get<0>(range_to_retrieve));
		const ColumnValues &last_key(get<1>(range_to_retrieve));
		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " <- rows " << table_job->table.name << ' ' << values_list(client, table_job->table, prev_key) << ' ' << values_list(client, table_job->table, last_key) << endl;
		send_command(output, Commands::ROWS, table_job->table_id, prev_key, last_key);
	}

	inline void send_hash_command(const shared_ptr<TableJob> &table_job, const KeyRangeToCheck &range_to_check, list<HashResult> &ranges_hashed) {
		const Table &table(table_job->table);
		const ColumnValues &prev_key(get<0>(range_to_check.key_range));
		const ColumnValues &last_key(get<1>(range_to_check.key_range));
		if (range_to_check.rows_to_hash == 0) throw logic_error("Can't hash 0 rows");

		// tell the other end to hash this range
		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " <- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << ' ' << range_to_check.rows_to_hash << endl;
		send_command(output, Commands::HASH, table_job->table_id, prev_key, last_key, range_to_check.rows_to_hash);

		// while that end is working, do the same at our end
		RowHasherAndLastKey hasher(hash_algorithm, table.primary_key_columns);
		size_t row_count = retrieve_rows(client, hasher, table, prev_key, last_key, range_to_check.rows_to_hash);

		// when the table has a subdividable primary key, we try to break the remaining range into two, so that if
		// there's another worker free it can start checking the second half.  we don't actually queue either half
		// to be checked yet (until we find out if the last range matched), but we might as well go ahead and find
		// the midpoint as early as possible since it involves blocking database calls (as below).
		ColumnValues next_midpoint;

		if (table_job->subdividable && // subdividable is immutable, don't need to lock to access it
			range_to_check.estimated_rows_in_range == UNKNOWN_ROW_COUNT && // only subdivide when scanning forward, not recursing for errors
			row_count == range_to_check.rows_to_hash && // don't subdivide if we're at the end of the table
			hasher.last_key != last_key) { // don't subdivide if we're at the end of the table
			// find the key about halfway through the range.  we could find the key more exactly using count queries
			// and limit/offset queries, but this would be incredibly expensive for a large table, so we estimate by
			// interpolating the actual key range values, and then do a query to find the next actual key.  finding
			// an actual key is not required for correctness, but makes testing easier.
			next_midpoint = std::move(first_key_not_earlier_than(client, table, subdivide_primary_key_range(table, hasher.last_key, last_key), hasher.last_key, last_key));
		}

		// and store the hash away temporarily for us to check when the corresponding response comes back
		// (which may not be the next response we read in due to our use of pipelining)
		ranges_hashed.emplace_back(
			prev_key,
			last_key,
			range_to_check.estimated_rows_in_range,
			range_to_check.priority,
			row_count,
			hasher.size,
			hasher.finish().to_string(),
			hasher.last_key,
			std::move(next_midpoint));
	}

	inline void handle_response(const shared_ptr<TableJob> &table_job, list<HashResult> &ranges_hashed, RowReplacer<DatabaseClient> &row_replacer) {
		verb_t verb;
		input >> verb;

		switch (verb) {
			case Commands::HASH:
				handle_hash_response(table_job, ranges_hashed);
				break;

			case Commands::ROWS:
				handle_rows_response(table_job->table, row_replacer);
				break;

			default:
				throw command_error("Unexpected command " + to_string(verb));
		}
	}

	void handle_range_response(const shared_ptr<TableJob> &table_job, RowReplacer<DatabaseClient> &row_replacer) {
		string _table_name;
		ColumnValues their_first_key, their_last_key;
		read_all_arguments(input, _table_name, their_first_key, their_last_key);
		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " -> range " << table_job->table.name << ' ' << values_list(client, table_job->table, their_first_key) << ' ' << values_list(client, table_job->table, their_last_key) << endl;

		if (their_first_key.empty()) {
			client.execute("DELETE FROM " + client.quote_table_name(table_job->table));
			return;
		}

		// we immediately know that we need to clear everything < their_first_key or > their_last_key; do that now
		string key_columns(columns_list(client, table_job->table.columns, table_job->table.primary_key_columns));
		string delete_from("DELETE FROM " + client.quote_table_name(table_job->table) + " WHERE (" + key_columns + ")");
		client.execute(delete_from + " < " + values_list(client, table_job->table, their_first_key));
		client.execute(delete_from + " > " + values_list(client, table_job->table, their_last_key));

		// having done that, find our last key, which must now be no greater than their_last_key
		ColumnValues our_last_key(last_key(client, table_job->table));

		if (!our_last_key.empty()) {
			// queue up a sync of everything up to our_last_key
			queue_initial_ranges(table_job, our_last_key, their_first_key, their_last_key);

			if (table_job->notify_when_work_could_be_shared) {
				sync_queue.have_work_to_share(table_job);
			}
		}

		// we immediately know that we need to retrieve any new rows > our_last_key, unless their last key is the same;
		// request and insert them now.  we do this before we start the main loop because we don't want to pipeline any
		// other requests while processing this one (potentially very large) set of rows, as we won't see the response
		// to the later commands in the pipeline until we're finished with this one, whereas there is a chance that
		// another worker could become free and process those other tasks in the meantime.
		if (our_last_key != their_last_key) {
			request_rows_without_pipelining(table_job, row_replacer, KeyRange(our_last_key, their_last_key));
		}
	}

	void queue_initial_ranges(const shared_ptr<TableJob> &table_job, const ColumnValues &our_last_key, const ColumnValues &their_first_key, const ColumnValues &their_last_key) {
		std::unique_lock<std::mutex> lock(table_job->mutex);

		// the way we have defined key ranges to work, we have no way to express start-inclusive ranges to the sync
		// methods, so we queue a sync from the start (empty key value) up to the last key - but this results in no
		// actual inefficiency because they'd see the same rows anyway.  we attempt to  do one subdivision straight
		// away, to facilitate parallelism; if we can't subdivide, \midpoint stays empty so we only queue one range.
		ColumnValues midpoint;
		if (table_job->subdividable) {
			midpoint = move(first_key_not_earlier_than(client, table_job->table, subdivide_primary_key_range(table_job->table, their_first_key /* ideally for consistency we'd use this value minus one, but it doesn't actually matter */, their_last_key /* our_last_key would be better but might be < their_first_key and that is unsupported */), ColumnValues(), our_last_key));
		}
		if (!midpoint.empty()) {
			table_job->ranges_to_check.emplace(ColumnValues(), midpoint, UNKNOWN_ROW_COUNT, 1 /* start with 1 row and build up */, 0);
		}
		if (midpoint != our_last_key) {
			table_job->ranges_to_check.emplace(midpoint, our_last_key, UNKNOWN_ROW_COUNT, 1 /* start with 1 row and build up */, 0);
		}
	}

	void request_rows_without_pipelining(const shared_ptr<TableJob> &table_job, RowReplacer<DatabaseClient> &row_replacer, const KeyRange &range_to_retrieve) {
		send_rows_command(table_job, range_to_retrieve);
		if (input.next<verb_t>() != Commands::ROWS) throw command_error("Didn't receive response to ROWS command");
		handle_rows_response(table_job->table, row_replacer, true);

		std::unique_lock<std::mutex> lock(table_job->mutex);
		table_job->rows_commands++;
	}

	void handle_rows_response(const Table &table, RowReplacer<DatabaseClient> &row_replacer, bool final_rows = false) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		string table_name;
		ColumnValues prev_key, last_key;
		read_array(input, table_name, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " -> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		if (final_rows || worker.insert_only) {
			RowInserter<DatabaseClient>(row_replacer, table).stream_from_input(input);
		} else {
			RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
		}
	}

	void handle_hash_response(const shared_ptr<TableJob> &table_job, list<HashResult> &ranges_hashed) {
		size_t rows_to_hash, their_row_count;
		string their_hash;
		string table_name;
		ColumnValues prev_key, last_key;
		read_all_arguments(input, table_name, prev_key, last_key, rows_to_hash, their_row_count, their_hash);

		const Table &table(table_job->table);
		if (ranges_hashed.empty()) throw command_error("Haven't issued a hash command for " + table.name + ", received " + values_list(client, table, prev_key) + " " + values_list(client, table, last_key));
		HashResult hash_result(move(ranges_hashed.front()));
		ranges_hashed.pop_front();
		if (table_name != table.name || prev_key != hash_result.prev_key || last_key != hash_result.last_key) throw command_error("Didn't issue hash command for " + table.name + " " + values_list(client, table, prev_key) + " " + values_list(client, table, last_key));

		bool match = (hash_result.our_hash == their_hash && hash_result.our_row_count == their_row_count);
		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << " -> hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << ' ' << their_row_count << (match ? " matches" : " doesn't match") << endl;

		std::unique_lock<std::mutex> lock(table_job->mutex);

		if (hash_result.our_row_count == rows_to_hash && hash_result.our_last_key != last_key) {
			// whether or not we found an error in the range we just did, we don't know whether
			// there is an error in the remaining part of the original range (which could be simply
			// the rest of the table); queue it to be scanned
			if (hash_result.estimated_rows_in_range == UNKNOWN_ROW_COUNT) {
				// we're scanning forward, do that last
				size_t rows_to_hash_next = rows_to_scan_forward_next(rows_to_hash, match, hash_result.our_row_count, hash_result.our_size);

				// as discussed in send_hash_command, when the table has a subdividable primary key, we
				// try to break the remaining range into two, so that if there's another worker free it
				// can start checking the second half.  doing this creates a risk that we could end up
				// breaking up the entire table's key range into a huge number of small ranges.  to avoid
				// this, we use a sorted priority queue to ensure that we always work on key ranges that
				// are the result of more subdivisions before ranges that are the result of fewer.
				if (!hash_result.next_midpoint.empty()) {
					if (hash_result.next_midpoint != hash_result.our_last_key) {
						table_job->ranges_to_check.emplace(hash_result.our_last_key, hash_result.next_midpoint, UNKNOWN_ROW_COUNT, rows_to_hash_next, hash_result.priority + 1);
					}
					if (last_key != hash_result.next_midpoint) {
						table_job->ranges_to_check.emplace(hash_result.next_midpoint, last_key, UNKNOWN_ROW_COUNT, rows_to_hash_next, hash_result.priority + 1);
					}
				} else {
					table_job->ranges_to_check.emplace(hash_result.our_last_key, last_key, UNKNOWN_ROW_COUNT, rows_to_hash_next, hash_result.priority);
				}
			} else {
				// we're hunting errors, do that first.  if the part just checked matched, then the
				// error must be in the remaining part, so hash only half the rows in the next check;
				// if it didn't match, then we don't know if the remaining part has error or not, but
				// we hope not, so hash the remaining rows in one go.
				size_t rows_remaining = hash_result.estimated_rows_in_range > hash_result.our_row_count ? hash_result.estimated_rows_in_range - hash_result.our_row_count : 1; // conditional to protect against underflow
				size_t rows_to_hash_next = match && rows_remaining > 1 && hash_result.our_size > target_minimum_block_size ? rows_remaining/2 : rows_remaining;

				table_job->ranges_to_check.emplace(hash_result.our_last_key, last_key, rows_remaining, rows_to_hash_next, hash_result.priority + 1);
			}
		}

		if (!match) {
			// the part that we checked has an error; decide whether it's large enough to bother locating it more precisely
			if (hash_result.our_row_count > 1 && hash_result.our_size > target_minimum_block_size) {
				// yup, queue it up for another iteration of hashing, checking half the rows at a time
				table_job->ranges_to_check.emplace(prev_key, hash_result.our_last_key, hash_result.our_row_count, hash_result.our_row_count/2, hash_result.priority + 1);
			} else {
				// not worth reducing the affected row range any further, queue it to be retrieved
				table_job->ranges_to_retrieve.emplace_back(prev_key, hash_result.our_last_key);
			}
		}

		table_job->hash_commands_completed++;

		if (worker.verbose > 1) cout << timestamp() << " worker " << worker.worker_number << "         " << table.name << " has " << table_job->ranges_to_check.size() << " range(s) to check and " << table_job->ranges_to_retrieve.size() << " to retrieve, " << string(table_job->notify_when_work_could_be_shared ? "sharing wanted" : "sharing not needed") << endl;

		if (table_job->notify_when_work_could_be_shared) {
			table_job->borrowed_task_completed.notify_all(); // not really borrowed if we are the writer worker, but since only the writer waits on this condition it's moot
			lock.unlock();
			sync_queue.have_work_to_share(table_job);
		}
	}

	inline size_t rows_to_scan_forward_next(size_t rows_scanned, bool match, size_t our_row_count, size_t our_size) {
		if (match) {
			// on the next iteration, scan more rows per iteration, to reduce the impact of latency between the ends -
			// up to a point, after which the cost of re-work when we finally run into a mismatch outweights the
			// benefit of the latency savings
			if (our_size <= target_maximum_block_size/2) {
				return max<size_t>(our_row_count*2, 1);
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

	inline void send_idle_command() {
		if (output.stream().protocol_version >= FIRST_IDLE_COMMAND_VERSION) {
			send_command(output, Commands::IDLE);
			read_expected_command(input, Commands::IDLE);
		}
	}

	Worker &worker;
	DatabaseClient &client;
	SyncQueue<DatabaseClient> &sync_queue;
	Unpacker<VersionedFDReadStream> &input;
	Packer<VersionedFDWriteStream> &output;
	HashAlgorithm hash_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
};
