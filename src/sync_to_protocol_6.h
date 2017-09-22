#include "sync_algorithm_6.h"

template <class Worker, class DatabaseClient>
struct SyncToProtocol6 {
	SyncToProtocol6(Worker &worker):
		worker(worker),
		client(worker.client),
		sync_queue(worker.sync_queue),
		input(worker.input),
		output(worker.output),
		sync_algorithm(*this, worker.client, worker.configured_hash_algorithm),
		target_minimum_block_size(1),
		target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE6) {
	}

	void negotiate_target_minimum_block_size() {
		send_command(output, Commands::TARGET_BLOCK_SIZE, DEFAULT_MINIMUM_BLOCK_SIZE6);

		// the real app always accepts the block size we request, but the test suite uses smaller block sizes to make it easier to set up different scenarios
		read_expected_command(input, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size);
	}

	void negotiate_hash_algorithm() {
		send_command(output, Commands::HASH_ALGORITHM, sync_algorithm.hash_algorithm);
		read_expected_command(input, Commands::HASH_ALGORITHM, sync_algorithm.hash_algorithm);
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
		bool finished = false;

		if (worker.verbose) {
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "starting " << table.name << endl << flush;
		}

		send_command(output, Commands::OPEN, table.name);

		while (!finished) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table; this is a good place to do it since it's likely we'll have no work to do for a short while

			if (worker.progress) {
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

		if (worker.verbose) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << "finished " << table.name << " in " << (now - started) << "s using " << hash_commands << " hash commands and " << rows_commands << " rows commands changing " << row_replacer.rows_changed << " rows" << endl << flush;
		}

		if (worker.commit_level >= CommitLevel::tables) {
			worker.commit();
			client.start_write_transaction();
		}
	}

	void handle_hash_next_command(const Table &table) {
		// the last hash we sent them matched, and so they've moved on to the next set of rows and sent us the hash
		ColumnValues prev_key, last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, hash);
		if (worker.verbose > 1) cout << "-> hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

		// after each hash command received it's our turn to send the next command
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, prev_key, last_key, nullptr, hash, target_minimum_block_size, target_maximum_block_size);
	}

	void handle_hash_fail_command(const Table &table) {
		// the last hash we sent them didn't match, so they've reduced the key range and sent us back
		// the hash for a smaller set of rows (but not so small that they sent back the data instead)
		ColumnValues prev_key, last_key, failed_last_key;
		string hash;
		read_all_arguments(input, prev_key, last_key, failed_last_key, hash);
		if (worker.verbose > 1) cout << "-> hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;

		// after each hash command received it's our turn to send the next command
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, prev_key, last_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
	}

	bool handle_rows_command(const Table &table, RowReplacer<DatabaseClient> &row_replacer) {
		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		ColumnValues prev_key, last_key;
		read_array(input, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (worker.verbose > 1) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;

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
		if (worker.verbose > 1) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (worker.verbose > 1) cout << "-> hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << endl;

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
		if (worker.verbose > 1) cout << "-> rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (worker.verbose > 1) cout << "-> hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;

		// same pipelining as the previous case
		sync_algorithm.check_hash_and_choose_next_range(table, nullptr, last_key, next_key, &failed_last_key, hash, target_minimum_block_size, target_maximum_block_size);
		RowRangeApplier<DatabaseClient>(row_replacer, table, prev_key, last_key).stream_from_input(input);
	}

	inline void send_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		if (worker.verbose > 1) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;
		send_command(output, Commands::HASH_NEXT, prev_key, last_key, hash);
	}

	inline void send_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &failed_last_key, const string &hash) {
		if (worker.verbose > 1) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;
		send_command(output, Commands::HASH_FAIL, prev_key, last_key, failed_last_key, hash);
	}

	inline void send_rows_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		if (worker.verbose > 1) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << endl;
		send_command(output, Commands::ROWS, prev_key, last_key);
	}

	inline void send_rows_and_hash_next_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const string &hash) {
		if (worker.verbose > 1) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (worker.verbose > 1) cout << "<- hash " << table.name << ' ' << values_list(client, table, last_key) << ' ' << values_list(client, table, next_key) << endl;
		send_command(output, Commands::ROWS_AND_HASH_NEXT, prev_key, last_key, next_key, hash);
	}

	inline void send_rows_and_hash_fail_command(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &next_key, const ColumnValues &failed_last_key, const string &hash) {
		if (worker.verbose > 1) cout << "<- rows " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " +" << endl;
		if (worker.verbose > 1) cout << "<- hash " << table.name << ' ' << values_list(client, table, prev_key) << ' ' << values_list(client, table, last_key) << " last-failure " << values_list(client, table, failed_last_key) << endl;
		send_command(output, Commands::ROWS_AND_HASH_FAIL, prev_key, last_key, next_key, failed_last_key, hash);
	}

	Worker &worker;
	DatabaseClient &client;
	SyncQueue &sync_queue;
	Unpacker<FDReadStream> &input;
	Packer<FDWriteStream> &output;
	SyncAlgorithm6<SyncToProtocol6<Worker, DatabaseClient>, DatabaseClient> sync_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;
};
