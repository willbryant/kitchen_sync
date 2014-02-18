#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

#include "schema.h"
#include "row_serialization.h"
#include "command.h"

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename DatabaseClient>
void extend_last_key(DatabaseClient &client, const Table &table, ColumnValues &last_key) {
	RowLastKey<typename DatabaseClient::RowType> row_last_key(table.primary_key_columns);
	client.retrieve_rows(table, last_key, 1, row_last_key);
	last_key = row_last_key.last_key; // may be empty if we have no more rows
}

template <typename Worker, typename DatabaseClient>
void check_hash_and_choose_next_range(Worker &worker, DatabaseClient &client, const Table &table, ColumnValues &prev_key, ColumnValues &last_key, const string &hash) {
	if (hash.empty()) throw logic_error("No hash to check given");
	if (last_key.empty()) throw logic_error("No range end given");

	// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
	RowHasher<typename DatabaseClient::RowType> hasher_for_their_range;
	worker.client.retrieve_rows(table, prev_key, last_key, hasher_for_their_range);

	if (hasher_for_their_range.finish() == hash) {
		// match, move on to the next set of rows, and optimistically double the row count
		prev_key = last_key;
		find_hash_of_next_range(worker, client, table, hasher_for_their_range.row_count*2, prev_key, last_key);

	} else if (hasher_for_their_range.row_count > 1) {
		// no match, try again starting at the same row for a smaller set of rows
		find_hash_of_next_range(worker, client, table, hasher_for_their_range.row_count/2, prev_key, last_key);

	} else {
		// rows don't match, and there's only 0 or 1 rows in that range on our side, so it's time to send
		// rows instead of trading hashes; don't advance prev_key, but if there were no rows in the range,
		// extend last_key to avoid pathologically poor performance when our end has deleted a range of
		// keys, as otherwise they'll keep requesting deleted rows one-by-one.  we extend it to include
		// the next row (arguably the hypothetical key value before that row's would be better).
		if (hasher_for_their_range.row_count == 0 && !last_key.empty()) {
			extend_last_key(client, table, last_key);
		}

		worker.send_rows_command(table, prev_key, last_key);

		// if that range extended to the end of the table, we're done; otherwise, follow up straight away
		// with the next command
		if (!last_key.empty()) {
			prev_key = last_key;
			find_hash_of_next_range(worker, client, table, 1, prev_key, last_key);
		}
	}
}

template <typename Worker, typename DatabaseClient>
void find_hash_of_next_range(Worker &worker, DatabaseClient &client, const Table &table, size_t rows_to_hash, ColumnValues &prev_key, ColumnValues &last_key) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");
	
	RowHasherAndLastKey<typename DatabaseClient::RowType> hasher_for_our_rows(table.primary_key_columns);
	worker.client.retrieve_rows(table, prev_key, rows_to_hash, hasher_for_our_rows);
	last_key = hasher_for_our_rows.last_key;

	if (hasher_for_our_rows.row_count == 0) {
		// we've reached the end of the table, so we just need to do a rows command for the range after the
		// previously—matched key, to clear out any extra entries at the 'to' end
		worker.send_rows_command(table, prev_key, last_key);
	} else {
		// found some rows, send the new key range and the new hash to the other end
		string hash = hasher_for_our_rows.finish().to_string();
		worker.send_hash_command(table, prev_key, last_key, hash);
	}
}

#endif
