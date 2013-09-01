#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

template <typename DatabaseClient>
size_t check_hash_and_choose_next_range(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash, ColumnValues &matched_up_to_key) {
	// at the start of a new table, or after requesting a row, start by hashing the next row
	if (last_key.empty()) {
		matched_up_to_key = prev_key; // will be empty if this is the start of a new table
		return 1;
	}

	// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
	RowHasher<typename DatabaseClient::RowType> hasher_for_their_rows;
	client.retrieve_rows(table, prev_key, last_key, hasher_for_their_rows);

	if (hasher_for_their_rows.finish() == hash) {
		// match, move on to the next set of rows, and optimistically double the row count
		matched_up_to_key = last_key;
		return hasher_for_their_rows.row_count*2;

	} else if (hasher_for_their_rows.row_count > 1) {
		// no match, try again starting at the same row for a smaller set of rows
		matched_up_to_key = prev_key;
		return hasher_for_their_rows.row_count/2;

	} else {
		// rows don't match, and there's only one or no rows in that range on our side, so it's time to send rows instead of trading hashes
		matched_up_to_key = prev_key; // will be empty if this is the start of a new table
		return 0;
	}
}

#endif
