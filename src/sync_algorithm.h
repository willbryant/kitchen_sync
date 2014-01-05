#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

#include "schema.h"
#include "row_serialization.h"

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename DatabaseClient>
void check_hash_and_choose_next_range(DatabaseClient &client, const Table &table, ColumnValues &prev_key, ColumnValues &last_key, string &hash) {
	size_t rows_to_hash;

	if (last_key.empty()) {
		// at the start of a new table, or after requesting a row, start by hashing the next row
		rows_to_hash = 1;
	} else {
		// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
		RowHasher<typename DatabaseClient::RowType> hasher_for_their_rows;
		client.retrieve_rows(table, prev_key, last_key, hasher_for_their_rows);

		if (hasher_for_their_rows.finish() == hash) {
			// match, move on to the next set of rows, and optimistically double the row count
			prev_key = last_key;
			rows_to_hash = hasher_for_their_rows.row_count*2;

		} else if (hasher_for_their_rows.row_count > 1) {
			// no match, try again starting at the same row for a smaller set of rows
			rows_to_hash = hasher_for_their_rows.row_count/2;

		} else {
			// rows don't match, and there's only 0 or 1 rows in that range on our side, so it's time to send
			// rows instead of trading hashes; don't advance prev_key or change last_key, so we send that range
			hash.clear();
			return;
		}
	}

	RowHasherAndLastKey<typename DatabaseClient::RowType> hasher_for_our_rows(table.primary_key_columns);
	client.retrieve_rows(table, prev_key, rows_to_hash, hasher_for_our_rows);
	last_key = hasher_for_our_rows.last_key;

	if (hasher_for_our_rows.row_count == 0) {
		// we've reached the end of the table, so we just need to do a rows command for the range after the
		// previously—matched key, to clear out any extra entries at the 'to' end
		hash.clear();
	} else {
		// found some rows, send the new key range and the new hash to the other end
		hash = hasher_for_our_rows.finish().to_string();
	}
}

#endif
