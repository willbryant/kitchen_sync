#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

#include "schema.h"
#include "row_serialization.h"

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename DatabaseClient>
size_t check_hash_and_choose_next_range(DatabaseClient &client, const Table &table, ColumnValues &prev_key, ColumnValues &last_key, string &hash) {
	if (hash.empty()) throw logic_error("No hash to check given");
	if (last_key.empty()) throw logic_error("No range end given");

	// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
	RowHasher<typename DatabaseClient::RowType> hasher_for_their_range;
	client.retrieve_rows(table, prev_key, last_key, hasher_for_their_range);

	if (hasher_for_their_range.finish() == hash) {
		// match, move on to the next set of rows, and optimistically double the row count
		prev_key = last_key;
		return find_hash_of_next_range(client, table, hasher_for_their_range.row_count*2, prev_key, last_key, hash);

	} else if (hasher_for_their_range.row_count > 1) {
		// no match, try again starting at the same row for a smaller set of rows
		return find_hash_of_next_range(client, table, hasher_for_their_range.row_count/2, prev_key, last_key, hash);

	} else {
		// rows don't match, and there's only 0 or 1 rows in that range on our side, so it's time to send
		// rows instead of trading hashes; don't advance prev_key or change last_key, so we send that range
		hash.clear();
		return hasher_for_their_range.row_count;
	}
}

template <typename DatabaseClient>
size_t find_hash_of_next_range(DatabaseClient &client, const Table &table, size_t rows_to_hash, const ColumnValues &prev_key, ColumnValues &last_key, string &hash) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");
	
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
	return hasher_for_our_rows.row_count;
}

#endif
