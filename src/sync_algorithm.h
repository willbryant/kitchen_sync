#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

#include "schema.h"
#include "row_serialization.h"
#include "command.h"

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

template <typename Worker>
void check_hash_and_choose_next_range(Worker &worker, const Table &table, const ColumnValues *failed_prev_key, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues *failed_last_key, const string &hash) {
	if (hash.empty()) throw logic_error("No hash to check given");
	if (last_key.empty()) throw logic_error("No range end given");

	// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
	RowHasher hasher_for_their_range;
	worker.client.retrieve_rows(table, prev_key, last_key, hasher_for_their_range);

	if (hasher_for_their_range.finish() == hash) {
		if (failed_prev_key) {
			// send the previously-requested rows, since there's a span of successful rows after it
			// we don't want to combine the next hash command.
			worker.send_rows_command(table, *failed_prev_key, prev_key);
		}

		if (!failed_last_key) {
			// match, move on to the next set of rows, and optimistically double the row count
			hash_next_range(worker, table, hasher_for_their_range.row_count*2, last_key);
		} else {
			// this range matched but somewhere > last_key & <= failed_last_key there is a mismatch, so subdivide that range
			size_t rows_to_failure = atoi(worker.client.count_rows(table, last_key, *failed_last_key).c_str());

			if (rows_to_failure > 1) {
				// subdivide the range containing the failure, starting at the next row
				hash_failed_range(worker, table, rows_to_failure/2, NULL, last_key, *failed_last_key);
			} else {
				// there's only 0 or 1 rows in that range on our side, so it's time to send rows instead of trading hashes
				rows_and_next_hash(worker, table, last_key, *failed_last_key, !rows_to_failure);
			}
		}

	} else if (hasher_for_their_range.row_count > 1) {
		// no match; send the previously-requested rows if any, and subdivide the range starting at the same row
		hash_failed_range(worker, table, hasher_for_their_range.row_count/2, failed_prev_key, prev_key, last_key);

	} else {
		// rows don't match, and there's only 0 or 1 rows in that range on our side, so it's time to send
		// rows instead of trading hashes.  if the other end requested preceding rows, combine the request.
		rows_and_next_hash(worker, table, failed_prev_key ? *failed_prev_key : prev_key, last_key, !hasher_for_their_range.row_count);
	}
}

template <typename Worker>
void hash_failed_range(Worker &worker, const Table &table, size_t rows_to_hash, const ColumnValues *failed_prev_key, const ColumnValues &prev_key, const ColumnValues &failed_last_key) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");
	RowHasherAndLastKey hasher(table.primary_key_columns);
	worker.client.retrieve_rows(table, prev_key, rows_to_hash, hasher);
	if (failed_prev_key) {
		worker.send_rows_and_hash_fail_command(table, *failed_prev_key, prev_key, hasher.last_key, failed_last_key, hasher.finish().to_string());
	} else {
		worker.send_hash_fail_command(table, prev_key, hasher.last_key, failed_last_key, hasher.finish().to_string());
	}
}

template <typename Worker>
void hash_next_range(Worker &worker, const Table &table, size_t rows_to_hash, const ColumnValues &prev_key) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");
	
	RowHasherAndLastKey hasher(table.primary_key_columns);
	worker.client.retrieve_rows(table, prev_key, rows_to_hash, hasher);

	if (hasher.row_count == 0) {
		// we've reached the end of the table, so we just need to do a rows command for the range after the
		// previously—matched key, to get/clear out any extra entries at the other end
		worker.send_rows_command(table, prev_key, hasher.last_key /* will be [] */);
	} else {
		// found some rows, send the new key range and the new hash to the other end
		worker.send_hash_next_command(table, prev_key, hasher.last_key, hasher.finish().to_string());
	}
}

template <typename Worker>
void hash_first_range(Worker &worker, const Table &table) {
	hash_next_range(worker, table, 1, ColumnValues());
}

template <typename Worker>
void rows_and_next_hash(Worker &worker, const Table &table, const ColumnValues &prev_key, ColumnValues last_key, bool extend_last_key) {
	// if there were no rows in the range, we need to extend last_key forward to avoid
	// pathologically poor performance when our end has deleted a range of keys, as otherwise
	// they'll keep requesting deleted rows one-by-one.  we extend it to include the next row
	// (the hypothetical key value before that row's would be preferrable if we could find it).
	if (extend_last_key && !last_key.empty()) {
		RowLastKey row_last_key(table.primary_key_columns);
		worker.client.retrieve_rows(table, last_key, 1, row_last_key);
		last_key = row_last_key.last_key; // may still be empty if we have no more rows
	}

	// if that range extended to the end of the table, we just need to send the rows and we're done.
	// otherwise, we want to follow up straight away with the next command.  we combo the two together
	// to allow the other end to start looking at the hash while it's still receiving the rows off the
	// network.
	if (last_key.empty()) {
		worker.send_rows_command(table, prev_key, last_key /* will be [] */);
	} else {
		// find the hash for the range *after* the rows that we will send
		RowHasherAndLastKey hasher(table.primary_key_columns);
		worker.client.retrieve_rows(table, last_key, 1 /* rows to hash */, hasher);

		if (hasher.row_count == 0) {
			// we've reached the end of the table, so we can simply extend the range we were going to send
			worker.send_rows_command(table, prev_key, hasher.last_key /* will be [] */);
		} else {
			// send the new key range and the new hash to the other end, plus the rows for the current range which didn't match
			worker.send_rows_and_hash_next_command(table, prev_key, last_key, hasher.last_key, hasher.finish().to_string());
		}
	}
}

#endif
