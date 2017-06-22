#ifndef SYNC_ALGORITHM_H
#define SYNC_ALGORITHM_H

#include "schema.h"
#include "row_serialization.h"
#include "command.h"
#include "hash_algorithm.h"
#include "sync_error.h"

const size_t DEFAULT_MINIMUM_BLOCK_SIZE =       256*1024; // arbitrary, but needs to be big enough to cope with a moderate amount of latency
const size_t DEFAULT_MAXIMUM_BLOCK_SIZE = 1024*1024*1024; // arbitrary, but needs to be small enough we don't waste unjustifiable amounts of CPU time if a block hash doesn't match

template <typename Worker>
void check_hash_and_choose_next_range(Worker &worker, const Table &table, const ColumnValues *failed_prev_key, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues *failed_last_key, const string &hash, size_t target_minimum_block_size, size_t target_maximum_block_size) {
	if (hash.empty()) throw logic_error("No hash to check given");
	if (last_key.empty()) throw logic_error("No range end given");

	// the other end has given us their hash for the key range (prev_key, last_key], calculate our hash
	RowHasher hasher(worker.hash_algorithm);
	worker.client.retrieve_rows(hasher, table, prev_key, last_key);

	if (hasher.finish() == hash) {
		if (failed_prev_key) {
			// send the previously-requested rows - since there's a span of successful rows after
			// it, we don't want to combine the next hash command.
			worker.send_rows_command(table, *failed_prev_key, prev_key);
		}

		if (!failed_last_key) {
			// match, move on to the next set of rows, and optimistically double the row count,
			// unless we're already up to very big blocks - each gigabyte takes ~5s to hash (with
			// CPU crypto support, and not including the overhead of transferring or encoding),
			// which is massively greater than the latency talking to a remote endpoint across
			// the world, so at some multiple of that size hashing any bigger blocks results in
			// minimal gain in the case when data matches and much more time wasted when it doesn't.
			size_t next_row_count = hasher.size <= target_maximum_block_size/2 ? hasher.row_count*2 : max<size_t>(hasher.row_count*target_maximum_block_size/hasher.size, 1);
			hash_next_range(worker, table, last_key, next_row_count, target_minimum_block_size);
		} else {
			// this range matched but somewhere > last_key & <= failed_last_key there is a mismatch,
			// so count how many rows we should use to subdivide that range.
			size_t rows_to_failure = worker.client.count_rows(table, last_key, *failed_last_key);

			// check if there's enough in that range (0 or 1 row(s), or less than target_minimum_block_size
			// bytes of data) on our side to bother subdividing and trying the shorter range.
			// approximation: since we know the other end is doing a binary search just like us, it
			// will have picked half the row range at its end; assume that the size of the data in
			// the range we just hashed is approximately equal to the range remaining before the
			// failed key, so use that to do the min size test.  if we have deleted subsequent rows,
			// this is a conservative number to use; if they have deleted rows, we will need to
			// send them to them anyway, so this approximation is unlikely to result in much excess
			// data transfer - it would really only happen if the next lot are much bigger per row.
			if (rows_to_failure > 1 && hasher.size > target_minimum_block_size) {
				// yup, subdivide the range containing the failure, starting at the next row
				hash_failed_range(worker, table, rows_to_failure/2, nullptr, last_key, *failed_last_key);
			} else {
				// nope, there's not enough in that range on our side, so it's time to send rows instead of trading hashes
				rows_and_next_hash(worker, table, last_key, *failed_last_key, !rows_to_failure, target_minimum_block_size);
			}
		}

	} else if (hasher.row_count > 1 && hasher.size > target_minimum_block_size) {
		// no match; send the previously-requested rows if any, and subdivide the range starting at the same row
		hash_failed_range(worker, table, hasher.row_count/2, failed_prev_key, prev_key, last_key);

	} else {
		// rows don't match, and there's not enough in that range (0 or 1 row(s), or less than
		// target_minimum_block_size bytes of data) on our side to bother subdividing and trying the shorter
		// range, so it's time to send rows instead of trading hashes.  if the other end requested
		// preceding rows, combine the request.
		rows_and_next_hash(worker, table, failed_prev_key ? *failed_prev_key : prev_key, last_key, !hasher.row_count, target_minimum_block_size);
	}
}

template <typename Worker, typename Hasher>
void hash_to_target_minimum_block_size(Worker &worker, const Table &table, Hasher &hasher, size_t target_minimum_block_size) {
	if (hasher.size == 0) return;
	while (hasher.size <= target_minimum_block_size/2 &&
		   worker.client.retrieve_rows(hasher, table, hasher.last_key, ColumnValues(), max<size_t>((target_minimum_block_size/2 - hasher.size)*hasher.row_count/hasher.size, 1)))
		/* continue */;
}

template <typename Worker>
void hash_failed_range(Worker &worker, const Table &table, size_t rows_to_hash, const ColumnValues *failed_prev_key, const ColumnValues &prev_key, const ColumnValues &failed_last_key) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");

	RowHasherAndLastKey hasher(worker.hash_algorithm, table.primary_key_columns);
	worker.client.retrieve_rows(hasher, table, prev_key, ColumnValues(), rows_to_hash);

	if (failed_prev_key) {
		worker.send_rows_and_hash_fail_command(table, *failed_prev_key, prev_key, hasher.last_key, failed_last_key, hasher.finish().to_string());
	} else {
		worker.send_hash_fail_command(table, prev_key, hasher.last_key, failed_last_key, hasher.finish().to_string());
	}
}

template <typename Worker>
void hash_next_range(Worker &worker, const Table &table, const ColumnValues &prev_key, size_t rows_to_hash, size_t target_minimum_block_size) {
	if (!rows_to_hash) throw logic_error("Can't hash 0 rows");
	
	RowHasherAndLastKey hasher(worker.hash_algorithm, table.primary_key_columns);
	worker.client.retrieve_rows(hasher, table, prev_key, ColumnValues(), rows_to_hash);
	hash_to_target_minimum_block_size(worker, table, hasher, target_minimum_block_size);

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
void hash_first_range(Worker &worker, const Table &table, size_t target_minimum_block_size) {
	hash_next_range(worker, table, ColumnValues(), 1, target_minimum_block_size);
}

template <typename Worker>
void rows_and_next_hash(Worker &worker, const Table &table, const ColumnValues &prev_key, ColumnValues last_key, bool extend_last_key, size_t target_minimum_block_size) {
	// if there were no rows in the range, we need to extend last_key forward to avoid
	// pathologically poor performance when our end has deleted a range of keys, as otherwise
	// they'll keep requesting deleted rows one-by-one.  we extend it to include the next row
	// (the hypothetical key value before that row's would be preferrable if we could find it).
	if (extend_last_key && !last_key.empty()) {
		RowLastKey row_last_key(table.primary_key_columns);
		worker.client.retrieve_rows(row_last_key, table, last_key, ColumnValues(), 1);
		last_key = row_last_key.last_key; // may still be empty if we have no more rows
	}

	// if that range extended to the end of the table, we just need to send the rows and we're
	// done.  otherwise, we want to follow up straight away with the next command.  we combo the
	// two together to allow the other end to start looking at the hash while it's still receiving
	// the rows off the network.
	if (last_key.empty()) {
		worker.send_rows_command(table, prev_key, last_key /* will be [] */);
	} else {
		// find the hash for the range *after* the rows that we will send
		RowHasherAndLastKey hasher(worker.hash_algorithm, table.primary_key_columns);
		worker.client.retrieve_rows(hasher, table, last_key, ColumnValues(), 1 /* rows to hash */);

		// hash more rows if we're not even close to the target block size, so we don't spend
		// forever trading hashes and rows for small ranges if most of the table doesn't match
		hash_to_target_minimum_block_size(worker, table, hasher, target_minimum_block_size);

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
