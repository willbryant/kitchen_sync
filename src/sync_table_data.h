#include "hash_result.h"

template <typename DatabaseClient>
void sync_table_data(
	DatabaseClient &client, Stream &input, const Table &table) {
	// start off small, and from the very beginning (in primary key order) of the table
	int rows_to_hash = 1;
	ColumnValues matched_up_to_key;

	while (true) {
		// ask the other end for its hash of the next rows_to_hash rows
		HashResult hash_result;
		send_command(cout, "hash", table.name, matched_up_to_key, rows_to_hash);
		input >> hash_result;

		// calculate our hash of the same set of rows, using key ranges rather than a count to improve the chances of a match
		RowHasher<typename DatabaseClient::RowType> row_hasher;
		client.retrieve_rows(table, matched_up_to_key, hash_result.last_key, row_hasher);

		// if the two hashes match, we believe the rows match
		if (row_hasher.finish() == hash_result.hash) {
			// if the other end said there was no last_key, that means that we got all the way up to the end of the table, so we're done.
			if (hash_result.last_key.empty()) break;

			// otherwise we carry on from there
			matched_up_to_key = hash_result.last_key;

			// be optimistic: hash more data next time
			rows_to_hash <<= 1;

		} else if (hash_result.last_key.empty()) {
			// we don't match, and furthermore, the other end has found they have no more rows after the last matched row
			// (if any).  so we know we need to delete our remaining rows, after which, we're done.
			ColumnValues empty_key;
			client.execute(client.delete_rows_sql(table, matched_up_to_key, empty_key));
			break;

		} else if (row_hasher.row_count == 0) {
			// we don't match, and furthermore, we've found we actually have no more rows after the last matched row (if any),
			// so we need to get the other end to send over the entire remainder of the table - after which, we're done.
			ColumnValues empty_key;
			sync_table_rows(client, input, table, matched_up_to_key, empty_key);
			break;

		} else if (rows_to_hash > 1) {
			// both this end and that have data after the last matching row, but it doesn't match; try matching a smaller set of rows
			rows_to_hash >>= 1;

			// there's no point requesting more rows than we actually have - if they have more than that number, then they must not
			// match, and if they have no more than that number, there's no harm in requesting that number
			if (rows_to_hash > row_hasher.row_count) {
				rows_to_hash = row_hasher.row_count;
			}

		} else {
			// we've got down to single rows already, so go ahead and request that row's data and update, then carry on after that
			sync_table_rows(client, input, table, matched_up_to_key, hash_result.last_key);
			matched_up_to_key = hash_result.last_key;
		}
	}
}
