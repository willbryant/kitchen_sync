#include "hash_result.h"

template <typename DatabaseClient>
void sync_table_data(DatabaseClient &client, Unpacker &input, const Table &table) {
	// start off small, and from the very beginning (in primary key order) of the table
	size_t rows_to_hash = 1;
	ColumnValues matched_up_to_key;

	while (true) {
		// calculate our hash of the next rows_to_hash rows
		RowHasherAndLastKey<typename DatabaseClient::RowType> row_hasher(table.primary_key_columns);
		client.retrieve_rows(table, matched_up_to_key, rows_to_hash, row_hasher);

		if (row_hasher.row_count == 0) {
			// we've found we actually have no more rows (after the last matched row, if any), so we need to
			// get the other end to send over the entire remainder of the table - after which, we're done.
			ColumnValues empty_key;
			sync_table_rows(client, input, table, matched_up_to_key, empty_key);
			break;
		} 

		// ask the other end for its hash of the same rows, using key ranges rather than a count to improve the chances of a match
		HashResult hash_result;
		send_command(cout, "hash", table.name, matched_up_to_key, row_hasher.last_key);
		input >> hash_result;

		// if the two hashes match, we believe the rows match
		if (row_hasher.finish() == hash_result.hash) {
			// otherwise we carry on from after the last key we read
			matched_up_to_key = row_hasher.last_key;

			// be optimistic: hash more data next time
			rows_to_hash <<= 1;

		} else if (hash_result.row_count == 0) {
			// we don't match, and furthermore, the other end has found they have no more rows (after the last matched row,
			// if any).  so we know we need to delete all our remaining rows, after which, we're done.
			ColumnValues empty_key;
			client.execute(client.delete_rows_sql(table, matched_up_to_key, empty_key));
			break;

		} else if (rows_to_hash > 1) {
			// both this end and that have data after the last matching row, but it doesn't match; try matching a smaller set of rows
			rows_to_hash >>= 2;
			if (rows_to_hash < 1) rows_to_hash = 1;

		} else {
			// we've got down to single rows already, so go ahead and request that row's data and update our copy, then carry on after that
			sync_table_rows(client, input, table, matched_up_to_key, row_hasher.last_key);
			matched_up_to_key = row_hasher.last_key;
		}
	}
}
