#ifndef ROW_RANGE_APPLIER_H
#define ROW_RANGE_APPLIER_H

#include "row_replacer.h"

template <typename DatabaseClient>
struct RowRangeApplier {
	static const size_t MAX_BYTES_TO_BUFFER = 16*1024*1024; // no particular rationale for this value - just large enough that it isn't usually the deciding factor in when we apply statements
	static const size_t MAX_ROWS_TO_SELECT = 10000; // also somewhat arbitrary, but because we can't send DELETE statements while we are still receiving the results of a SELECT query on the same connection, this can effectively determine how many IDs we list in a single DELETE statement
	static const size_t MAX_SENSIBLE_INSERT_STATEMENT_SIZE = 4*1024*1024;
	static const size_t MAX_SENSIBLE_DELETE_STATEMENT_SIZE =     16*1024;

	typedef map<ColumnValues, PackedRow> RowsByPrimaryKey;

	RowRangeApplier(RowReplacer<DatabaseClient> &replacer, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key):
		replacer(replacer),
		client(replacer.client),
		table(table),
		prev_key(prev_key),
		curr_key(prev_key),
		last_key(last_key),
		approx_buffered_bytes(0) {
		// RowInserter below can be used without keys if we completely clear and reload the table, but RowRangeApplier can't do anything useful
		if (table.primary_key_columns.empty()) throw runtime_error("Can't stream and detect differences without a primary key");
	}

	template <typename InputStream>
	void stream_from_input(Unpacker<InputStream> &input) {
		PackedRow row;

		while (true) {
			// in the KS protocol command responses are a series of arrays, terminated by an empty array.
			// this avoids having to determine the number of results in advance; an empty array is not a
			// valid database row, so it's unambiguous.
			input >> row;
			if (row.size() == 0) break;

			received_source_row(row);
		}

		received_all_source_rows();
	}

	ColumnValues primary_key_of(const PackedRow &row) {
		ColumnValues primary_key;
		Packer<ColumnValues> packer(primary_key);
		pack_array_length(packer, table.primary_key_columns.size());
		for (size_t column_number : table.primary_key_columns) {
			PackedValueReadStream stream(row, column_number);
			Unpacker<PackedValueReadStream> unpacker(stream);
			copy_object(unpacker, primary_key);
		}
		return primary_key;
	}

	void received_source_row(const PackedRow &row) {
		curr_key = primary_key_of(row);
		source_rows[curr_key] = row;

		// if the other end is sending a large set of data (for example, the entire remainder of the
		// table), we need to periodically apply the data received so far rather than buffering up
		// huge amounts.  nb. we used to buffer the local data rather than the source data, which
		// mostly avoided this particular problem, but we still had trouble in the case where the
		// source dataset had deleted a large range that was still present on the local end; this
		// way around requires fewer special cases.
		approx_buffered_bytes += row.encoded_size();
		if (approx_buffered_bytes > MAX_BYTES_TO_BUFFER) {
			check_rows_to_curr_key();
			insert_remaining_rows();
		}
	}

	void received_all_source_rows() {
		// clear any rows after the last entry we should have in the table (within the range we are
		// processing, which may or may not go to the end of the table); this is an optimisation, as
		// the retrieve_rows callback would do the same thing for each extra row found.
		if (last_key.empty() || curr_key != last_key) {
			delete_range(curr_key, last_key);
		}

		check_rows_to_curr_key();
		insert_remaining_rows();
	}

	void delete_range(const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		client.execute("DELETE FROM " + client.quote_table_name(table) + where_sql(client, table, matched_up_to_key, last_not_matching_key));
	}

	void check_rows_to_curr_key() {
		// we select in batches to avoid large buffering in clients that can't turn buffering off; and in those
		// that can, we also need to execute DML periodically (but can't do that while SELECT is returning results)
		while (retrieve_rows(client, *this, table, prev_key, curr_key, MAX_ROWS_TO_SELECT) == MAX_ROWS_TO_SELECT) {
			if (need_to_apply()) replacer.apply();
		}
		if (need_to_apply()) replacer.apply();
		prev_key = curr_key; // prev_key is iteratively updated in operator() to serve the loop above, but we may not have had the curr_key row locally
	}

	void operator()(const typename DatabaseClient::RowType &database_row) {
		PackedRow row;
		pack_row_into(row, database_row);
		prev_key = primary_key_of(row);

		RowsByPrimaryKey::iterator source_row = source_rows.find(prev_key);

		if (source_row == source_rows.end()) {
			// we have a row that we shouldn't have, so we need to remove it
			replacer.remove_row(row);

		} else if (source_row->second != row) {
			// we do have the row at both ends, but it's changed, so we need to replace it
			replacer.replace_row(source_row->second);

			// done with this row, don't need to insert it in insert_remaining_rows
			source_rows.erase(source_row);

		} else {
			// the row matches; done with this row, don't need to insert it in insert_remaining_rows
			source_rows.erase(source_row);
		}
	}

	void insert_remaining_rows() {
		for (RowsByPrimaryKey::iterator source_row = source_rows.begin(); source_row != source_rows.end(); ++source_row) {
			replacer.insert_row(source_row->second);
			if (need_to_apply()) replacer.apply();
		}
		source_rows.clear();
		approx_buffered_bytes = 0;
	}

	bool need_to_apply() {
		// to reduce the trips to the database server, we don't execute a statement for each row -
		// but we do it periodically, as it's not efficient to build up enormous strings either.
		// note that this method is only called while retrieve_rows is not running - we can't
		// execute another statement while one is already running, because we turn off database
		// client row buffering for efficiency.

		if (replacer.insert_sql.curr.size() > MAX_SENSIBLE_INSERT_STATEMENT_SIZE) return true;

		for (auto unique_key_clearer : replacer.unique_key_clearers) {
			if (unique_key_clearer.delete_sql.curr.size() > MAX_SENSIBLE_DELETE_STATEMENT_SIZE) return true;
		}

		return false;
	}

	RowReplacer<DatabaseClient> &replacer;
	DatabaseClient &client;
	const Table &table;
	ColumnValues prev_key;
	ColumnValues curr_key;
	ColumnValues last_key;
	RowsByPrimaryKey source_rows;
	size_t approx_buffered_bytes;
};

// special-case version of RowRangeApplier that simply inserts all the received rows without comparing
// them to the current database (because the caller knows that there are no comparable rows in the database)
template <typename DatabaseClient>
struct RowInserter {
	static const size_t MAX_SENSIBLE_INSERT_STATEMENT_SIZE = 4*1024*1024;

	RowInserter(RowReplacer<DatabaseClient> &replacer, const Table &table):
		replacer(replacer),
		table(table) {
	}

	template <typename InputStream>
	void stream_from_input(Unpacker<InputStream> &input) {
		PackedRow row;

		while (true) {
			// in the KS protocol command responses are a series of arrays, terminated by an empty array.
			// this avoids having to determine the number of results in advance; an empty array is not a
			// valid database row, so it's unambiguous.
			input >> row;
			if (row.size() == 0) break;

			replacer.insert_row(row);
			if (replacer.insert_sql.curr.size() > MAX_SENSIBLE_INSERT_STATEMENT_SIZE) {
				replacer.apply();
			}
		}
	}

	RowReplacer<DatabaseClient> &replacer;
	const Table &table;
};

#endif
