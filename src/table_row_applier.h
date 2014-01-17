#ifndef TABLE_ROW_APPLIER_H
#define TABLE_ROW_APPLIER_H

#include "sql_functions.h"
#include "unique_key_clearer.h"

template <typename DatabaseClient>
struct TableRowApplier {
	TableRowApplier(DatabaseClient &client, const Table &table):
		client(client),
		table(table),
		primary_key_columns(columns_list(table.columns, table.primary_key_columns)),
		insert_sql(client.replace_sql_prefix() + table.name + " VALUES\n(", ")"),
		delete_sql("DELETE FROM " + table.name + " WHERE\n(", ")"),
		rows(0) {

		// if the client doesn't support REPLACE, we will need to clear later rows that have our unique key values in order to insert
		client.add_replace_clearers(unique_keys, table);
	}

	template <typename InputStream>
	size_t stream_from_input(Unpacker<InputStream> &input, const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		// we're being sent the range of rows > matched_up_to_key and <= last_not_matching_key; apply them to our end

		// for now, we clear and re-insert; testing matching up and using UPDATE statements is on the future list, though it would mean a lot more statements
		add_to_delete_range(matched_up_to_key, last_not_matching_key);

		NullableRow row;
		size_t rows_sent = 0;

		while (true) {
			// the rows command is unusual.  to avoid needing to know the number of results in advance,
			// instead of a single response object, there's one response object per row, terminated by
			// an empty row (which is not valid data, so is unambiguous).
			input >> row;
			if (row.size() == 0) break;
			rows_sent++;

			// we also have to clear any later rows with the same unique key values so that we can
			// insert our rows first; if there can be no later rows, we can skip this bit
			if (!last_not_matching_key.empty()) {
				for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) {
					unique_key.row(row);
				}
			}

			// built a string of rows to insert as we go
			add_to_insert(row);
		}

		rows += rows_sent;
		return rows_sent;
	}

	void add_to_insert(const NullableRow &row) {
		if (insert_sql.have_content()) insert_sql += "),\n(";
		for (size_t n = 0; n < row.size(); n++) {
			if (n > 0) {
				insert_sql += ',';
			}
			if (row[n].null) {
				insert_sql += "NULL";
			} else {
				insert_sql += '\'';
				insert_sql += client.escape_value(row[n].value);
				insert_sql += '\'';
			}
		}

		// to reduce the trips to the database server, we don't execute a statement for each row -
		// but we do it periodically, as it's not efficient to build up enormous strings either
		if (insert_sql.curr.size() > BaseSQL::MAX_SENSIBLE_INSERT_COMMAND_SIZE) {
			apply();
		}
	}

	void add_to_delete_range(const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		if (!pending_delete_last_key.empty() && pending_delete_last_key == matched_up_to_key) {
			// this range follows on immediately after the pending range, so we can just extend the pending range
			pending_delete_last_key = last_not_matching_key;

		} else {
			// there is no pending range, or this range isn't contiguous with it; if the latter, we need to add the pending range to the DELETE statement WHERE clause now
			if (!pending_delete_last_key.empty()) {
				apply_pending_delete_range();

				// like inserts, it's not efficient to build up enormous delete strings (in fact testing showed the cutoff should be quite low)
				if (delete_sql.curr.size() > BaseSQL::MAX_SENSIBLE_DELETE_COMMAND_SIZE) {
					delete_sql.apply(client);
				}
			}

			// and then make the new range pending
			pending_delete_prev_key = matched_up_to_key;
			pending_delete_last_key = last_not_matching_key;
		}
	}

	void apply_pending_delete_range() {
		delete_sql += client.where_sql(primary_key_columns, pending_delete_prev_key, pending_delete_last_key, delete_sql.have_content() ? ")\nOR (" : "");
	}

	inline void apply() {
		apply_pending_delete_range();
		pending_delete_prev_key.clear();
		pending_delete_last_key.clear();
		delete_sql.apply(client);

		for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) unique_key.apply();

		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Table &table;
	string primary_key_columns;
	BaseSQL delete_sql;
	ColumnValues pending_delete_prev_key;
	ColumnValues pending_delete_last_key;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys;
	BaseSQL insert_sql;
	size_t rows;
};

#endif
