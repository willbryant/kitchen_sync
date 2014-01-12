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
	void stream_from_input(Unpacker<InputStream> &input, const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		// we're being sent the range of rows > matched_up_to_key and <= last_not_matching_key; apply them to our end

		// for now, we clear and re-insert; testing matching up and using UPDATE statements is on the future list, though it would mean a lot more statements
		delete_sql += client.where_sql(primary_key_columns, matched_up_to_key, last_not_matching_key, delete_sql.have_content() ? ")\nOR (" : "");
		if (delete_sql.curr.size() > BaseSQL::MAX_SENSIBLE_DELETE_COMMAND_SIZE) {
			delete_sql.apply(client);
		}

		NullableRow row;

		while (true) {
			// the rows command is unusual.  to avoid needing to know the number of results in advance,
			// instead of a single response object, there's one response object per row, terminated by
			// an empty row (which is not valid data, so is unambiguous).
			input >> row;
			if (row.size() == 0) break;
			rows++;

			// built a string of rows to insert as we go
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

			// we also have to clear any later rows with the same unique key values so that we can
			// insert our rows first; if there can be no later rows, we can skip this bit
			if (!last_not_matching_key.empty()) {
				for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) {
					unique_key.row(row);
				}
			}

			// to reduce the trips to the database server, we don't execute a statement for each row -
			// but we do it periodically, as it's not efficient to build up enormous strings either
			if (insert_sql.curr.size() > BaseSQL::MAX_SENSIBLE_INSERT_COMMAND_SIZE) {
				apply();
			}
		}
	}

	inline void apply() {
		delete_sql.apply(client);
		for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) unique_key.apply();
		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Table &table;
	string primary_key_columns;
	BaseSQL delete_sql;
	BaseSQL insert_sql;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys;
	size_t rows;
};

#endif
