#ifndef TABLE_ROW_APPLIER_H
#define TABLE_ROW_APPLIER_H

#include "database_client_traits.h"
#include "sql_functions.h"
#include "unique_key_clearer.h"
#include "reset_table_sequences.h"

typedef map<PackedRow, PackedRow> RowsByPrimaryKey;

PackedRow primary_key(const Table &table, const PackedRow &row) {
	PackedRow primary_key;
	primary_key.reserve(table.primary_key_columns.size());
	for (size_t column_number : table.primary_key_columns) {
		primary_key.push_back(row[column_number]);
	}
	return primary_key;
}

template <typename DatabaseClient>
struct RowLoader {
	RowLoader(const Table &table, RowsByPrimaryKey &rows): table(table), rows(rows) {}

	void operator()(const typename DatabaseClient::RowType &database_row) {
		PackedRow row;
		database_row.pack_row_into(row);
		rows[primary_key(table, row)] = row;
	}

	const Table &table;
	RowsByPrimaryKey &rows;
};

template <typename DatabaseClient>
void append_row_tuple(DatabaseClient &client, const Columns &columns, BaseSQL &sql, const PackedRow &row) {
	if (sql.have_content()) sql += "),\n(";
	for (size_t n = 0; n < row.size(); n++) {
		if (n > 0) {
			sql += ',';
		}
		sql += encode(client, columns[n], row[n]);
	}
}

// databases that don't support the REPLACE statement must explicitly clear conflicting rows
template <typename DatabaseClient, bool = is_base_of<SupportsReplace, DatabaseClient>::value>
struct Replacer {
	Replacer(DatabaseClient &client, const Table &table):
		client(client),
		columns(table.columns),
		insert_sql("INSERT INTO " + table.name + " VALUES\n(", ")"),
		primary_key_clearer(client, table, table.primary_key_columns) {
		// set up the clearers we'll need to insert rows - these clear any conflicting values from later in the same table
		for (const Key &key : table.keys) {
			if (key.unique) {
				unique_keys_clearers.emplace_back(client, table, key.columns);
			}
		}
	}

	void row(const PackedRow &row, bool exists, bool end_of_table) {
		// when we apply(), first we will delete existing rows - we do that rather than use UPDATE
		// statements because you can't really batch UPDATE, whereas you can batch DELETE & INSERT.
		if (exists) {
			primary_key_clearer.row(row);
		}

		// before we can insert our rows we will also have to first clear any later rows with the
		// same unique key values.  if we're inserting rows at the end of the table, by definition
		// there are no later rows, so nothing we need to clear.
		if (!end_of_table) {
			for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) {
				unique_key_clearer.row(row);
			}
		}

		// we can then batch up a big INSERT statement
		append_row_tuple(client, columns, insert_sql, row);
	}

	inline void apply() {
		primary_key_clearer.apply();

		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) {
			unique_key_clearer.apply();
		}

		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	UniqueKeyClearer<DatabaseClient> primary_key_clearer;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys_clearers;
};

// databases that do support REPLACE are much simpler - we just use the same statement for any type of insert/update
template <typename DatabaseClient>
struct Replacer<DatabaseClient, true> {
	Replacer(DatabaseClient &client, const Table &table):
		client(client),
		columns(table.columns),
		insert_sql("REPLACE INTO " + table.name + " VALUES\n(", ")"),
		primary_key_clearer(client, table, table.primary_key_columns) {
	}

	void row(const PackedRow &row, bool _exists, bool _end_of_table) {
		append_row_tuple(client, columns, insert_sql, row);
	}

	inline void apply() {
		// although we don't need or use primary_key_clearer ourself, if the TableRowApplier has listed some rows to
		// delete, we want to do that too
		primary_key_clearer.apply();

		// aside from that, we're simply running batched REPLACE statements
		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	UniqueKeyClearer<DatabaseClient> primary_key_clearer;
};

template <typename DatabaseClient>
struct TableRowApplier {
	TableRowApplier(DatabaseClient &client, const Table &table, bool commit_often):
		client(client),
		table(table),
		replacer(client, table),
		commit_often(commit_often),
		rows_changed(0) {
	}

	~TableRowApplier() {
		apply();

		// reset sequences on those databases that don't automatically bump the high-water mark for inserts
		ResetTableSequences<DatabaseClient>::execute(client, table);
	}

	void apply() {
		replacer.apply();

		if (commit_often) {
			client.commit_transaction();
			client.start_write_transaction();
		}
	}

	template <typename InputStream>
	size_t stream_from_input(Unpacker<InputStream> &input, const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		// we're being sent the range of rows > matched_up_to_key and <= last_not_matching_key; apply them to our end

		RowsByPrimaryKey existing_rows;

		if (last_not_matching_key.empty()) {
			// if the range is to the end of the table, clear all remaining rows at our end
			delete_range(matched_up_to_key, last_not_matching_key);
		} else {
			// otherwise, load our rows in the range so we can compare them
			RowLoader<DatabaseClient> row_loader(table, existing_rows);
			client.retrieve_rows(row_loader, table, matched_up_to_key, last_not_matching_key);
		}

		PackedRow row;
		size_t rows_in_range = 0;

		while (true) {
			// command responses are a series of arrays, terminated by an empty array.  this avoids having
			// to know the number of results in advance; an empty row is not valid, so it's unambiguous.
			input >> row;
			if (row.size() == 0) break;
			rows_in_range++;

			if (replace_row(existing_rows, row, last_not_matching_key.empty())) {
				rows_changed++;

				// to reduce the trips to the database server, we don't execute a statement for each row -
				// but we do it periodically, as it's not efficient to build up enormous strings either
				if (replacer.insert_sql.curr.size() > BaseSQL::MAX_SENSIBLE_INSERT_COMMAND_SIZE) {
					apply();
				}
			}
		}

		// clear any remaining rows the other end didn't have
		for (RowsByPrimaryKey::const_iterator it = existing_rows.begin(); it != existing_rows.end(); ++it) {
			replacer.primary_key_clearer.row(it->second);
		}
		rows_changed  += existing_rows.size();
		rows_in_range += existing_rows.size();

		return rows_in_range;
	}

	bool replace_row(RowsByPrimaryKey &existing_rows, const PackedRow &row, bool end_of_table) {
		// if we're inserting the range to the end of the table, we know we need to insert this row
		if (end_of_table) {
			replacer.row(row, false, true);
			return true;
		}

		RowsByPrimaryKey::iterator existing_row = existing_rows.find(primary_key(table, row));

		if (existing_row != existing_rows.end()) {
			// we do have the row, but if it's changed we need to replace it
			bool matches = (existing_row->second == row);

			// don't want to delete this row later
			existing_rows.erase(existing_row);

			if (matches) return false;

			// row is different
			replacer.row(row, true, false);
		} else {
			// if we don't have this row, we need to insert it
			replacer.row(row, false, false);
		}

		return true;
	}

	void delete_range(const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		client.execute("DELETE FROM " + table.name + where_sql(client, table, matched_up_to_key, last_not_matching_key));
	}

	DatabaseClient &client;
	const Table &table;
	Replacer<DatabaseClient> replacer;
	bool commit_often;
	size_t rows_changed;
};

#endif
