#ifndef TABLE_ROW_APPLIER_H
#define TABLE_ROW_APPLIER_H

#include "sql_functions.h"
#include "unique_key_clearer.h"

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
struct TableRowApplier {
	TableRowApplier(DatabaseClient &client, const Table &table):
		client(client),
		table(table),
		primary_key_columns(columns_list(client, table.columns, table.primary_key_columns)),
		primary_key_clearer(client, table, table.primary_key_columns),
		insert_sql(client.replace_sql_prefix() + table.name + " VALUES\n(", ")"),
		rows_changed(0) {

		// if the client doesn't support REPLACE, we will need to delete rows with the PKs we want to
		// insert, and also clear later rows that have our unique key values in order to insert
		client.add_replace_clearers(unique_keys_clearers, table);
	}

	~TableRowApplier() {
		apply();
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

			if (last_not_matching_key.empty()) {
				// if we're inserting the range to the end of the table, we know we need to insert this row
				// since there can be no later rows, we don't need to clear unique keys these rows use
				add_to_insert(row);
				rows_changed++;
			} else {
				// otherwise, if we don't have this row or if our row is different, we need replace our row
				if (consider_replace(existing_rows, row)) {
					rows_changed++;
				}
			}
		}

		// clear any remaining rows the other end didn't have
		for (RowsByPrimaryKey::const_iterator it = existing_rows.begin(); it != existing_rows.end(); ++it) {
			add_to_primary_key_clearer(it->second);
		}
		rows_changed  += existing_rows.size();
		rows_in_range += existing_rows.size();

		return rows_in_range;
	}

	bool consider_replace(RowsByPrimaryKey &existing_rows, const PackedRow &row) {
		RowsByPrimaryKey::iterator existing_row = existing_rows.find(primary_key(table, row));

		// if we don't have this row, we need to insert it
		if (existing_row != existing_rows.end()) {
			// we do have the row, but if it's changed we need to replace it
			bool matches = (existing_row->second == row);

			// don't want to delete this row later
			existing_rows.erase(existing_row);

			if (matches) return false;

			// row is different, so we need to delete it and insert it
			if (client.need_primary_key_clearer_to_replace()) {
				add_to_primary_key_clearer(row);
			}
		}

		add_to_unique_keys_clearers(row);
		add_to_insert(row);

		return true;
	}

	void add_to_insert(const PackedRow &row) {
		if (insert_sql.have_content()) insert_sql += "),\n(";
		for (size_t n = 0; n < row.size(); n++) {
			if (n > 0) {
				insert_sql += ',';
			}
			insert_sql += encode(client, row[n]);
		}

		// to reduce the trips to the database server, we don't execute a statement for each row -
		// but we do it periodically, as it's not efficient to build up enormous strings either
		if (insert_sql.curr.size() > BaseSQL::MAX_SENSIBLE_INSERT_COMMAND_SIZE) {
			apply();
		}
	}

	void add_to_primary_key_clearer(const PackedRow &row) {
		primary_key_clearer.row(row);
	}

	void add_to_unique_keys_clearers(const PackedRow &row) {
		// before we can insert our rows we also have to first clear any later rows with the same
		// unique key values - unless the database supports REPLACE in which case the constructor
		// will have left unique_keys_clearers empty.
		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) {
			unique_key_clearer.row(row);
		}
	}

	void delete_range(const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		client.execute("DELETE FROM " + table.name + where_sql(client, primary_key_columns, matched_up_to_key, last_not_matching_key));
	}

	inline void apply() {
		primary_key_clearer.apply();

		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) unique_key_clearer.apply();

		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Table &table;
	string primary_key_columns;
	UniqueKeyClearer<DatabaseClient> primary_key_clearer;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys_clearers;
	BaseSQL insert_sql;
	size_t rows_changed;
};

#endif
