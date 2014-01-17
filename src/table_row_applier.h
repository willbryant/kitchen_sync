#ifndef TABLE_ROW_APPLIER_H
#define TABLE_ROW_APPLIER_H

#include "sql_functions.h"
#include "unique_key_clearer.h"

typedef map<ColumnValues, NullableRow> RowsByPrimaryKey;

template <typename DatabaseClient>
struct RowLoader {
	RowLoader(const Table &table, RowsByPrimaryKey &rows): table(table), rows(rows) {}

	void operator()(const typename DatabaseClient::RowType &database_row) {
		ColumnValues primary_key;
		primary_key.resize(table.primary_key_columns.size());
		for (size_t column = 0; column < table.primary_key_columns.size(); column++) {
			primary_key[column] = database_row.string_at(table.primary_key_columns[column]);
		}

		NullableRow &row(rows[primary_key]);
		row.resize(database_row.n_columns());
		for (size_t column = 0; column < database_row.n_columns(); column++) {
			if (!database_row.null_at(column)) {
				row[column] = database_row.string_at(column);
			}
		}
	}

	const Table &table;
	RowsByPrimaryKey &rows;
};

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

		RowsByPrimaryKey existing_rows;

		if (last_not_matching_key.empty()) {
			// if the range is to the end of the table, clear all remaining rows at our end
			add_to_delete_range(matched_up_to_key, last_not_matching_key);
		} else {
			// otherwise, load our rows in the range so we can compare them
			RowLoader<DatabaseClient> row_loader(table, existing_rows);
			client.retrieve_rows(table, matched_up_to_key, last_not_matching_key, row_loader);
		}

		NullableRow row;
		size_t rows_sent = 0;

		while (true) {
			// the rows command is unusual.  to avoid needing to know the number of results in advance,
			// instead of a single response object, there's one response object per row, terminated by
			// an empty row (which is not valid data, so is unambiguous).
			input >> row;
			if (row.size() == 0) break;
			rows_sent++;

			if (last_not_matching_key.empty()) {
				// if we're inserting the range to the end of the table, we know we need to insert this row
				// since there can be no later rows, we don't need to clear unique keys these rows use
				add_to_insert(row);
			} else {
				// otherwise, if we don't have this row or if our row is different, we need replace our row
				consider_replace(existing_rows, row);
			}
		}

		for (RowsByPrimaryKey::const_iterator it = existing_rows.begin(); it != existing_rows.end(); ++it) {
			add_to_delete(it->first);
		}

		rows += rows_sent;
		return rows_sent;
	}

	void consider_replace(RowsByPrimaryKey &existing_rows, const NullableRow &row) {
		ColumnValues primary_key;
		primary_key.resize(table.primary_key_columns.size());
		for (size_t column = 0; column < table.primary_key_columns.size(); column++) {
			primary_key[column] = row[table.primary_key_columns[column]].value; // note that primary key columns cannot be null
		}
		RowsByPrimaryKey::iterator existing_row = existing_rows.find(primary_key);

		if (existing_row == existing_rows.end()) {
			// we don't have this row, so we need to insert it
			add_to_clear_unique_keys(row);
			add_to_insert(row);

		} else {
			// we have this row, see if the data is the same
			if (existing_row->second != row) {
				// row is different, so we need to delete it and insert it
				add_to_delete(primary_key);
				add_to_clear_unique_keys(row);
				add_to_insert(row);
			}

			// don't want to delete this row later
			existing_rows.erase(existing_row);
		}
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

	void add_to_clear_unique_keys(const NullableRow &row) {
		// before we can insert our rows we also have to first clear any later rows with the same
		// unique key values - unless the database supports REPLACE in which case the constructor
		// will have left unique_keys empty.
		for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) {
			unique_key.row(row);
		}
	}

	void add_to_delete(const ColumnValues &primary_key) {
		delete_sql += delete_sql.have_content() ? ")\nOR (" : "";
		delete_sql += primary_key_columns;
		delete_sql += " = ";
		delete_sql += non_binary_string_values_list(primary_key);

		// like inserts, it's not efficient to build up enormous delete strings (in fact testing showed the cutoff should be quite low)
		if (delete_sql.curr.size() > BaseSQL::MAX_SENSIBLE_DELETE_COMMAND_SIZE) {
			delete_sql.apply(client);
		}
	}

	void add_to_delete_range(const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		delete_sql += client.where_sql(primary_key_columns, matched_up_to_key, last_not_matching_key, delete_sql.have_content() ? ")\nOR (" : "");

		// like inserts, it's not efficient to build up enormous delete strings (in fact testing showed the cutoff should be quite low)
		if (delete_sql.curr.size() > BaseSQL::MAX_SENSIBLE_DELETE_COMMAND_SIZE) {
			delete_sql.apply(client);
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
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys;
	BaseSQL insert_sql;
	size_t rows;
};

#endif
