#ifndef SQL_ROW_REPLACER
#define SQL_ROW_REPLACER

#include "database_client_traits.h"
#include "sql_functions.h"
#include "unique_key_clearer.h"

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
struct RowReplacer {
	RowReplacer(DatabaseClient &client, const Table &table, bool commit_often):
		client(client),
		columns(table.columns),
		insert_sql("INSERT INTO " + table.name + " VALUES\n(", ")"),
		primary_key_clearer(client, table, table.primary_key_columns),
		commit_often(commit_often),
		rows_changed(0) {
		// set up the clearers we'll need to insert rows - these clear any conflicting values from later in the same table
		for (const Key &key : table.keys) {
			if (key.unique) {
				unique_keys_clearers.emplace_back(client, table, key.columns);
			}
		}
	}

	inline void append_row(const PackedRow &row) {
		// if we're inserting rows at the end of the table, by definition there are no later rows,
		// so unlike insert_row we don't need to clear later conflicting unique key values.
		append_row_tuple(client, columns, insert_sql, row);

		rows_changed++;
	}

	void insert_row(const PackedRow &row) {
		// before we can insert our rows we will also have to first clear any later rows with the
		// same unique key values.
		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) {
			unique_key_clearer.row(row);
		}

		// we can then batch up a big INSERT statement
		append_row(row);
	}

	inline void replace_row(const PackedRow &row) {
		// when we apply(), first we will delete existing rows - we do that rather than use UPDATE
		// statements because you can't really batch UPDATE, whereas you can batch DELETE & INSERT.
		primary_key_clearer.row(row);
		insert_row(row);
	}

	inline void remove_row(const PackedRow &row) {
		primary_key_clearer.row(row);

		rows_changed++;
	}

	inline void apply() {
		primary_key_clearer.apply();

		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_keys_clearers) {
			unique_key_clearer.apply();
		}

		insert_sql.apply(client);

		if (commit_often) {
			client.commit_transaction();
			client.start_write_transaction();
		}
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	UniqueKeyClearer<DatabaseClient> primary_key_clearer;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys_clearers;
	bool commit_often;
	size_t rows_changed;
};

// databases that do support REPLACE are much simpler - we just use the same statement for any type of insert/update
template <typename DatabaseClient>
struct RowReplacer<DatabaseClient, true> {
	RowReplacer(DatabaseClient &client, const Table &table, bool commit_often):
		client(client),
		columns(table.columns),
		insert_sql("REPLACE INTO " + table.name + " VALUES\n(", ")"),
		primary_key_clearer(client, table, table.primary_key_columns),
		commit_often(commit_often),
		rows_changed(0) {
	}

	inline void append_row(const PackedRow &row) {
		replace_row(row);
	}

	inline void insert_row(const PackedRow &row) {
		replace_row(row);
	}

	inline void replace_row(const PackedRow &row) {
		append_row_tuple(client, columns, insert_sql, row);

		rows_changed++;
	}

	inline void remove_row(const PackedRow &row) {
		primary_key_clearer.row(row);

		rows_changed++;
	}

	inline void apply() {
		primary_key_clearer.apply();

		insert_sql.apply(client);

		if (commit_often) {
			client.commit_transaction();
			client.start_write_transaction();
		}
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	UniqueKeyClearer<DatabaseClient> primary_key_clearer;
	bool commit_often;
	size_t rows_changed;
};

#endif
