#ifndef SQL_ROW_REPLACER
#define SQL_ROW_REPLACER

#include <functional>

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

typedef std::function<void ()> ProgressCallback;

// databases that don't support the REPLACE statement must explicitly clear conflicting rows
template <typename DatabaseClient, bool = is_base_of<SupportsReplace, DatabaseClient>::value>
struct RowReplacer {
	RowReplacer(DatabaseClient &client, const Table &table, bool commit_often, ProgressCallback progress_callback):
		client(client),
		columns(table.columns),
		insert_sql("INSERT INTO " + table.name + " VALUES\n(", ")"),
		commit_often(commit_often),
		progress_callback(progress_callback),
		rows_changed(0) {
		// set up the clearers we'll need to insert rows - these clear any conflicting values from elsewhere in the same table
		unique_key_clearers.emplace_back(client, table, table.primary_key_columns);
		for (const Key &key : table.keys) {
			if (key.unique) {
				unique_key_clearers.emplace_back(client, table, key.columns);
			}
		}
	}

	void insert_row(const PackedRow &row) {
		// before we can insert our rows we will also have to first clear any other rows with the
		// same unique key values.
		for (auto unique_key_clearer = unique_key_clearers.begin() + 1; unique_key_clearer != unique_key_clearers.end(); ++unique_key_clearer) {
			unique_key_clearer->row(row);
		}

		// we can then batch up a big INSERT statement
		append_row_tuple(client, columns, insert_sql, row);

		rows_changed++;
	}

	inline void replace_row(const PackedRow &row) {
		// when we apply(), first we will delete existing rows - we do that rather than use UPDATE
		// statements because you can't really batch UPDATE, whereas you can batch DELETE & INSERT.
		for (auto unique_key_clearer = unique_key_clearers.begin(); unique_key_clearer != unique_key_clearers.end(); ++unique_key_clearer) {
			unique_key_clearer->row(row);
		}

		append_row_tuple(client, columns, insert_sql, row);

		rows_changed++;
	}

	inline void remove_row(const PackedRow &row) {
		unique_key_clearers.front().row(row);

		rows_changed++;
	}

	inline void apply() {
		for (UniqueKeyClearer<DatabaseClient> &unique_key_clearer : unique_key_clearers) {
			unique_key_clearer.apply();
		}

		insert_sql.apply(client);

		if (commit_often) {
			client.commit_transaction();
			client.start_write_transaction();
		}

		if (progress_callback) {
			progress_callback();
		}
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	vector< UniqueKeyClearer<DatabaseClient> > unique_key_clearers;
	bool commit_often;
	ProgressCallback progress_callback;
	size_t rows_changed;
};

// databases that do support REPLACE are much simpler - we just use the same statement for any type of insert/update
template <typename DatabaseClient>
struct RowReplacer<DatabaseClient, true> {
	RowReplacer(DatabaseClient &client, const Table &table, bool commit_often, ProgressCallback progress_callback):
		client(client),
		columns(table.columns),
		insert_sql("REPLACE INTO " + table.name + " VALUES\n(", ")"),
		commit_often(commit_often),
		rows_changed(0) {
		unique_key_clearers.emplace_back(client, table, table.primary_key_columns);
	}

	inline void insert_row(const PackedRow &row) {
		replace_row(row);
	}

	inline void replace_row(const PackedRow &row) {
		append_row_tuple(client, columns, insert_sql, row);

		rows_changed++;
	}

	inline void remove_row(const PackedRow &row) {
		unique_key_clearers.front().row(row);

		rows_changed++;
	}

	inline void apply() {
		unique_key_clearers.front().apply();

		insert_sql.apply(client);

		if (commit_often) {
			client.commit_transaction();
			client.start_write_transaction();
		}

		if (progress_callback) {
			progress_callback();
		}
	}

	DatabaseClient &client;
	const Columns &columns;
	BaseSQL insert_sql;
	vector< UniqueKeyClearer<DatabaseClient> > unique_key_clearers;
	bool commit_often;
	ProgressCallback progress_callback;
	size_t rows_changed;
};

#endif
