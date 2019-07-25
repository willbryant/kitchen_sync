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
		sql_encode_and_append_packed_value_to(sql.curr, client, columns[n], row[n]);
	}
}

typedef std::function<void ()> ProgressCallback;

template <typename DatabaseClient>
struct RowReplacer;

template <typename DatabaseClient, bool = is_base_of<SupportsReplace, DatabaseClient>::value>
struct RowReplacerBuilder {
	static const char *insert_sql_base() {
		return "INSERT INTO ";
	}

	static const char *values_base(const DatabaseClient &client) {
		return (client.supports_generated_as_identity() ? " OVERRIDING SYSTEM VALUE VALUES\n(" : " VALUES\n(");
	}

	static void construct_clearers(RowReplacer<DatabaseClient> &row_replacer) {
		// databases that don't support the REPLACE statement must explicitly clear conflicting rows
		for (const Key &key : row_replacer.table.keys) {
			if (key.unique()) {
				row_replacer.unique_key_clearers.emplace_back(row_replacer.client, row_replacer.table, key.columns);
			}
		}

		row_replacer.replace_clearers_start = row_replacer.unique_key_clearers.begin();
		row_replacer.insert_clearers_start = row_replacer.unique_key_clearers.begin() + 1;
	}
};

template <typename DatabaseClient>
struct RowReplacerBuilder<DatabaseClient, true> {
	static const char *insert_sql_base() {
		return "REPLACE INTO ";
	}

	static const char *values_base(const DatabaseClient &client) {
		return " VALUES\n(";
	}

	static void construct_clearers(RowReplacer<DatabaseClient> &row_replacer) {
		// databases that support the REPLACE statement will clear any conflicting rows automatically
		row_replacer.replace_clearers_start = row_replacer.unique_key_clearers.end();
		row_replacer.insert_clearers_start = row_replacer.unique_key_clearers.end();
	}
};

template <typename DatabaseClient>
struct RowReplacer {
	RowReplacer(DatabaseClient &client, const Table &table, bool commit_often, ProgressCallback progress_callback):
		client(client),
		table(table),
		insert_sql(RowReplacerBuilder<DatabaseClient>::insert_sql_base() + client.quote_identifier(table.name) + RowReplacerBuilder<DatabaseClient>::values_base(client), ")"),
		commit_often(commit_often),
		progress_callback(progress_callback),
		rows_changed(0) {
		// set up the clearers we'll need to insert rows - these clear any conflicting values from elsewhere in the same table
		unique_key_clearers.emplace_back(client, table, table.primary_key_columns);
		RowReplacerBuilder<DatabaseClient>::construct_clearers(*this);
	}

	inline void insert_row(const PackedRow &row) {
		// before we can insert our rows we will also have to first clear any other rows with the
		// same unique key values.
		for (auto unique_key_clearer = insert_clearers_start; unique_key_clearer != unique_key_clearers.end(); ++unique_key_clearer) {
			unique_key_clearer->row(row);
		}

		// we can then batch up a big INSERT statement
		append_row_tuple(client, table.columns, insert_sql, row);

		rows_changed++;
	}

	inline void replace_row(const PackedRow &row) {
		// when we apply(), first we will delete existing rows - we do that rather than use UPDATE
		// statements because you can't really batch UPDATE, whereas you can batch DELETE & INSERT.
		for (auto unique_key_clearer = replace_clearers_start; unique_key_clearer != unique_key_clearers.end(); ++unique_key_clearer) {
			unique_key_clearer->row(row);
		}

		append_row_tuple(client, table.columns, insert_sql, row);

		rows_changed++;
	}

	inline void remove_row(const PackedRow &row) {
		unique_key_clearers.front().row(row);

		rows_changed++;
	}

	void apply() {
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

	void clear_range(const ColumnValues &prev_key, const ColumnValues &last_key) {
		apply();
		rows_changed += client.execute("DELETE FROM " + client.quote_identifier(table.name) + where_sql(client, table, prev_key, last_key));
	}

	DatabaseClient &client;
	const Table &table;
	BaseSQL insert_sql;
	vector< UniqueKeyClearer<DatabaseClient> > unique_key_clearers;
	typename vector< UniqueKeyClearer<DatabaseClient> >::iterator insert_clearers_start;
	typename vector< UniqueKeyClearer<DatabaseClient> >::iterator replace_clearers_start;
	bool commit_often;
	ProgressCallback progress_callback;
	size_t rows_changed;
};

#endif
