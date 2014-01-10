#include "sql_functions.h"
#include "message_pack/unpack_nullable.h"

struct BaseSQL {
	static const size_t MAX_SENSIBLE_INSERT_COMMAND_SIZE = 8*1024*1024;

	inline BaseSQL(const string &prefix, const string &suffix): prefix(prefix), suffix(suffix) {
		reset();
	}

	inline void reset() {
		curr = prefix;
		curr.reserve(2*MAX_SENSIBLE_INSERT_COMMAND_SIZE);
	}

	inline void operator += (const string &sql) {
		curr += sql;
	}

	inline void operator += (char sql) {
		curr += sql;
	}

	inline bool have_content() {
		return curr.size() > prefix.size();
	}

	template <typename DatabaseClient>
	inline void apply(DatabaseClient &client) {
		if (have_content()) {
			client.execute(curr + suffix);
			reset();
		}
	}

	string prefix;
	string suffix;
	string curr;
};

typedef Nullable<string> NullableColumnValue;
typedef vector<NullableColumnValue> NullableRow;

template <typename DatabaseClient>
struct UniqueKeyClearer {
	UniqueKeyClearer(DatabaseClient &client, const Table &table, const Key &key):
		client(&client),
		columns(&key.columns),
		delete_sql("DELETE FROM " + table.name + " WHERE " + columns_list(table.columns, key.columns) + " IN (\n(", "))") {
	}

	bool key_enforceable(const NullableRow &row) {
		for (size_t n = 0; n < columns->size(); n++) {
			if (row[(*columns)[n]].null) return false;
		}
		return true;
	}

	void row(const NullableRow &row) {
		// rows with any NULL values won't enforce a uniqueness constraint, so we don't need to clear them
		if (!key_enforceable(row)) return;

		if (delete_sql.have_content()) delete_sql += "),\n(";
		for (size_t n = 0; n < columns->size(); n++) {
			if (n > 0) {
				delete_sql += ',';
			}
			delete_sql += '\'';
			delete_sql += client->escape_value(row[(*columns)[n]].value);
			delete_sql += '\'';
		}
	}

	inline void apply() {
		delete_sql.apply(*client);
	}

	// these two should both be references, but g++ 4.6's STL needs vector element types to be Assignable,
	// which is impossible with references.
	DatabaseClient *client;
	const ColumnIndices *columns;
	BaseSQL delete_sql;
};

template <typename DatabaseClient>
struct TableRowApplier {
	TableRowApplier(DatabaseClient &client, const Table &table):
		client(client),
		table(table),
		insert_sql("INSERT INTO " + table.name + " VALUES\n(", ")"),
		rows(0) {

		// we will need to clear later rows that have our unique key values in order to insert
		for (const Key &key : table.keys) {
			if (key.unique) {
				unique_keys.push_back(UniqueKeyClearer<DatabaseClient>(client, table, key));
			}
		}
	}

	template <typename InputStream>
	void stream_from_input(Unpacker<InputStream> &input, const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {
		// we're being sent the range of rows > matched_up_to_key and <= last_not_matching_key; apply them to our end

		// for now, we clear and re-insert; matching up and using UPDATE statements is on the future list
		string delete_sql = client.delete_rows_sql(table, matched_up_to_key, last_not_matching_key);
		client.execute(delete_sql);

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

	inline void apply_forward_deletes() {
		for (UniqueKeyClearer<DatabaseClient> &unique_key : unique_keys) unique_key.apply();
	}

	inline void apply() {
		apply_forward_deletes();
		insert_sql.apply(client);
	}

	DatabaseClient &client;
	const Table &table;
	BaseSQL insert_sql;
	vector< UniqueKeyClearer<DatabaseClient> > unique_keys;
	size_t rows;
};
