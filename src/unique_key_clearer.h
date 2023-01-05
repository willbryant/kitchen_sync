#ifndef UNIQUE_KEY_CLEARER_H
#define UNIQUE_KEY_CLEARER_H

#include "base_sql.h"
#include "encode_packed.h"
#include "message_pack/packed_row.h"

template <typename DatabaseClient>
struct UniqueKeyClearer {
	UniqueKeyClearer(DatabaseClient &client, const Table &table, const ColumnIndices &key_columns):
		client(&client),
		table(&table),
		key_columns(&key_columns),
		delete_sql("DELETE FROM " + client.quote_table_name(table) + " WHERE (", ")") {
	}

	bool key_enforceable(const PackedRow &row) {
		for (size_t column : *key_columns) {
			if (row.is_nil(column)) return false;
		}
		return true;
	}

	void row(const PackedRow &row) {
		// rows with any NULL values won't enforce a uniqueness constraint, so we don't need to clear them
		if (!key_enforceable(row)) return;

		if (delete_sql.have_content()) delete_sql += ")\nOR (";
		for (size_t n = 0; n < key_columns->size(); n++) {
			// frustratingly http://bugs.mysql.com/bug.php?id=31188 was not fixed until 5.7.3 so we can't simply make a big WHERE (key columns) IN (tuples) here, and have to use AND/OR repetition instead
			if (n > 0) {
				delete_sql += " AND ";
			}
			size_t column = (*key_columns)[n];
			delete_sql += table->columns[column].name;
			delete_sql += '=';
			PackedValueReadStream stream(row, column);
			sql_encode_and_append_packed_value_to(delete_sql.curr, *client, table->columns[column], stream);
		}
	}

	inline void apply() {
		delete_sql.apply(*client);
	}

	// these three should both be references, but g++ 4.6's STL needs vector element types to be Assignable,
	// which is impossible with references.
	DatabaseClient *client;
	const Table *table;
	const ColumnIndices *key_columns;
	BaseSQL delete_sql;
};

#endif
