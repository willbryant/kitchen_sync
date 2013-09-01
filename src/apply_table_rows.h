#include "sql_functions.h"

template <typename DatabaseClient>
size_t apply_table_rows(
	DatabaseClient &client, Unpacker &input, const Table &table,
	const ColumnValues &matched_up_to_key, const ColumnValues &last_not_matching_key) {

	const size_t MAX_SENSIBLE_INSERT_COMMAND_SIZE = 8*1024*1024;

	// we're being sent the range of rows > matched_up_to_key and <= last_not_matching_key; apply them to our end

	// for now, we clear and re-insert; matching up and using UPDATE statements is on the future list
	client.execute(client.delete_rows_sql(table, matched_up_to_key, last_not_matching_key));

	// built a string of rows to insert as we go
	string base_insert_sql("INSERT INTO " + table.name + " VALUES\n");
	string insert_sql(base_insert_sql);
	insert_sql.reserve(2*MAX_SENSIBLE_INSERT_COMMAND_SIZE);
	size_t row_count = 0;

	while (true) {
		// the rows command is unusual.  to avoid needing to know the number of results in advance,
		// instead of a single response object, there's one response object per row, terminated by
		// an empty row (which is not valid data, so is unambiguous).
		size_t columns = input.next_array_length();
		if (columns == 0) break;

		insert_sql += insert_sql.size() > base_insert_sql.size() ? ",\n(" : "(";
		for (size_t n = 0; n < columns; n++) {
			if (n > 0) {
				insert_sql += ',';
			}
			if (input.next_is_nil()) {
				input.next_nil();
				insert_sql += "NULL";
			} else {
				insert_sql += '\'';
				insert_sql += client.escape_value(input.next<string>());
				insert_sql += '\'';
			}
		}
		insert_sql += ')';

		if (insert_sql.size() > MAX_SENSIBLE_INSERT_COMMAND_SIZE) {
			client.execute(insert_sql);
			insert_sql = base_insert_sql;
			insert_sql.reserve(2*MAX_SENSIBLE_INSERT_COMMAND_SIZE);
		}

		row_count++;
	}

	if (insert_sql.size() > base_insert_sql.size()) {
		client.execute(insert_sql);
	}

	return row_count;
}
