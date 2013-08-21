#include "sql_functions.h"

template <typename DatabaseClient>
void sync_table_rows(
	DatabaseClient &client, Unpacker &input, Packer<ostream> &output, const Table &table,
	const ColumnValues &matched_up_to_key, ColumnValues &last_not_matching_key) {

	// for now, we clear and re-insert; matching up and using UPDATE statements is next on the list
	client.execute(client.delete_rows_sql(table, matched_up_to_key, last_not_matching_key));

	// retrieve the range of rows > matched_up_to_key and <= last_not_matching_key, and apply them to our end
	send_command(output, "rows", table.name, matched_up_to_key, last_not_matching_key);

	// built a string of rows to insert as we go
	string insert_sql("INSERT INTO " + table.name + " VALUES\n");
	bool any_values = false;

	while (true) {
		// the rows command is unusual.  to avoid needing to know the number of results in advance,
		// instead of a single response object, there's one response object per row, terminated by
		// an empty row (which is not valid data, so is unambiguous).
		size_t columns = input.next_array_length();
		if (columns == 0) break;

		insert_sql += any_values ? ",\n(" : "(";
		for (size_t n = 0; n < columns; n++) {
			if (n > 0) {
				insert_sql += ",";
			}
			if (input.next_is_nil()) {
				input.next_nil();
				insert_sql += "NULL";
			} else {
				insert_sql += "'";
				insert_sql += client.escape_value(input.next<string>());
				insert_sql += "'";
			}
		}
		insert_sql += ")";
		any_values = true;
	}

	if (any_values) {
		client.execute(insert_sql);
	}
}
