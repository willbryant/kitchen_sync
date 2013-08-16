#include "sql_functions.h"

template <typename DatabaseClient>
void sync_table_rows(
	DatabaseClient &client, Stream &input, const Table &table,
	const ColumnValues &matched_up_to_key, ColumnValues &last_not_matching_key) {

	// for now, we clear and re-insert; matching up and using UPDATE statements is next on the list
	client.execute(client.delete_rows_sql(table, matched_up_to_key, last_not_matching_key));

	// retrieve the range of rows > matched_up_to_key and <= last_not_matching_key, and apply them to our end
	send_command(cout, "rows", table.name, matched_up_to_key, last_not_matching_key);

	// built a string of rows to insert as we go
	string insert_sql("INSERT INTO " + table.name + " VALUES ");
	bool any_values = false;

	while (true) {
		// the rows command is unusual.  to avoid needing to know the number of results in advance,
		// instead of a single response object, there's one response object per row, terminated by
		// an empty row (which is not valid data, so is unambiguous).
		vector<msgpack::object> values;
		input >> values;
		if (values.empty()) break;

		insert_sql += any_values ? ", (" : "(";
		for (vector<msgpack::object>::const_iterator column_value = values.begin(); column_value != values.end(); ++column_value) {
			if (column_value != values.begin()) {
				insert_sql += ", ";
			}
			if (column_value->is_nil()) {
				insert_sql += "NULL";
			} else {
				insert_sql += "'";
				insert_sql += escape_non_binary_string(column_value->as<string>());
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
