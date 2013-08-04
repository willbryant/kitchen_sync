#include "database_client.h"

#include <sstream>
#include <stdexcept>
#include "sql_functions.h"

string DatabaseClient::retrieve_rows_sql(const string &table_name, const RowValues &first_key, const RowValues &last_key) {
	const Table &table(*tables_by_name.at(table_name)); // throws out_of_range if not present in the map

	string key_columns(columns_list(table.primary_key_columns));

	// mysql doesn't support BETWEEN for tuples, so we use >= and <= instead
	return "SELECT * FROM " + table_name + " WHERE " + key_columns + " >= " + non_binary_string_values_list(first_key) + " AND " + key_columns + " <= " + non_binary_string_values_list(last_key) + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
}

string DatabaseClient::retrieve_rows_sql(const string &table_name, const RowValues &first_key, size_t row_count) {
	const Table &table(*tables_by_name.at(table_name)); // throws out_of_range if not present in the map

	string key_columns(columns_list(table.primary_key_columns));
	std::ostringstream limit;
	limit << row_count;

	return "SELECT * FROM " + table_name + " WHERE " + key_columns + " >= " + non_binary_string_values_list(first_key) + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2) + " LIMIT " + limit.str();
}

void DatabaseClient::index_database_tables() {
	for (Tables::const_iterator table = database.tables.begin(); table != database.tables.end(); ++table) {
		tables_by_name[table->name] = table;
	}
}
