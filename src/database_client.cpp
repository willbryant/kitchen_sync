#include "database_client.h"

#include <sstream>
#include <stdexcept>
#include "sql_functions.h"

string DatabaseClient::retrieve_rows_sql(const Table &table, const RowValues &first_key, const RowValues &last_key) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));

	// mysql doesn't support BETWEEN for tuples, so we use >= and <= instead
	return "SELECT * FROM " + table.name + " WHERE " + key_columns + " >= " + non_binary_string_values_list(first_key) + " AND " + key_columns + " <= " + non_binary_string_values_list(last_key) + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
}

string DatabaseClient::retrieve_rows_sql(const Table &table, const RowValues &first_key, size_t row_count) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));
	std::ostringstream limit;
	limit << row_count;

	return "SELECT * FROM " + table.name + " WHERE " + key_columns + " >= " + non_binary_string_values_list(first_key) + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2) + " LIMIT " + limit.str();
}

void DatabaseClient::index_database_tables() {
	for (Tables::const_iterator table = database.tables.begin(); table != database.tables.end(); ++table) {
		tables_by_name[table->name] = table;
	}
}
