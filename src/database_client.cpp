#include "database_client.h"

#include <sstream>
#include <stdexcept>
#include "sql_functions.h"
#include "to_string.h"

string DatabaseClient::retrieve_rows_sql(const Table &table, const RowValues &prev_key, const RowValues &last_key) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));

	string result("SELECT * FROM " + table.name + " WHERE ");
	if (!prev_key.empty()) {
		result += key_columns + " > " + non_binary_string_values_list(prev_key) + " AND ";
	}
	result += key_columns + " <= " + non_binary_string_values_list(last_key);
	result += + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	return result;
}

string DatabaseClient::retrieve_rows_sql(const Table &table, const RowValues &prev_key, size_t row_count) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));

	string result("SELECT * FROM " + table.name);
	if (!prev_key.empty()) {
		result += " WHERE " + key_columns + " > " + non_binary_string_values_list(prev_key);
	}
	result += " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	result += " LIMIT " + to_string(row_count);
	return result;
}

void DatabaseClient::index_database_tables() {
	for (Tables::const_iterator table = database.tables.begin(); table != database.tables.end(); ++table) {
		tables_by_name[table->name] = table;
	}
}
