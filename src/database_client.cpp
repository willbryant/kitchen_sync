#include "database_client.h"

#include <sstream>
#include <stdexcept>
#include "sql_functions.h"
#include "to_string.h"

string DatabaseClient::retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, size_t row_count) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));

	string result("SELECT ");
	for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
		if (column != table.columns.begin()) result += ", ";
		result += column->name;
	}
	result += " FROM ";
	result += table.name;
	if (!prev_key.empty()) {
		result += " WHERE " + key_columns + " > " + non_binary_string_values_list(prev_key);
	}
	result += " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	result += " LIMIT " + to_string(row_count);
	return result;
}

string DatabaseClient::retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string key_columns(columns_list(table.columns, table.primary_key_columns));

	string result("SELECT ");
	for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
		if (column != table.columns.begin()) result += ", ";
		result += column->name;
	}
	result += " FROM ";
	result += table.name;
	result += where_sql(key_columns, prev_key, last_key);
	result += + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	return result;
}

void DatabaseClient::index_database_tables() {
	for (Tables::const_iterator table = database.tables.begin(); table != database.tables.end(); ++table) {
		tables_by_name[table->name] = table;
	}
}
