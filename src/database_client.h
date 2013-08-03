#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include "sql_functions.h"

class DatabaseClient {
public:
	inline const Database &database_schema() { return database; }

	string retrieve_rows_sql(const string &table_name, const RowValues &first_key, const RowValues &last_key) {
		if (table_key_columns[table_name].empty()) {
			throw runtime_error("Don't know the key columns for " + table_name);
		}

		string key_columns(columns_list(table_key_columns[table_name]));

		// mysql doesn't support BETWEEN for tuples, so we use >= and <= instead
		return "SELECT * FROM " + table_name + " WHERE " + key_columns + " >= " + non_binary_string_values_list(first_key) + " AND " + key_columns + " <= " + non_binary_string_values_list(last_key) + " ORDER BY " + key_columns.substr(1, key_columns.size() - 2);
	}

protected:
	Database database;
	map<string, ColumnNames> table_key_columns;
};

#endif
