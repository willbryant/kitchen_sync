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

		return "SELECT * FROM " + table_name + " WHERE " + columns_list(table_key_columns[table_name]) + " BETWEEN " + non_binary_string_values_list(first_key) + " AND " + non_binary_string_values_list(last_key);
	}

protected:
	Database database;
	map<string, ColumnNames> table_key_columns;
};

#endif
