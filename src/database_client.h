#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include <map>
#include "schema.h"

class DatabaseClient {
public:
	inline const Database &database_schema() { return database; }

	string retrieve_rows_sql(const string &table_name, const RowValues &first_key, const RowValues &last_key);
	string retrieve_rows_sql(const string &table_name, const RowValues &first_key, size_t row_count);

protected:
	Database database;
	map<string, ColumnNames> table_key_columns;
};

#endif
