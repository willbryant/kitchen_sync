#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include <map>
#include "schema.h"

class DatabaseClient {
public:
	inline const Database &database_schema() { return database; }
	inline const Table &table_by_name(const string &table_name) { return *tables_by_name.at(table_name); } // throws out_of_range if not present in the map

	string retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, size_t row_count);
	string retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key);
	string   delete_rows_sql(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key);

protected:
	void index_database_tables();
	string where_sql(const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key);

protected:
	Database database;
	map<string, Tables::const_iterator> tables_by_name;
};

#endif
