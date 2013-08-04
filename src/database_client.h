#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include <map>
#include "schema.h"

class DatabaseClient {
public:
	inline const Database &database_schema() { return database; }
	inline const Table &table_by_name(const string &table_name) { return *tables_by_name.at(table_name); } // throws out_of_range if not present in the map

	string retrieve_rows_sql(const Table &table, const RowValues &first_key, const RowValues &last_key);
	string retrieve_rows_sql(const Table &table, const RowValues &first_key, size_t row_count);

protected:
	void index_database_tables();

protected:
	Database database;
	map<string, Tables::const_iterator> tables_by_name;
};

#endif
