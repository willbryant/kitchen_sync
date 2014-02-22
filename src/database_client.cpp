#include "database_client.h"

void DatabaseClient::index_database_tables() {
	for (Tables::const_iterator table = database.tables.begin(); table != database.tables.end(); ++table) {
		tables_by_name[table->name] = table;
	}
}
