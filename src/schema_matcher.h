#ifndef SCHEMA_MATCHER_H
#define SCHEMA_MATCHER_H

#include <list>

template <typename DatabaseClient>
struct SchemaMatcher {
	SchemaMatcher(DatabaseClient &client): client(client) {}

	void match_schemas(const Database &from_database, Database to_database) {
		// currently we only pay attention to tables, but in the future we might support other schema items
		// match_tables(from_database.tables, to_database.tables);
	}

	DatabaseClient &client;
	list<string> statements;
};

#endif
