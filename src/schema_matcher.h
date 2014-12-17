#ifndef SCHEMA_MATCHER_H
#define SCHEMA_MATCHER_H

#include <list>

#include "database_client_traits.h"
#include "schema.h"

typedef list<string> Statements;

// keys
template <typename DatabaseClient, bool = is_base_of<GlobalKeys, DatabaseClient>::value>
struct DropKeyStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const Key &key) {
		string result("ALTER TABLE ");
		result += table.name;
		result += " DROP INDEX ";
		result += client.quote_identifiers_with();
		result += key.name;
		result += client.quote_identifiers_with();
		statements.push_back(result);
	}
};

template <typename DatabaseClient>
struct DropKeyStatements <DatabaseClient, true> {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const Key &key) {
		string result("DROP INDEX ");
		result += client.quote_identifiers_with();
		result += key.name;
		result += client.quote_identifiers_with();
		statements.push_back(result);
	}
};

template <typename DatabaseClient>
struct CreateKeyStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const Key &key) {
		string result(key.unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ");
		result += client.quote_identifiers_with();
		result += key.name;
		result += client.quote_identifiers_with();
		result += " ON ";
		result += table.name;
		result += ' ';
		result += columns_list(client, table.columns, key.columns);
		statements.push_back(result);
	}
};

// tables
template <typename DatabaseClient>
struct DropTableStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		statements.emplace_back("DROP TABLE " + table.name);
	}
};

template <typename DatabaseClient>
struct CreateTableStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		string result("CREATE TABLE ");
		result += table.name;
		for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
			result += (column == table.columns.begin() ? " (\n  " : ",\n  ");
			result += client.column_definition(*column);
		}
		result += ",\n  PRIMARY KEY";
		result += columns_list(client, table.columns, table.primary_key_columns);
		result += ")";
		statements.push_back(result);

		for (const Key &key : table.keys) {
			CreateKeyStatements<DatabaseClient>::add_to(statements, client, table, key);
		}
	}
};

template <typename DatabaseClient>
struct SchemaMatcher {
	SchemaMatcher(DatabaseClient &client): client(client) {}

	void match_schemas(const Database &from_database, Database to_database) {
		// currently we only pay attention to tables, but in the future we might support other schema items
		match_tables(from_database.tables, to_database.tables);
	}

	void match_tables(Tables from_tables, Tables &to_tables) { // copies from_tables so we can sort it, mutates to_tables
		// sort the table lists so they have the same order - they typically are already,
		// but in the database server it depends on locale, so we enforce consistency here
		sort(from_tables.begin(), from_tables.end());
		sort(  to_tables.begin(),   to_tables.end());

		Tables::const_iterator from_table = from_tables.begin();
		Tables::iterator         to_table =   to_tables.begin();
		while (to_table != to_tables.end()) {
			if (from_table == from_tables.end() ||
				from_table->name > to_table->name) {
				// our end has an extra table, drop it
				DropTableStatements<DatabaseClient>::add_to(statements, client, *to_table);
				to_table = to_tables.erase(to_table);
				// keep the current from_table and re-evaluate on the next iteration

			} else if (to_table->name > from_table->name) {
				CreateTableStatements<DatabaseClient>::add_to(statements, client, *from_table);
				to_table = ++to_tables.insert(to_table, *from_table);
				++from_table;

			} else {
				match_table(*from_table, *to_table);
				++to_table;
				++from_table;
			}
		}
		while (from_table != from_tables.end()) {
			CreateTableStatements<DatabaseClient>::add_to(statements, client, *from_table);
			to_tables.push_back(*from_table);
			++from_table;
		}
	}

	void match_table(const Table &from_table, Table &to_table) {
		if (from_table == to_table) return;

		recreate_table(from_table, to_table);
	}

	void recreate_table(const Table &from_table, Table &to_table) {
		DropTableStatements<DatabaseClient>::add_to(statements, client, to_table);
		CreateTableStatements<DatabaseClient>::add_to(statements, client, from_table);
		to_table = from_table;
	}

	DatabaseClient &client;
	Statements statements;
};

#endif
