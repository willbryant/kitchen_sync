#ifndef SCHEMA_MATCHER_H
#define SCHEMA_MATCHER_H

#include <algorithm>
#include <functional>
#include <list>

#include "database_client_traits.h"
#include "schema.h"

typedef list<string> Statements;
typedef function<void(const string&)> StatementFunction;

StatementFunction append_to(Statements &statements) {
	return [&statements](const string &statement) { statements.push_back(statement); };
}

StatementFunction prepend_to(Statements &statements) {
	return [&statements](const string &statement) { statements.push_front(statement); };
}

// keys
template <typename DatabaseClient, bool = is_base_of<GlobalKeys, DatabaseClient>::value>
struct DropKeyStatements {
	static void generate(DatabaseClient &client, const Table &table, const Key &key, StatementFunction f) {
		string result("ALTER TABLE ");
		result += client.quote_table_name(table);
		result += " DROP INDEX ";
		result += client.quote_identifier(key.name);
		f(result);
	}
};

template <typename DatabaseClient>
struct DropKeyStatements <DatabaseClient, true> {
	static void generate(DatabaseClient &client, const Table &table, const Key &key, StatementFunction f) {
		string result("DROP INDEX ");
		result += client.quote_identifier(key.name);
		f(result);
	}
};

template <typename DatabaseClient>
struct CreateKeyStatements {
	static void generate(DatabaseClient &client, const Table &table, const Key &key, StatementFunction f) {
		f(client.key_definition(table, key));
	}
};

// sequences - for cross-compatibility, these are only supported for sequence columns
// all this can go away once we drop support for postgresql 9.6 and earlier, as 10+ use the new identity columns
// which can be implemented the same way as mysql's auto_increment (just a different DEFAULT string)

template <typename DatabaseClient, bool = is_base_of<SequenceColumns, DatabaseClient>::value>
struct CreateTableSequencesStatements {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		/* nothing required */
	}
};

template <typename DatabaseClient>
struct CreateTableSequencesStatements <DatabaseClient, true> {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		for (const Column &column : table.columns) {
			if (column.default_type == DefaultType::generated_by_sequence) {
				string result("DROP SEQUENCE IF EXISTS ");
				result += client.quote_schema_name(table.schema_name) + '.' + client.quote_identifier(column.default_value);
				f(result);

				result = "CREATE SEQUENCE ";
				result += client.quote_schema_name(table.schema_name) + '.' + client.quote_identifier(column.default_value);
				f(result);
			}
		}
	}
};

template <typename DatabaseClient, bool = is_base_of<SequenceColumns, DatabaseClient>::value>
struct OwnTableSequencesStatements {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		/* nothing required */
	}
};

template <typename DatabaseClient>
struct OwnTableSequencesStatements <DatabaseClient, true> {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		for (const Column &column : table.columns) {
			if (column.default_type == DefaultType::generated_by_sequence) {
				string result("ALTER SEQUENCE ");
				result += client.quote_schema_name(table.schema_name) + '.' + client.quote_identifier(column.default_value);
				result += " OWNED BY ";
				result += client.quote_table_name(table);
				result += '.';
				result += client.quote_identifier(column.name);
				f(result);
			}
		}
	}
};

// tables
template <typename DatabaseClient>
struct DropTableStatements {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		f("DROP TABLE " + client.quote_table_name(table));
	}
};

template <typename DatabaseClient>
struct CreateTableStatements {
	static void generate(DatabaseClient &client, const Table &table, StatementFunction f) {
		CreateTableSequencesStatements<DatabaseClient>::generate(client, table, f);

		string result("CREATE TABLE ");
		result += client.quote_table_name(table);
		for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
			result += (column == table.columns.begin() ? " (\n  " : ",\n  ");
			result += client.column_definition(table, *column);
		}
		if (table.primary_key_type == PrimaryKeyType::explicit_primary_key) {
			result += ",\n  PRIMARY KEY";
			result += columns_tuple(client, table.columns, table.primary_key_columns);
		}
		result += ")";
		f(result);

		for (const Key &key : table.keys) {
			CreateKeyStatements<DatabaseClient>::generate(client, table, key, f);
		}

		OwnTableSequencesStatements<DatabaseClient>::generate(client, table, f);
	}
};

template <typename DatabaseClient>
struct AlterTableStatements {
	static void generate(DatabaseClient &client, const Table &table, const string &alter_table_clauses, StatementFunction f) {
		string result("ALTER TABLE ");
		result += client.quote_table_name(table);
		result += alter_table_clauses;
		f(result);
	}
};

template <typename DatabaseClient>
struct AlterColumnDefaultClauses {
	static void add_to(string &alter_table_clauses, DatabaseClient &client, const Table &table, const Column &from_column, Column &to_column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " ALTER ";
		alter_table_clauses += client.quote_identifier(to_column.name);
		if (from_column.default_type != DefaultType::no_default) {
			alter_table_clauses += " SET";
			alter_table_clauses += client.column_default(table, from_column);
		} else {
			alter_table_clauses += " DROP DEFAULT";
		}
		to_column.default_type  = from_column.default_type;
		to_column.default_value = from_column.default_value;
	}
};

template <typename DatabaseClient>
struct ModifyColumnDefinitionClauses {
	static void add_to(string &alter_table_clauses, DatabaseClient &client, const Table &table, const Column &from_column, Column &to_column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " MODIFY ";
		alter_table_clauses += client.column_definition(table, from_column);
		to_column.nullable = from_column.nullable;
		to_column.default_type = from_column.default_type;
		to_column.default_value = from_column.default_value;
		// MODIFY column_definition will actually change the data type too, but that may or may not succeed (if the current values aren't
		// valid for the new type), so currently we don't say we've fixed the type so that our matcher algorithm will still drop and recreate
	}
};

template <typename DatabaseClient, bool = is_base_of<SetNullability, DatabaseClient>::value>
struct AlterColumnNullabilityClauses {
	static void add_to(string &alter_table_clauses, DatabaseClient &client, const Table &table, const Column &from_column, Column &to_column) {
		ModifyColumnDefinitionClauses<DatabaseClient>::add_to(alter_table_clauses, client, table, from_column, to_column);
	}
};

template <typename DatabaseClient>
struct AlterColumnNullabilityClauses<DatabaseClient, true> {
	static void add_to(string &alter_table_clauses, DatabaseClient &client, const Table &table, const Column &from_column, Column &to_column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " ALTER ";
		alter_table_clauses += client.quote_identifier(to_column.name);
		if (from_column.nullable) {
			alter_table_clauses += " DROP NOT NULL";
		} else {
			alter_table_clauses += " SET NOT NULL";
		}
		to_column.nullable = from_column.nullable;
	}
};

template <typename DatabaseClient>
struct UpdateTableStatements {
	static void generate(DatabaseClient &client, const Table &table, const string &update_table_clauses, StatementFunction f) {
		string result("UPDATE ");
		result += client.quote_table_name(table);
		result += " SET ";
		result += update_table_clauses;
		f(result);
	}
};

template <typename DatabaseClient>
struct OverwriteColumnNullValueClauses {
	static void add_to(string &update_table_clauses, DatabaseClient &client, const Table &table, const Column &column) {
		if (!update_table_clauses.empty()) {
			update_table_clauses += ",";
		}
		update_table_clauses += client.quote_identifier(column.name);
		update_table_clauses += " = COALESCE(";
		update_table_clauses += client.quote_identifier(column.name);
		update_table_clauses += ", ";
		client.append_quoted_column_value_to(update_table_clauses, column, usable_column_value(column));
		update_table_clauses += ")";
	}

	static string usable_column_value(const Column &column) {
		switch (column.column_type) {
			case ColumnType::binary:
			case ColumnType::text:
			case ColumnType::text_varchar:
			case ColumnType::text_fixed:
				return "";

			case ColumnType::date:
				return "2000-01-01";

			case ColumnType::time:
				return "00:00:00";

			case ColumnType::datetime:
				return "2000-01-01 00:00:00";

			case ColumnType::uuid:
				return "00000000-0000-0000-0000-000000000000";

			case ColumnType::enumeration:
				if (column.enumeration_values.empty()) { // should never be empty in reality, but don't segfault
					return "";
				} else {
					return column.enumeration_values[0];
				}

			case ColumnType::boolean:
				return "false";

			default:
				return "0";
		}
	}
};

template <typename DatabaseClient>
struct DropColumnClauses {
	static void add_to(string &alter_table_clauses, Statements &alter_statements, DatabaseClient &client, Table &table, size_t column_index) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " DROP ";
		alter_table_clauses += client.quote_identifier(table.columns[column_index].name);

		if (find(table.primary_key_columns.begin(), table.primary_key_columns.end(), column_index) != table.primary_key_columns.end()) {
			table.primary_key_columns.clear(); // so the table compares not-equal and gets recreated
		} else {
			update_indices(table.primary_key_columns, column_index);

			Keys::iterator key = table.keys.begin();
			while (key != table.keys.end()) {
				if (find(key->columns.begin(), key->columns.end(), column_index) != key->columns.end()) {
					// proactively drop the key at the start to work around https://bugs.mysql.com/bug.php?id=57497 and
					// to avoid the the related new behavior from https://jira.mariadb.org/browse/MDEV-13613.
					DropKeyStatements<DatabaseClient>::generate(client, table, *key, prepend_to(alter_statements));
					key = table.keys.erase(key);
				} else {
					update_indices(key->columns, column_index);
					++key;
				}
			}
		}
	}

	static void update_indices(ColumnIndices &column_indices, size_t column_index) {
		// decrement the column indices for subsequent columns
		for (size_t &key_column_index : column_indices) {
			if (key_column_index > column_index) {
				--key_column_index;
			}
		}
	}
};

template <typename DatabaseClient, bool = is_base_of<SupportsAddNonNullableColumns, DatabaseClient>::value>
struct AddColumnClauses {
	static void add_to(string &alter_table_clauses, string &second_round_alter_table_clauses, DatabaseClient &client, Table &table, const Column &column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " ADD ";
		if (column.nullable || column.default_type == DefaultType::default_value || column.default_type == DefaultType::default_expression) {
			alter_table_clauses += client.column_definition(table, column);
		} else {
			// first add the column, with a default value to get past the non-nullability
			Column temp(column);
			temp.default_type = DefaultType::default_value;
			temp.default_value = OverwriteColumnNullValueClauses<DatabaseClient>::usable_column_value(column);
			alter_table_clauses += client.column_definition(table, temp);

			// then change the default to what it should be; unfortunately postgresql won't let us combine this
			// into the same ALTER statement, so we have to have this silly second_round_alter_table_clauses
			AlterColumnDefaultClauses<DatabaseClient>::add_to(second_round_alter_table_clauses, client, table, column, temp);
		}
		table.columns.push_back(column); // our schema matching algorithm only supports adding columns at the end because not all databases support AFTER clauses
	}
};

template <typename DatabaseClient>
struct AddColumnClauses<DatabaseClient, true> {
	static void add_to(string &alter_table_clauses, string &second_round_alter_table_clauses, DatabaseClient &client, Table &table, const Column &column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " ADD ";
		alter_table_clauses += client.column_definition(table, column);
		table.columns.push_back(column);
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

		Tables::iterator from_table = from_tables.begin();
		Tables::iterator   to_table =   to_tables.begin();
		while (to_table != to_tables.end()) {
			if (from_table == from_tables.end() ||
				from_table->name > to_table->name) {
				// our end has an extra table, drop it
				DropTableStatements<DatabaseClient>::generate(client, *to_table, append_to(statements));
				to_table = to_tables.erase(to_table);
				// keep the current from_table and re-evaluate on the next iteration

			} else if (to_table->name > from_table->name) {
				CreateTableStatements<DatabaseClient>::generate(client, *from_table, append_to(statements));
				to_table = ++to_tables.insert(to_table, *from_table);
				++from_table;

			} else {
				match_table(*from_table, *to_table);
				++to_table;
				++from_table;
			}
		}
		while (from_table != from_tables.end()) {
			CreateTableStatements<DatabaseClient>::generate(client, *from_table, append_to(statements));
			to_tables.push_back(*from_table);
			++from_table;
		}
	}

	void match_table(Table &from_table, Table &to_table) {
		// sort the key lists so they have the same order; we consider keys to be unordered
		sort(from_table.keys.begin(), from_table.keys.end());
		sort(  to_table.keys.begin(),   to_table.keys.end());

		// if the tables match, we don't have to do anything
		if (from_table == to_table) return;

		// so the table differs.  see if it's something we can fix without recreating the table.
		Statements alter_statements;

		match_columns(alter_statements, from_table, to_table);
		match_keys(alter_statements, from_table, to_table);

		if (from_table == to_table) {
			// yup, the statements we can construct would fix it - append those statements to the list
			statements.splice(statements.end(), alter_statements);
		} else {
			// nope, throw away those ALTER statements, and recreate the table
			comment_on_table_differences(statements, from_table, to_table);
			DropTableStatements<DatabaseClient>::generate(client, to_table, append_to(statements));
			CreateTableStatements<DatabaseClient>::generate(client, from_table, append_to(statements));
			to_table = from_table;
		}
	}

	void comment_on_table_differences(Statements &statements, const Table &from_table, const Table &to_table) {
		string comment("-- ");
		comment += to_table.name;
		if (to_table.columns == from_table.columns) {
			comment += ": columns match";
			comment += (to_table.primary_key_columns == from_table.primary_key_columns ? ", primary key columns match" : ", primary key columns don't match");
			comment += (to_table.keys == from_table.keys ? ", keys match" : ", keys don't match");
		} else {
			comment += ": columns don't match";
		}
		statements.push_back(comment);
	}

	void match_keys(Statements &alter_statements, const Table &from_table, Table &to_table) {
		// if the columns list doesn't match up yet, then comparing the keys may attempt to access invalid indices in the columns list
		if (from_table.columns != to_table.columns) return;

		Keys::const_iterator from_key = from_table.keys.begin();
		Keys::iterator         to_key =   to_table.keys.begin();

		Statements key_statements;

		while (to_key != to_table.keys.end()) {
			if (from_key == from_table.keys.end() ||
				from_key->name > to_key->name) {
				// our end has an extra key, drop it
				DropKeyStatements<DatabaseClient>::generate(client, to_table, *to_key, append_to(key_statements));
				to_key = to_table.keys.erase(to_key);
				// keep the current from_key and re-evaluate on the next iteration

			} else if (to_key->name > from_key->name) {
				// their end has an extra key, add it; prepend before any DROP KEYS statements so in the edge case where an FKC
				// was using the old index but would be equally happy with a new one we're creating, it doesn't block the DROP
				CreateKeyStatements<DatabaseClient>::generate(client, to_table, *from_key, prepend_to(key_statements));
				to_key = ++to_table.keys.insert(to_key, *from_key);
				++from_key;
				// keep the current to_key and re-evaluate on the next iteration

			} else if (from_key != to_key) {
				// recreate the index.  not all databases can combine these two statements, so we implement the general case only for now.
				DropKeyStatements<DatabaseClient>::generate(client, from_table, *to_key, append_to(key_statements));
				CreateKeyStatements<DatabaseClient>::generate(client, from_table, *from_key, append_to(key_statements));
				*to_key++ = *from_key++;

			} else {
				++to_key;
				++from_key;
			}
		}

		while (from_key != from_table.keys.end()) {
			// as above, do this before DROP KEYS when possible to avoid FKC errors
			CreateKeyStatements<DatabaseClient>::generate(client, to_table, *from_key, prepend_to(key_statements));
			to_key = ++to_table.keys.insert(to_key, *from_key);
			++from_key;
		}

		alter_statements.splice(alter_statements.end(), key_statements);
	}

	void match_columns(Statements &alter_statements, const Table &from_table, Table &to_table) {
		string update_table_clauses;
		string alter_table_clauses, second_round_alter_table_clauses;

		size_t column_index = 0; // we use indices here because we need to update the key column lists, which use indices rather than names
		while (column_index < to_table.columns.size()) {
			Columns::const_iterator from_column(from_table.columns.begin() + column_index);
			Columns::iterator         to_column(  to_table.columns.begin() + column_index);

			if (column_index >= from_table.columns.size() || to_column->name != from_column->name) {
				DropColumnClauses<DatabaseClient>::add_to(alter_table_clauses, alter_statements, client, to_table, column_index);
				to_table.columns.erase(to_column);
			} else {
				if (from_column->nullable && !to_column->nullable) {
					AlterColumnNullabilityClauses<DatabaseClient>::add_to(alter_table_clauses, client, to_table, *from_column, *to_column);
				} else if (!from_column->nullable && to_column->nullable && !column_used_in_unique_key(from_table, column_index)) {
					OverwriteColumnNullValueClauses<DatabaseClient>::add_to(update_table_clauses, client, to_table, *to_column);
					AlterColumnNullabilityClauses<DatabaseClient>::add_to(alter_table_clauses, client, to_table, *from_column, *to_column);
				}
				if ((from_column->default_type != to_column->default_type || from_column->default_value != to_column->default_value) &&
					(from_column->default_type == DefaultType::no_default || from_column->default_type == DefaultType::default_value || from_column->default_type == DefaultType::default_expression)) {
					AlterColumnDefaultClauses<DatabaseClient>::add_to(alter_table_clauses, client, to_table, *from_column, *to_column);
				}
				++column_index;
			}
		}
		while (column_index < from_table.columns.size()) {
			Columns::const_iterator from_column(from_table.columns.begin() + column_index);
			if (!from_column->nullable && column_used_in_unique_key(from_table, column_index)) {
				// since the column is non-nullable we will fill in a default value (eg. 0 or "") for all rows, which means that adding
				// the unique key would fail.  so we're going to have to recreate the table; no point trying to add the column.
				return;
			}
			AddColumnClauses<DatabaseClient>::add_to(alter_table_clauses, second_round_alter_table_clauses, client, to_table, *from_column);
			++column_index;
		}

		if (!update_table_clauses.empty()) {
			UpdateTableStatements<DatabaseClient>::generate(client, to_table, update_table_clauses, append_to(alter_statements));
		}
		if (!alter_table_clauses.empty()) {
			AlterTableStatements<DatabaseClient>::generate(client, to_table, alter_table_clauses, append_to(alter_statements));
		}
		if (!second_round_alter_table_clauses.empty()) {
			AlterTableStatements<DatabaseClient>::generate(client, to_table, second_round_alter_table_clauses, append_to(alter_statements));
		}
	}

	bool column_used_in_unique_key(const Table &table, size_t column_index) {
		for (const Key &key : table.keys) {
			for (size_t index : key.columns) {
				if (index == column_index) {
					return true;
				}
			}
		}
		return false;
	}

	DatabaseClient &client;
	Statements statements;
};

#endif
