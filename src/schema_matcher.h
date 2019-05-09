#ifndef SCHEMA_MATCHER_H
#define SCHEMA_MATCHER_H

#include <algorithm>
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
		result += client.quote_identifier(key.name);
		statements.push_front(result);
	}
};

template <typename DatabaseClient>
struct DropKeyStatements <DatabaseClient, true> {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const Key &key) {
		string result("DROP INDEX ");
		result += client.quote_identifier(key.name);
		statements.push_front(result);
	}
};

template <typename DatabaseClient>
struct CreateKeyStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const Key &key) {
		string result(key.unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ");
		result += client.quote_identifier(key.name);
		result += " ON ";
		result += table.name;
		result += ' ';
		result += columns_tuple(client, table.columns, key.columns);
		statements.push_back(result);
	}
};

// sequences - for cross-compatibility, these are only supported for sequence columns

template <typename DatabaseClient, bool = is_base_of<SequenceColumns, DatabaseClient>::value>
struct CreateTableSequencesStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		/* nothing required */
	}
};

template <typename DatabaseClient>
struct CreateTableSequencesStatements <DatabaseClient, true> {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		for (const Column &column : table.columns) {
			if (column.default_type == DefaultType::sequence) {
				string result("DROP SEQUENCE IF EXISTS ");
				result += client.quote_identifier(client.column_sequence_name(table, column));
				statements.push_back(result);

				result = "CREATE SEQUENCE ";
				result += client.quote_identifier(client.column_sequence_name(table, column));
				statements.push_back(result);
			}
		}
	}
};

template <typename DatabaseClient, bool = is_base_of<SequenceColumns, DatabaseClient>::value>
struct OwnTableSequencesStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		/* nothing required */
	}
};

template <typename DatabaseClient>
struct OwnTableSequencesStatements <DatabaseClient, true> {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table) {
		for (const Column &column : table.columns) {
			if (column.default_type == DefaultType::sequence) {
				string result("ALTER SEQUENCE ");
				result += client.quote_identifier(client.column_sequence_name(table, column));
				result += " OWNED BY ";
				result += client.quote_identifier(table.name);
				result += '.';
				result += client.quote_identifier(column.name);
				statements.push_back(result);
			}
		}
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
		CreateTableSequencesStatements<DatabaseClient>::add_to(statements, client, table);

		string result("CREATE TABLE ");
		result += table.name;
		for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
			result += (column == table.columns.begin() ? " (\n  " : ",\n  ");
			result += client.column_definition(table, *column);
		}
		if (table.primary_key_type == explicit_primary_key) {
			result += ",\n  PRIMARY KEY";
			result += columns_tuple(client, table.columns, table.primary_key_columns);
		}
		result += ")";
		statements.push_back(result);

		for (const Key &key : table.keys) {
			CreateKeyStatements<DatabaseClient>::add_to(statements, client, table, key);
		}

		OwnTableSequencesStatements<DatabaseClient>::add_to(statements, client, table);
	}
};

template <typename DatabaseClient>
struct AlterTableStatements {
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const string &alter_table_clauses) {
		string result("ALTER TABLE ");
		result += table.name;
		result += alter_table_clauses;
		statements.push_back(result);
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
		if (from_column.default_type) {
			alter_table_clauses += " SET ";
			alter_table_clauses += client.column_default(table, from_column);
		} else {
			alter_table_clauses += " DROP DEFAULT";
		}
		to_column.default_type  = from_column.default_type;
		to_column.default_value = from_column.default_value;
	}
};

template <typename DatabaseClient, bool = is_base_of<SetNullability, DatabaseClient>::value>
struct AlterColumnNullabilityClauses {
	static void add_to(string &alter_table_clauses, DatabaseClient &client, const Table &table, const Column &from_column, Column &to_column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " MODIFY ";
		alter_table_clauses += client.column_definition(table, from_column);
		to_column.nullable = from_column.nullable;
		to_column.default_type = from_column.default_type;
		// MODIFY column_definition will actually change the data type too, but that may or may not succeed,
		// so currently we don't say we've fixed the type so that our matcher algorithm will still drop and recreate
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
	static void add_to(Statements &statements, DatabaseClient &client, const Table &table, const string &update_table_clauses) {
		string result("UPDATE ");
		result += table.name;
		result += " SET ";
		result += update_table_clauses;
		statements.push_back(result);
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
		update_table_clauses += ", '";
		update_table_clauses += client.escape_column_value(column, usable_column_value(column));
		update_table_clauses += "')";
	}

	static string usable_column_value(const Column &column) {
		if (column.column_type == ColumnTypes::BLOB || column.column_type == ColumnTypes::TEXT ||
			column.column_type == ColumnTypes::VCHR || column.column_type == ColumnTypes::FCHR) {
			return "";
		} else if (column.column_type == ColumnTypes::DATE) {
			return "2000-01-01";
		} else if (column.column_type == ColumnTypes::TIME) {
			return "00:00:00";
		} else if (column.column_type == ColumnTypes::DTTM) {
			return "2000-01-01 00:00:00";
		} else {
			return "0"; // covers bool too - quoted '0' values will be accepted by mysql, but quoted 'false' values wouldn't
		}
	}
};

template <typename DatabaseClient, bool = is_base_of<DropKeysWhenColumnsDropped, DatabaseClient>::value>
struct UpdateKeyForDroppedColumn {
	static bool update_key_columns(ColumnIndices &key_columns, bool unique, size_t column_index) {
		ColumnIndices::iterator column = key_columns.begin();
		while (column != key_columns.end()) {
			if (*column == column_index) {
				column = key_columns.erase(column);
				if (unique) return false; // after https://jira.mariadb.org/browse/MDEV-13613 we can't reliably remove columns from unique keys
			} else {
				if (*column > column_index) {
					--*column;
				}
				++column;
			}
		}
		return (!key_columns.empty()); // key successfully updated, unless it's now empty, in which case the key will be dropped by the database server
	}
};

template <typename DatabaseClient>
struct UpdateKeyForDroppedColumn <DatabaseClient, true> {
	static bool update_key_columns(ColumnIndices &key_columns, bool unique, size_t column_index) {
		ColumnIndices::iterator column = key_columns.begin();
		while (column != key_columns.end()) {
			if (*column == column_index) {
				return false; // key will be dropped by the database server, not updated
			} else {
				if (*column > column_index) {
					--*column;
				}
				++column;
			}
		}
		return true; // key successfully updated
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

		if (!UpdateKeyForDroppedColumn<DatabaseClient>::update_key_columns(table.primary_key_columns, true, column_index)) {
			table.primary_key_columns.clear(); // so the table compares not-equal and gets recreated
		} else {
			Keys::iterator key = table.keys.begin();
			while (key != table.keys.end()) {
				if (UpdateKeyForDroppedColumn<DatabaseClient>::update_key_columns(key->columns, key->unique, column_index)) {
					++key;
				} else {
					// proactively drop the key at the start to work around one case of https://bugs.mysql.com/bug.php?id=57497 -
					// the single-column index case.  we also use this to avoid the new behavior from https://jira.mariadb.org/browse/MDEV-13613.
					DropKeyStatements<DatabaseClient>::add_to(alter_statements, client, table, *key);
					key = table.keys.erase(key);
				}
			}
		}
	}
};

template <typename DatabaseClient, bool = is_base_of<DatabaseClient, SupportsAddNonNullableColumns>::value>
struct AddColumnClauses {
	static void add_to(string &alter_table_clauses, string &second_round_alter_table_clauses, DatabaseClient &client, Table &table, const Column &column) {
		if (!alter_table_clauses.empty()) {
			alter_table_clauses += ",";
		}
		alter_table_clauses += " ADD ";
		if (column.nullable || column.default_type == DefaultType::default_value || column.default_type == DefaultType::default_function) {
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

	void match_table(Table &from_table, Table &to_table) {
		// sort the key lists so they have the same order; we consider keys to be unordered
		sort(from_table.keys.begin(), from_table.keys.end());
		sort(  to_table.keys.begin(),   to_table.keys.end());

		// turn off any flags not supported by the target database
		mask_unsupported_flags(from_table);

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
			DropTableStatements<DatabaseClient>::add_to(statements, client, to_table);
			CreateTableStatements<DatabaseClient>::add_to(statements, client, from_table);
			to_table = from_table;
		}
	}

	void mask_unsupported_flags(Table &from_table) {
		for (Column &column : from_table.columns) {
			ColumnFlags masked_flags = (ColumnFlags)(column.flags & client.supported_flags());
			if (column.flags != masked_flags) {
				cerr << "Warning: the schema for column " << from_table.name << '.' << column.name << " is not fully supported by this database." << endl;
				column.flags = masked_flags;
			}
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

		while (to_key != to_table.keys.end()) {
			if (from_key == from_table.keys.end() ||
				from_key->name > to_key->name) {
				// our end has an extra key, drop it
				DropKeyStatements<DatabaseClient>::add_to(alter_statements, client, to_table, *to_key);
				to_key = to_table.keys.erase(to_key);
				// keep the current from_key and re-evaluate on the next iteration

			} else if (to_key->name > from_key->name) {
				// their end has an extra key, add it
				CreateKeyStatements<DatabaseClient>::add_to(alter_statements, client, to_table, *from_key);
				to_key = ++to_table.keys.insert(to_key, *from_key);
				++from_key;
				// keep the current to_key and re-evaluate on the next iteration

			} else {
				match_key(alter_statements, from_table, *from_key, *to_key);
				++to_key;
				++from_key;
			}
		}

		while (from_key != from_table.keys.end()) {
			CreateKeyStatements<DatabaseClient>::add_to(alter_statements, client, to_table, *from_key);
			to_key = ++to_table.keys.insert(to_key, *from_key);
			++from_key;
		}
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
					(from_column->default_type != DefaultType::sequence)) {
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
			UpdateTableStatements<DatabaseClient>::add_to(alter_statements, client, to_table, update_table_clauses);
		}
		if (!alter_table_clauses.empty()) {
			AlterTableStatements<DatabaseClient>::add_to(alter_statements, client, to_table, alter_table_clauses);
		}
		if (!second_round_alter_table_clauses.empty()) {
			AlterTableStatements<DatabaseClient>::add_to(alter_statements, client, to_table, second_round_alter_table_clauses);
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

	void match_key(Statements &alter_statements, const Table &from_table, const Key &from_key, Key &to_key) {
		if (from_key == to_key) return;

		// recreate the index.  not all databases can combine these two statements, so we implement the general case only for now.
		DropKeyStatements<DatabaseClient>::add_to(alter_statements, client, from_table, to_key);
		CreateKeyStatements<DatabaseClient>::add_to(alter_statements, client, from_table, from_key);
		to_key = from_key;
	}

	DatabaseClient &client;
	Statements statements;
};

#endif
