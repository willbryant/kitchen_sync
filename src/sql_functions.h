#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"
#include "encode_packed.h"

using namespace std;

const string ASCENDING("ASC");
const string DESCENDING("DESC");

template <typename DatabaseClient>
string column_orders_list(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices, const string &order = ASCENDING) {
	string result(" ORDER BY ");

	result += client.quote_identifiers_with();
	result += columns[*column_indices.begin()].name;
	result += client.quote_identifiers_with();
	result += ' ';
	result += order;

	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result += ", ";
		result += client.quote_identifiers_with();
		result += columns[*column_index].name;
		result += client.quote_identifiers_with();
		result += ' ';
		result += order;
	}

	return result;
}

template <typename DatabaseClient>
string columns_list(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices) {
	string result;

	result += client.quote_identifiers_with();
	result += columns[*column_indices.begin()].name;
	result += client.quote_identifiers_with();

	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result += ", ";
		result += client.quote_identifiers_with();
		result += columns[*column_index].name;
		result += client.quote_identifiers_with();
	}

	return result;
}

template <typename DatabaseClient>
string columns_tuple(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices) {
	if (column_indices.empty()) {
		return "(NULL)";
	}

	return "(" + columns_list(client, columns, column_indices) + ")";
}

template <typename DatabaseClient>
string values_list(DatabaseClient &client, const Table &table, const ColumnValues &values) {
	if (values.empty()) {
		return "(NULL)";
	}

	string result("(");
	for (size_t n = 0; n < table.primary_key_columns.size(); n++) {
		if (n > 0) {
			result += ',';
		}
		result += encode(client, table.columns[table.primary_key_columns[n]], values[n]);
	}
	result += ")";
	return result;
}

template <typename DatabaseClient>
string where_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &extra_where_conditions = "", const char *prefix = " WHERE ") {
	string key_columns(columns_tuple(client, table.columns, table.primary_key_columns));
	string result;
	if (!prev_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " > ";
		result += values_list(client, table, prev_key);
		prefix = " AND ";
	}
	if (!last_key.empty()) {
		result += prefix;
		result += key_columns;
		result += " <= ";
		result += values_list(client, table, last_key);
		prefix = " AND ";
	}
	if (!extra_where_conditions.empty()) {
		result += prefix;
		result += "(";
		result += extra_where_conditions;
		result += ")";
	}
	return result;
}

template <typename DatabaseClient>
string select_columns_sql(DatabaseClient &client, const Table &table) {
	string result;
	for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
		if (column != table.columns.begin()) result += ", ";
		if (!column->filter_expression.empty()) {
			result += column->filter_expression;
			result += " AS ";
		}
		result += client.quote_identifiers_with();
		result += column->name;
		result += client.quote_identifiers_with();
	}
	return result;
}

const ssize_t NO_ROW_COUNT_LIMIT = -1;

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
	string result("SELECT ");
	result += select_columns_sql(client, table);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, table, prev_key, last_key, table.where_conditions);
	result += column_orders_list(client, table.columns, table.primary_key_columns);
	if (row_count != NO_ROW_COUNT_LIMIT) {
		result += " LIMIT " + to_string(row_count);
	}
	return result;
}

template <typename DatabaseClient>
string count_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string result("SELECT COUNT(*) FROM ");
	result += table.name;
	result += where_sql(client, table, prev_key, last_key, table.where_conditions);
	return result;
}

template <typename DatabaseClient>
string select_first_key_sql(DatabaseClient &client, const Table &table) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, table, ColumnValues(), ColumnValues(), table.where_conditions);
	result += column_orders_list(client, table.columns, table.primary_key_columns, ASCENDING);
	result += " LIMIT 1";
	return result;
}

template <typename DatabaseClient>
string select_last_key_sql(DatabaseClient &client, const Table &table) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += table.name;
	result += where_sql(client, table, ColumnValues(), ColumnValues(), table.where_conditions);
	result += column_orders_list(client, table.columns, table.primary_key_columns, DESCENDING);
	result += " LIMIT 1";
	return result;
}

#endif
