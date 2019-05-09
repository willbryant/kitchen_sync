#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"
#include "encode_packed.h"

using namespace std;

const string ASCENDING("ASC");
const string DESCENDING("DESC");

string quote_identifier(const string &name, char quote) {
	string result = quote + name + quote;
	for (size_t pos = 1; (pos = result.find(quote, pos)) != result.length() - 1; pos += 2) {
		result.insert(pos, 1, quote);
	}
	return result;
}

template <typename DatabaseClient>
string column_orders_list(DatabaseClient &client, const Table &table, const string &order = ASCENDING) {
	if (table.primary_key_columns.empty()) return "";

	string result(" ORDER BY ");

	for (ColumnIndices::const_iterator column_index = table.primary_key_columns.begin(); column_index != table.primary_key_columns.end(); ++column_index) {
		if (column_index != table.primary_key_columns.begin()) result += ", ";
		result += client.quote_identifier(table.columns[*column_index].name);
		result += ' ';
		result += order;
	}

	return result;
}

template <typename DatabaseClient>
string columns_list(DatabaseClient &client, const Columns &columns, const ColumnIndices &column_indices) {
	string result;

	result += client.quote_identifier(columns[*column_indices.begin()].name);

	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result += ", ";
		result += client.quote_identifier(columns[*column_index].name);
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
string where_sql(DatabaseClient &client, const Table &table, const char *op1, const ColumnValues &key1, const char *op2, const ColumnValues &key2, const char *op3, const ColumnValues &key3, const string &extra_where_conditions = "") {
	const char *prefix = " WHERE ";
	string key_columns(columns_tuple(client, table.columns, table.primary_key_columns));
	string result;
	if (!key1.empty()) {
		result += prefix;
		result += key_columns;
		result += op1;
		result += values_list(client, table, key1);
		prefix = " AND ";
	}
	if (!key2.empty()) {
		result += prefix;
		result += key_columns;
		result += op2;
		result += values_list(client, table, key2);
		prefix = " AND ";
	}
	if (!key3.empty()) {
		result += prefix;
		result += key_columns;
		result += op3;
		result += values_list(client, table, key3);
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
inline string where_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &extra_where_conditions = "") {
	return where_sql(client, table, " > ", prev_key, " <= ", last_key, "", ColumnValues(), extra_where_conditions);
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
		result += client.quote_identifier(column->name);
	}
	return result;
}

const ssize_t NO_ROW_COUNT_LIMIT = -1;

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
	string result("SELECT ");
	result += select_columns_sql(client, table);
	result += " FROM ";
	result += client.quote_identifier(table.name);
	result += where_sql(client, table, prev_key, last_key, table.where_conditions);
	result += column_orders_list(client, table);
	if (row_count != NO_ROW_COUNT_LIMIT) {
		result += " LIMIT " + to_string(row_count);
	}
	return result;
}

template <typename DatabaseClient>
string count_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string result("SELECT COUNT(*) FROM ");
	result += client.quote_identifier(table.name);
	result += where_sql(client, table, prev_key, last_key, table.where_conditions);
	return result;
}

template <typename DatabaseClient>
string select_first_key_sql(DatabaseClient &client, const Table &table) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += client.quote_identifier(table.name);
	result += where_sql(client, table, ColumnValues(), ColumnValues(), table.where_conditions);
	result += column_orders_list(client, table, ASCENDING);
	result += " LIMIT 1";
	return result;
}

template <typename DatabaseClient>
string select_last_key_sql(DatabaseClient &client, const Table &table) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += client.quote_identifier(table.name);
	result += where_sql(client, table, ColumnValues(), ColumnValues(), table.where_conditions);
	result += column_orders_list(client, table, DESCENDING);
	result += " LIMIT 1";
	return result;
}

template <typename DatabaseClient>
string select_not_earlier_key_sql(DatabaseClient &client, const Table &table, const ColumnValues &key, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += client.quote_identifier(table.name);
	result += where_sql(client, table, " >= ", key, " > ", prev_key, " <= ", last_key, table.where_conditions);
	result += column_orders_list(client, table, ASCENDING);
	result += " LIMIT 1";
	return result;
}

#endif
