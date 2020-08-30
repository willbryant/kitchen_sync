#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"
#include "encode_packed.h"
#include "packed_key.h"

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
string columns_list(DatabaseClient &client, const Columns &columns, bool include_generated_columns = false) {
	string result;

	for (Columns::const_iterator column = columns.begin(); column != columns.end(); ++column) {
		if (column->generated_always() && !include_generated_columns) continue; // normally no need to look at generated columns, which by definition are just calculated from the other columns
		if (column != columns.begin()) result += ", ";
		result += client.quote_identifier(column->name);
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

	PackedValueReadStream stream(values.data());
	Unpacker<PackedValueReadStream> unpacker(stream);

	size_t size = unpacker.next_array_length();
	if (size != table.primary_key_columns.size()) { // must move the read stream past the encoded array length anyway, so we might as well check it
		backtrace();
		throw runtime_error("read incorrect element count from key: " + to_string(size) + " vs " + to_string(table.primary_key_columns.size()));
	}

	string result("(");
	for (size_t n = 0; n < size; n++) {
		if (n > 0) {
			result += ',';
		}
		sql_encode_and_append_packed_value_to(result, client, table.columns[table.primary_key_columns[n]], stream);
	}
	result += ")";
	return result;
}

template <typename DatabaseClient>
string values_list(DatabaseClient &client, const vector<string> &values) {
	if (values.empty()) {
		return "(NULL)";
	}

	string result("('");
	for (size_t n = 0; n < values.size(); n++) {
		if (n > 0) {
			result += "', '";
		}
		result += client.escape_string_value(values[n]);
	}
	result += "')";
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

const ssize_t NO_ROW_COUNT_LIMIT = -1;

template <typename DatabaseClient>
string retrieve_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT, bool include_generated_columns = false) {
	string result("SELECT ");
	for (Columns::const_iterator column = table.columns.begin(); column != table.columns.end(); ++column) {
		if (column->generated_always() && !include_generated_columns) continue; // normally no need to look at generated columns, which by definition are just calculated from the other columns
		if (column != table.columns.begin()) result += ", ";
		if (!column->filter_expression.empty()) {
			result += column->filter_expression;
			result += " AS ";
		}
		result += client.quote_identifier(column->name);
	}
	if (table.group_and_count_entire_row()) {
		result += ", COUNT(*)";
	}

	result += " FROM ";
	result += client.quote_table_name(table);

	result += where_sql(client, table, prev_key, last_key, table.where_conditions);

	if (table.group_and_count_entire_row()) {
		result += " GROUP BY ";
		for (size_t n = 1; n <= table.columns.size(); n++) {
			if (n > 1) result += ", ";
			result += to_string(n); // we use the ordinal (position) syntax rather than column names to avoid ambiguity as to whether input or output column will be used if there is a filter expression
		}
	}

	result += column_orders_list(client, table);

	if (row_count != NO_ROW_COUNT_LIMIT) {
		result += " LIMIT " + to_string(row_count);
	}
	return result;
}

template <typename DatabaseClient>
string count_rows_sql(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	string result("SELECT COUNT(*) FROM ");
	result += client.quote_table_name(table);
	result += where_sql(client, table, prev_key, last_key, table.where_conditions);
	return result;
}

template <typename DatabaseClient>
string select_first_key_sql(DatabaseClient &client, const Table &table) {
	string result("SELECT ");
	result += columns_list(client, table.columns, table.primary_key_columns);
	result += " FROM ";
	result += client.quote_table_name(table);
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
	result += client.quote_table_name(table);
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
	result += client.quote_table_name(table);
	result += where_sql(client, table, " >= ", key, " > ", prev_key, " <= ", last_key, table.where_conditions);
	result += column_orders_list(client, table, ASCENDING);
	result += " LIMIT 1";
	return result;
}

#endif
