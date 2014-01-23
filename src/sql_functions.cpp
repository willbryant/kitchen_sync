#include "sql_functions.h"

string columns_list(const vector<string> &column_names, char quote_identifiers_with) {
	if (column_names.empty()) {
		return "(NULL)";
	}

	string result("(");
	result.append(*column_names.begin());
	for (vector<string>::const_iterator column_name = column_names.begin() + 1; column_name != column_names.end(); ++column_name) {
		result.append(", ");
		if (quote_identifiers_with) result += quote_identifiers_with;
		result.append(*column_name);
		if (quote_identifiers_with) result += quote_identifiers_with;
	}
	result.append(")");
	return result;
}

string columns_list(const Columns &columns, const ColumnIndices &column_indices, char quote_identifiers_with){
	if (column_indices.empty()) {
		return "(NULL)";
	}

	string result("(");
	result.append(columns[*column_indices.begin()].name);
	for (ColumnIndices::const_iterator column_index = column_indices.begin() + 1; column_index != column_indices.end(); ++column_index) {
		result.append(", ");
		if (quote_identifiers_with) result += quote_identifiers_with;
		result.append(columns[*column_index].name);
		if (quote_identifiers_with) result += quote_identifiers_with;
	}
	result.append(")");
	return result;
}

string escape_non_binary_string(const string &str) {
	string result;
	result.reserve(str.size());
	for (string::const_iterator p = str.begin(); p != str.end(); ++p) {
		if (*p == '\'' || *p == '\\') result += '\\';
		result += *p;
	}
	return result;
}

string non_binary_string_values_list(const vector<string> &values) {
	if (values.empty()) {
		return "(NULL)";
	}

	string result("('");
	result.append(*values.begin());
	for (vector<string>::const_iterator value = values.begin() + 1; value != values.end(); ++value) {
		result.append("', '");
		result.append(escape_non_binary_string(*value));
	}
	result.append("')");
	return result;
}

string where_sql(const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key, const char *prefix) {
	string result;
	if (!prev_key.empty() || !last_key.empty())	result += prefix;
	if (!prev_key.empty())						result += key_columns + " > " + non_binary_string_values_list(prev_key);
	if (!prev_key.empty() && !last_key.empty())	result += " AND ";
	if (!last_key.empty()) 						result += key_columns + " <= " + non_binary_string_values_list(last_key);
	return result;
}
