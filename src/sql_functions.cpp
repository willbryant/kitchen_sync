#include "sql_functions.h"

string columns_list(const vector<string> &column_names) {
	if (column_names.empty()) {
		return "(NULL)";
	}

	string result("(");
	result.append(*column_names.begin());
	for (vector<string>::const_iterator column_name = column_names.begin() + 1; column_name != column_names.end(); ++column_name) {
		result.append(", ");
		result.append(*column_name);
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