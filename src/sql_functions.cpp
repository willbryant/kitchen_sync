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
