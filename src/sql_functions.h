#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"

using namespace std;

string columns_list(const vector<string> &column_names);
string columns_list(const Columns &columns, const ColumnIndices &column_indices);
string escape_non_binary_string(const string &str);
string non_binary_string_values_list(const vector<string> &values);
string where_sql(const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key, const char *prefix = " WHERE ");

#endif
