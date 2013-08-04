#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"

using namespace std;

string columns_list(const vector<string> &column_names);
string columns_list(const Columns &columns, const ColumnIndices &column_indices);
string non_binary_string_values_list(const vector<string> &values);

#endif
