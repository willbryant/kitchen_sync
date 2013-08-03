#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

using namespace std;

string columns_list(const vector<string> &column_names);
string non_binary_string_values_list(const vector<string> &values);

#endif
