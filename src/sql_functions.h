#ifndef SQL_FUNCTIONS_H
#define SQL_FUNCTIONS_H

#include <string>
#include <vector>

#include "schema.h"

using namespace std;

string columns_list(const vector<string> &column_names, char quote_identifiers_with = 0);
string columns_list(const Columns &columns, const ColumnIndices &column_indices, char quote_identifiers_with = 0);
string escape_non_binary_string(const string &str);
string non_binary_string_values_list(const vector<string> &values);
string where_sql(const string &key_columns, const ColumnValues &prev_key, const ColumnValues &last_key, const string &extra_where_conditions = "", const char *prefix = " WHERE ");
string retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, size_t row_count, char quote_identifiers_with = 0);
string retrieve_rows_sql(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, char quote_identifiers_with = 0);
string count_rows_sql(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, char quote_identifiers_with = 0);

#endif
