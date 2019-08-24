#ifndef FILTERS_H
#define FILTERS_H

#include <string>
#include <vector>
#include <map>

#include "schema.h"

using namespace std;

struct filter_definition_error: public runtime_error {
	filter_definition_error(const string &error): runtime_error(error) { }
};

struct TableFilter {
	string where_conditions;
	map<string, string> filter_expressions;
};

typedef map<string, TableFilter> TableFilters;

TableFilters load_filters(const string &filters_file);

void apply_filters(const TableFilters &table_filters, Tables &tables);

#endif
