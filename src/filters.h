#ifndef FILTERS_H
#define FILTERS_H

#include <string>
#include <vector>
#include <map>

#include "schema.h"

using namespace std;

struct TableFilter {
	string where_conditions;
	map<string, string> filter_expressions;
};

typedef map<string, TableFilter> TableFilters;

TableFilters load_filters(const string &filters_file);

void apply_filters(const TableFilters &table_filters, Tables &tables);

#endif
