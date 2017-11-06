#ifndef FILTERS_H
#define FILTERS_H

#include <string>
#include <vector>
#include <map>

using namespace std;

struct TableFilter {
	string where_conditions;
	map<string, string> filter_expressions;
};

typedef map<string, TableFilter> TableFilters;

TableFilters load_filters(const string &filters_file);

#endif
