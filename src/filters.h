#ifndef FILTERS_H
#define FILTERS_H

#include "schema.h"

void load_filters(const string &filters_file, map<string, Table*> &tables_by_name);

#endif
