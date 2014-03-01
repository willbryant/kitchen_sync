#ifndef FILTERS_H
#define FILTERS_H

#include "schema.h"

void load_filters(const char *filters_file, map<string, Table*> &tables_by_name);

#endif
