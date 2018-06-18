#ifndef SUBDIVISION_H
#define SUBDIVISION_H

#include "schema.h"

bool primary_key_subdividable(const Table &table);
ColumnValues subdivide_primary_key_range(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key);

#endif
