#ifndef SUBDIVISION_H
#define SUBDIVISION_H

#include "schema.h"
#include "row_serialization.h"

bool primary_key_subdividable(const Table &table);
ColumnValues subdivide_primary_key_range(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key);

template <typename DatabaseClient>
ColumnValues first_key_not_earlier_than(DatabaseClient &client, const Table &table, const ColumnValues &key, const ColumnValues &prev_key, const ColumnValues &last_key) {
	ColumnValues result(not_earlier_key(client, table, key, prev_key, last_key));

	if (result.empty()) {
		return prev_key;
	} else {
		return result;
	}
}

#endif
