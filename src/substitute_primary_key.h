#ifndef SUBSTITUTE_PRIMARY_KEY_H
#define SUBSTITUTE_PRIMARY_KEY_H

#include <algorithm>

struct ColumnNullableOrReplaced {
	ColumnNullableOrReplaced(const Table &table): table(table) {}

	inline bool operator()(const Column &column) {
		return (column.nullable || !column.filter_expression.empty());
	}

	inline bool operator()(size_t column) {
		return (*this)(table.columns[column]);
	}

	const Table &table;
};

inline void choose_primary_key_for(Table &table) {
	// generally we expect most tables to have a real primary key
	if (table.primary_key_type == PrimaryKeyType::explicit_primary_key) return;

	ColumnNullableOrReplaced usable(table);

	// if not, we want to find a unique key with no nullable columns to act as a surrogate primary key.
	// we also need to ensure there's no replace: expression applied as the WHERE conditions would
	// apply to the actual values, which presumably don't match the replaced values.
	for (const Key &key : table.keys) {
		if (key.unique() && none_of(key.columns.begin(), key.columns.end(), usable)) {
			table.primary_key_columns = key.columns;
			table.primary_key_type = PrimaryKeyType::suitable_unique_key;
			return;
		}
	}

	// if there's no unique key usable as a pseudo-primary key, we can try to treat the whole row as
	// if it were the primary key and group and count to spot duplicates.  that's only possible if
	// there are no nullable columns, though; otherwise we can't query based on key ranges, since the
	// comparison operators like > and <= will return NULL for any comparisons involving NULL values.
	if (none_of(table.columns.begin(), table.columns.end(), usable)) {
		// ok, no nullable columns, so in principle we can use the whole row as its own primary key.
		// but tables like that are potentially very slow to query because the database may not have
		// any good way to sort the rows, and we can't assume that it will happen to serve them up
		// in the same order at both ends; look for an index with all the columns in it.
		for (const Key &key : table.keys) {
			if (key.columns.size() == table.columns.size()) {
				table.primary_key_columns = key.columns;
				table.primary_key_type = PrimaryKeyType::entire_row_as_key;
				return;
			}
		}
	}
}

#endif
