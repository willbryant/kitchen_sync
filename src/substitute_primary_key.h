#ifndef SUBSTITUTE_PRIMARY_KEY_H
#define SUBSTITUTE_PRIMARY_KEY_H

inline bool any_column_nullable(const Table &table, const ColumnIndices &columns) {
	for (size_t column : columns) {
		if (table.columns[column].nullable) return true;
	}
	return false;
}

inline void choose_primary_key_for(Table &table) {
	// generally we expect most tables to have a real primary key
	if (!table.primary_key_columns.empty()) return;

	// if not, we need to find a unique key with no nullable columns to act as a surrogate primary key
	for (const Key &key : table.keys) {
		if (key.unique && !any_column_nullable(table, key.columns)) {
			table.primary_key_columns = key.columns;
			table.primary_key_type = suitable_unique_key;
			return;
		}
	}
}

#endif
