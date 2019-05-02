#ifndef SUBSTITUTE_PRIMARY_KEY_H
#define SUBSTITUTE_PRIMARY_KEY_H

#include <algorithm>
#include <set>

struct ColumnNullable {
	ColumnNullable(const Table &table): table(table) {}

	inline bool operator()(const Column &column) { return column.nullable; }
	inline bool operator()(size_t column) { return table.columns[column].nullable; }

	const Table &table;
};

template <typename DatabaseClient>
size_t cardinality(DatabaseClient &client, const Table &table, const ColumnIndices &column_indices) {
	return atoi(client.select_one(count_distinct_values_sql(client, table, column_indices)).c_str());
}

template <typename DatabaseClient>
const Key *highest_cardinality_key(DatabaseClient &client, const Table &table, const vector<const Key*> &candidate_keys) {
	ColumnNullable nullable(table);
	const Key *best_candidate = nullptr;
	size_t best_candidate_cardinality = 0;

	for (const Key *key : candidate_keys) {
		ColumnIndices::const_iterator first_nullable = find_if(key->columns.cbegin(), key->columns.cend(), nullable);

		size_t this_candidate_cardinality = cardinality(client, table, ColumnIndices(key->columns.cbegin(), first_nullable));

		if (!best_candidate || this_candidate_cardinality > best_candidate_cardinality) {
			best_candidate = key;
			best_candidate_cardinality = this_candidate_cardinality;
		}
	}

	return best_candidate;
}

void configure_partial_key(Table &table, const Key &key) {
	ColumnNullable nullable(table);
	ColumnIndices::const_iterator first_nullable = find_if(key.columns.cbegin(), key.columns.cend(), nullable);

	table.primary_key_type = partial_key;
	table.primary_key_columns.insert(table.primary_key_columns.begin(), key.columns.cbegin(), first_nullable);
	table.secondary_sort_columns.insert(table.secondary_sort_columns.begin(), first_nullable, key.columns.cend());

	// add on any missing columns, arbitrarily keeping with the order that they're in
	if (table.primary_key_columns.size() + table.secondary_sort_columns.size() < table.columns.size()) {
		set<size_t> columns_in_key(table.primary_key_columns.begin(), table.primary_key_columns.end());
		columns_in_key.insert(table.secondary_sort_columns.begin(), table.secondary_sort_columns.end());

		for (size_t column = 0; column < table.columns.size(); ++column) {
			if (!columns_in_key.count(column)) {
				table.secondary_sort_columns.push_back(column);
			}
		}
	}
}

template <typename DatabaseClient>
void choose_primary_key_for(DatabaseClient &client, Table &table) {
	// generally we expect most tables to have a real primary key
	if (table.primary_key_type == explicit_primary_key) return;

	ColumnNullable nullable(table);

	// if not, we need to find a unique key with no nullable columns to act as a surrogate primary key
	for (const Key &key : table.keys) {
		if (key.unique && none_of(key.columns.begin(), key.columns.end(), nullable)) {
			table.primary_key_type = suitable_unique_key;
			table.primary_key_columns = key.columns;
			return;
		}
	}

	// failing that, look for any key we could use to at least partially order the rows. if the columns
	// are all non-nullable, start by looking for a key that covers *all* the columns in the table.
	// that's a bit niche, but it covers patterns like "join tables" that are just pairs of foreign
	// keys (note that even in that case, there could be entire duplicate rows, so we still can't use
	// this like a real PK). taking it one step further, if we see a key that covers all of the columns
	// in the table that *are* non-nullable, then we know that's as good as we can do anyway.
	size_t number_of_non_nullable_columns = table.columns.size() - count_if(table.columns.begin(), table.columns.end(), nullable);

	for (const Key &key : table.keys) {
		if (key.columns.size() - count_if(key.columns.begin(), key.columns.end(), nullable) == number_of_non_nullable_columns) {
			configure_partial_key(table, key);
			return;
		}
	}

	// ok, see which (if any) keys have at least some non-nullable columns at the start we could use.
	vector<const Key*> candidate_keys;

	for (const Key &key : table.keys) {
		if (!nullable(key.columns.front())) {
			candidate_keys.push_back(&key);
		}
	}

	// if there's no keys with non-nullable columns at the start, we can't order the rows even partially.
	// if there's more than one candidate, we need to choose, and there's no real way to make a choice
	// based on the schema alone - we have to look at the data as we'd prefer the key with the highest
	// cardinality to get as close as possible to one row per key (like any real or substitute primary
	// key would have). this is obviously expensive to determine - it's much better to have real PKs!
	if (candidate_keys.size() > 1) {
		configure_partial_key(table, *highest_cardinality_key(client, table, candidate_keys));
	} else if (!candidate_keys.empty()) {
		configure_partial_key(table, *candidate_keys.front());
	}
}

#endif
