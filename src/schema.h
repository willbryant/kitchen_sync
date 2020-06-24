#ifndef SCHEMA_H
#define SCHEMA_H

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include "message_pack/packed_value.h"
#include "column_types.h"

using namespace std;

typedef vector<size_t> ColumnIndices;
typedef vector<PackedValue> ColumnValues;
typedef set<ColumnType> ColumnTypeList;

enum class DefaultType {
	// serialized by name not value, so the values here can be changed if required
	no_default = 0,
	default_value = 1,

	generated_by_sequence = 64,
	generated_by_default_as_identity = 65,
	generated_always_as_identity = 66,

	default_expression = 127,
	generated_always_virtual = 128,
	generated_always_stored = 129,
};

enum class AutoUpdateType {
	no_auto_update,
	current_timestamp,
};

struct Column {
	string name;
	bool nullable = true; // so we only serialize if changed to false
	ColumnType column_type = ColumnType::unknown;
	size_t size = 0;
	size_t scale = 0;
	DefaultType default_type = DefaultType::no_default;
	string default_value;
	AutoUpdateType auto_update_type = AutoUpdateType::no_auto_update;
	string subtype;
	string reference_system;
	vector<string> enumeration_values;

	// the following member isn't serialized currently (could be, but not required):
	string filter_expression;

	inline bool operator ==(const Column &other) const {
		return (name == other.name &&
				nullable == other.nullable &&
				column_type == other.column_type &&
				size == other.size &&
				scale == other.scale &&
				default_type == other.default_type &&
				default_value == other.default_value &&
				auto_update_type == other.auto_update_type &&
				subtype == other.subtype &&
				reference_system == other.reference_system &&
				enumeration_values == other.enumeration_values);
	}
	inline bool operator !=(const Column &other) const { return (!(*this == other)); }

	inline bool values_need_quoting() const {
		return (column_type < ColumnType::non_quoted_literals_min || column_type > ColumnType::non_quoted_literals_max);
	}

	inline bool generated_always() const { return (default_type >= DefaultType::generated_always_virtual); }
};

typedef vector<Column> Columns;
typedef vector<string> ColumnNames;

enum class KeyType {
	unique_key = 0,
	standard_key = 1,
	spatial_key = 2,
};

struct Key {
	string name;
	KeyType key_type;
	ColumnIndices columns;

	inline Key(const string &name, KeyType key_type): name(name), key_type(key_type) {}
	inline Key(): key_type(KeyType::standard_key) {}

	inline bool standard() const { return (key_type == KeyType::standard_key); }
	inline bool unique() const { return (key_type == KeyType::unique_key); }
	inline bool spatial() const { return (key_type == KeyType::spatial_key); }

	inline bool operator <(const Key &other) const { return (key_type != other.key_type ? key_type < other.key_type : name < other.name); }
	inline bool operator ==(const Key &other) const { return (name == other.name && key_type == other.key_type && columns == other.columns); }
	inline bool operator !=(const Key &other) const { return (!(*this == other)); }
};

typedef vector<Key> Keys;

enum class PrimaryKeyType {
	no_available_key = 0,
	explicit_primary_key = 1,
	suitable_unique_key = 2,
	entire_row_as_key = 3,
};

struct Table {
	string schema_name; // only used on databases supporting multiple schema, and only set when not the default (eg. "public") for that database server
	string name;
	Columns columns;
	ColumnIndices primary_key_columns;
	PrimaryKeyType primary_key_type = PrimaryKeyType::no_available_key;
	Keys keys;

	// the following member isn't serialized currently (could be, but not required):
	string where_conditions;

	inline Table(const string &schema_name, const string &name): schema_name(schema_name), name(name) {}
	inline Table() {}

	inline const string id_from_name() const { return (schema_name.empty() ? "" : escape_name_for_id(schema_name) + '.') + escape_name_for_id(name); }

	inline bool operator <(const Table &other) const { return (schema_name == other.schema_name ? name < other.name : schema_name < other.schema_name); }
	inline bool operator ==(const Table &other) const { return (schema_name == other.schema_name && name == other.name && columns == other.columns && same_primary_key_as(other) && keys == other.keys); }
	inline bool operator !=(const Table &other) const { return (!(*this == other)); }
	size_t index_of_column(const string &name) const;

	inline bool enforceable_primary_key() const { return (primary_key_type == PrimaryKeyType::explicit_primary_key || primary_key_type == PrimaryKeyType::suitable_unique_key); }
	inline bool group_and_count_entire_row() const { return (primary_key_type == PrimaryKeyType::entire_row_as_key); }

protected:
	inline bool same_primary_key_as(const Table &other) const {
		size_t this_explicit_columns = primary_key_type == PrimaryKeyType::explicit_primary_key ? primary_key_columns.size() : 0;
		size_t that_explicit_columns = other.primary_key_type == PrimaryKeyType::explicit_primary_key ? other.primary_key_columns.size() : 0;
		return (this_explicit_columns == that_explicit_columns && equal(primary_key_columns.begin(), primary_key_columns.begin() + this_explicit_columns, other.primary_key_columns.begin()));
	}

	inline string escape_name_for_id(const string &str) const {
		string result(str);
		for (size_t pos = 0; (pos = result.find(".\\", pos)) != string::npos; pos += 2) {
			result.insert(pos, 1, '\\');
		}
		return result;
	}
};

typedef vector<Table> Tables;

struct Database {
	Tables tables;
	vector<string> errors;
};

#endif
