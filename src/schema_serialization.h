#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "message_pack/unpack.h"

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Column &column) {
	int fields = 2;
	if (column.size) fields++;
	if (column.scale) fields++;
	if (!column.nullable) fields++;
	if (!column.type_restriction.empty()) fields++;
	if (!column.reference_system.empty()) fields++;
	if (!column.enumeration_values.empty()) fields++;
	if (!column.db_type_def.empty()) fields++;
	if (column.default_type != DefaultType::no_default) fields++;
	if (column.flags.mysql_timestamp) fields++;
	if (column.flags.mysql_on_update_timestamp) fields++;
	if (column.flags.time_zone) fields++;
	if (column.flags.simple_geometry) fields++;
	if (column.flags.binary_storage) fields++;
	if (column.flags.identity_generated_always) fields++;
	pack_map_length(packer, fields);
	packer << string("name");
	packer << column.name;
	packer << string("column_type");
	packer << column.column_type;
	if (column.size) {
		packer << string("size");
		packer << column.size;
	}
	if (column.scale) {
		packer << string("scale");
		packer << column.scale;
	}
	if (!column.nullable) {
		packer << string("nullable");
		packer << column.nullable;
	}
	if (!column.type_restriction.empty()) {
		packer << string("type_restriction");
		packer << column.type_restriction;
	}
	if (!column.reference_system.empty()) {
		packer << string("reference_system");
		packer << column.reference_system;
	}
	if (!column.enumeration_values.empty()) {
		packer << string("enumeration_values");
		packer << column.enumeration_values;
	}
	if (!column.db_type_def.empty()) {
		packer << string("db_type_def");
		packer << column.db_type_def;
	}
	switch (column.default_type) {
		case DefaultType::no_default:
			break;

		case DefaultType::sequence:
			packer << string("sequence");
			packer << column.default_value; // currently unused, but allowed for forward compatibility
			break;

		case DefaultType::default_value:
			packer << string("default_value");
			packer << column.default_value;
			break;

		case DefaultType::default_expression:
			packer << string("default_function"); // legacy name
			packer << column.default_value;
			break;
	}
	if (column.flags.mysql_timestamp) {
		packer << string("mysql_timestamp");
		packer << true;
	}
	if (column.flags.mysql_on_update_timestamp) {
		packer << string("mysql_on_update_timestamp");
		packer << true;
	}
	if (column.flags.time_zone) {
		packer << string("time_zone");
		packer << true;
	}
	if (column.flags.simple_geometry) {
		packer << string("simple_geometry");
		packer << true;
	}
	if (column.flags.binary_storage) {
		packer << string("binary_storage");
		packer << true;
	}
	if (column.flags.identity_generated_always) {
		packer << string("identity_generated_always");
		packer << true;
	}
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Key &key) {
	pack_map_length(packer, 3);
	packer << string("name");
	packer << key.name;
	switch (key.key_type) {
		case KeyType::standard_key:
			// we send the "unique" flag for backwards compatibility with v1.17 and earlier; therefore, there's no point in sending "key_type" as well
			packer << string("unique");
			packer << false;
			break;

		case KeyType::unique_key:
			// as for KeyType::standard_key
			packer << string("unique");
			packer << true;
			break;

		case KeyType::spatial_key:
			packer << string("key_type");
			packer << string("spatial");
			break;
	}
	packer << string("columns");
	packer << key.columns;
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Table &table) {
	pack_map_length(packer, 5);
	packer << string("name");
	packer << table.name;
	packer << string("columns");
	packer << table.columns;
	packer << string("primary_key_columns");
	packer << table.primary_key_columns;
	packer << string("primary_key_type");
	packer << static_cast<int>(table.primary_key_type); // unfortunately this was implemented as value-serialised, unlike the other enums
	packer << string("keys");
	packer << table.keys;
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Database &database) {
	pack_map_length(packer, 1);
	packer << string("tables");
	packer << database.tables;
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Column &column) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			unpacker >> column.name;
		} else if (attr_key == "column_type") {
			unpacker >> column.column_type;
		} else if (attr_key == "size") {
			unpacker >> column.size;
		} else if (attr_key == "scale") {
			unpacker >> column.scale;
		} else if (attr_key == "nullable") {
			unpacker >> column.nullable;
		} else if (attr_key == "type_restriction") {
			unpacker >> column.type_restriction;
		} else if (attr_key == "reference_system") {
			unpacker >> column.reference_system;
		} else if (attr_key == "enumeration_values") {
			unpacker >> column.enumeration_values;
		} else if (attr_key == "db_type_def") {
			unpacker >> column.db_type_def;
		} else if (attr_key == "sequence") {
			column.default_type = DefaultType::sequence;
			unpacker >> column.default_value; // currently unused, but allowed for forward compatibility
		} else if (attr_key == "default_value") {
			column.default_type = DefaultType::default_value;
			unpacker >> column.default_value;
		} else if (attr_key == "default_function") { // legacy name
			column.default_type = DefaultType::default_expression;
			unpacker >> column.default_value;
		} else if (attr_key == "mysql_timestamp") {
			unpacker >> column.flags.mysql_timestamp;
		} else if (attr_key == "mysql_on_update_timestamp") {
			unpacker >> column.flags.mysql_on_update_timestamp;
		} else if (attr_key == "time_zone") {
			unpacker >> column.flags.time_zone;
		} else if (attr_key == "simple_geometry") {
			unpacker >> column.flags.simple_geometry;
		} else if (attr_key == "binary_storage") {
			unpacker >> column.flags.binary_storage;
		} else if (attr_key == "identity_generated_always") {
			unpacker >> column.flags.identity_generated_always;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Key &key) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			unpacker >> key.name;
		} else if (attr_key == "unique") {
			key.key_type = (unpacker.template next<bool>() ? KeyType::unique_key : KeyType::standard_key);
		} else if (attr_key == "key_type") {
			string key_type(unpacker.template next<string>());
			if (key_type == "standard") {
				key.key_type = KeyType::standard_key;
			} else if (key_type == "unique") {
				key.key_type = KeyType::unique_key;
			} else if (key_type == "spatial") {
				key.key_type = KeyType::spatial_key;
			}
		} else if (attr_key == "columns") {
			unpacker >> key.columns;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Table &table) {
	size_t map_length = unpacker.next_map_length(); // checks type

	bool primary_key_type_set = false;

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			unpacker >> table.name;
		} else if (attr_key == "columns") {
			unpacker >> table.columns;
		} else if (attr_key == "primary_key_columns") {
			unpacker >> table.primary_key_columns;
		} else if (attr_key == "primary_key_type") {
			// unfortunately this was implemented as value-serialised, unlike the other enums
			table.primary_key_type = static_cast<PrimaryKeyType>(unpacker.template next<int>());
			primary_key_type_set = true;
		} else if (attr_key == "keys") {
			unpacker >> table.keys;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}

	// backwards compatibility with v1.13 and earlier, which didn't have primary_key_type
	if (!primary_key_type_set) {
		table.primary_key_type = table.primary_key_columns.empty() ? PrimaryKeyType::no_available_key : PrimaryKeyType::explicit_primary_key;
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Database &database) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "tables") {
			unpacker >> database.tables;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

#endif
