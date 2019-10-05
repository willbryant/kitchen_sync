#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "message_pack/unpack.h"
#include "protocol_versions.h"
#include "legacy_schema_serialization.h"

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Column &column) {
	if (packer.stream().protocol_version <= LAST_LEGACY_SCHEMA_FORMAT_VERSION) {
		legacy_serialize(packer, column);
		return;
	}

	int fields = 2;
	if (column.size) fields++;
	if (column.scale) fields++;
	if (!column.nullable) fields++;
	if (!column.subtype.empty()) fields++;
	if (!column.reference_system.empty()) fields++;
	if (!column.enumeration_values.empty()) fields++;
	if (column.default_type != DefaultType::no_default) fields++;
	if (column.flags.auto_update_timestamp) fields++;
	if (column.flags.identity_generated_always) fields++;
	pack_map_length(packer, fields);
	packer << string("name");
	packer << column.name;
	packer << string("column_type");
	packer << ColumnTypeNames.at(column.column_type);
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
	if (!column.subtype.empty()) {
		packer << string("subtype");
		packer << column.subtype;
	}
	if (!column.reference_system.empty()) {
		packer << string("reference_system");
		packer << column.reference_system;
	}
	if (!column.enumeration_values.empty()) {
		packer << string("enumeration_values");
		packer << column.enumeration_values;
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
			packer << string("default_expression");
			packer << column.default_value;
			break;

		case DefaultType::generated_always_virtual:
			packer << string("generated_always_virtual");
			packer << column.default_value;
			break;

		case DefaultType::generated_always_stored:
			packer << string("generated_always_stored");
			packer << column.default_value;
			break;
	}
	if (column.flags.auto_update_timestamp) {
		packer << string("auto_update_timestamp");
		packer << true;
	}
	if (column.flags.identity_generated_always) {
		packer << string("identity_generated_always");
		packer << true;
	}
}

template <typename VersionedFDWriteStream>
void operator << (Packer<VersionedFDWriteStream> &packer, const Key &key) {
	if (packer.stream().protocol_version <= LAST_LEGACY_SCHEMA_FORMAT_VERSION) {
		legacy_serialize(packer, key);
		return;
	}

	pack_map_length(packer, key.standard() ? 2 : 3);
	packer << string("name");
	packer << key.name;
	switch (key.key_type) {
		case KeyType::standard_key:
			break;

		case KeyType::unique_key:
			packer << string("key_type");
			packer << string("unique");
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
	pack_map_length(packer, database.errors.empty() ? 1 : 2);
	packer << string("tables");
	packer << database.tables;
	if (!database.errors.empty()) {
		packer << string("errors");
		packer << database.errors;
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Column &column) {
	if (unpacker.stream().protocol_version <= LAST_LEGACY_SCHEMA_FORMAT_VERSION) {
		legacy_deserialize(unpacker, column);
		return;
	}

	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			unpacker >> column.name;
		} else if (attr_key == "column_type") {
			column.column_type = ColumnTypesByName.at(unpacker.template next<string>()); // non-present entries shouldn't get hit since we negotiate supported types
		} else if (attr_key == "size") {
			unpacker >> column.size;
		} else if (attr_key == "scale") {
			unpacker >> column.scale;
		} else if (attr_key == "nullable") {
			unpacker >> column.nullable;
		} else if (attr_key == "subtype") {
			unpacker >> column.subtype;
		} else if (attr_key == "reference_system") {
			unpacker >> column.reference_system;
		} else if (attr_key == "enumeration_values") {
			unpacker >> column.enumeration_values;
		} else if (attr_key == "sequence") {
			column.default_type = DefaultType::sequence;
			unpacker >> column.default_value; // currently unused, but allowed for forward compatibility
		} else if (attr_key == "default_value") {
			column.default_type = DefaultType::default_value;
			unpacker >> column.default_value;
		} else if (attr_key == "default_function") { // legacy name for protocol version 7 and earlier
			column.default_type = DefaultType::default_expression;
			unpacker >> column.default_value;
		} else if (attr_key == "default_expression") {
			column.default_type = DefaultType::default_expression;
			unpacker >> column.default_value;
		} else if (attr_key == "generated_always_virtual") {
			column.default_type = DefaultType::generated_always_virtual;
			unpacker >> column.default_value;
		} else if (attr_key == "generated_always_stored") {
			column.default_type = DefaultType::generated_always_stored;
			unpacker >> column.default_value;
		} else if (attr_key == "auto_update_timestamp") {
			unpacker >> column.flags.auto_update_timestamp;
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
		} else if (attr_key == "errors") {
			unpacker >> database.errors;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

#endif
