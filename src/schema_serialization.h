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
	if (!column.db_type_def.empty()) fields++;
	if (column.default_type) fields++;
	if (column.flags & mysql_timestamp) fields++;
	if (column.flags & mysql_on_update_timestamp) fields++;
	if (column.flags & time_zone) fields++;
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

		case DefaultType::default_function:
			packer << string("default_function");
			packer << column.default_value;
			break;
	}
	if (column.flags & ColumnFlags::mysql_timestamp) {
		packer << string("mysql_timestamp");
		packer << true;
	}
	if (column.flags & ColumnFlags::mysql_on_update_timestamp) {
		packer << string("mysql_on_update_timestamp");
		packer << true;
	}
	if (column.flags & ColumnFlags::time_zone) {
		packer << string("time_zone");
		packer << true;
	}
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Key &key) {
	pack_map_length(packer, 3);
	packer << string("name");
	packer << key.name;
	packer << string("unique");
	packer << key.unique;
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
	packer << table.primary_key_type;
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
		} else if (attr_key == "db_type_def") {
			unpacker >> column.db_type_def;
		} else if (attr_key == "sequence") {
			column.default_type = DefaultType::sequence;
			unpacker >> column.default_value; // currently unused, but allowed for forward compatibility
		} else if (attr_key == "default_value") {
			column.default_type = DefaultType::default_value;
			unpacker >> column.default_value;
		} else if (attr_key == "default_function") {
			column.default_type = DefaultType::default_function;
			unpacker >> column.default_value;
		} else if (attr_key == "mysql_timestamp") {
			bool flag;
			unpacker >> flag;
			if (flag) column.flags = (ColumnFlags)(column.flags | mysql_timestamp);
		} else if (attr_key == "mysql_on_update_timestamp") {
			bool flag;
			unpacker >> flag;
			if (flag) column.flags = (ColumnFlags)(column.flags | mysql_on_update_timestamp);
		} else if (attr_key == "time_zone") {
			bool flag;
			unpacker >> flag;
			if (flag) column.flags = (ColumnFlags)(column.flags | time_zone);
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
			unpacker >> key.unique;
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

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			unpacker >> table.name;
		} else if (attr_key == "columns") {
			unpacker >> table.columns;
		} else if (attr_key == "primary_key_columns") {
			unpacker >> table.primary_key_columns;
		} else if (attr_key == "primary_key_type") {
			unpacker >> table.primary_key_type;
		} else if (attr_key == "keys") {
			unpacker >> table.keys;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
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
