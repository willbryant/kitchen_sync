#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "message_pack/unpack.h"

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Column &column) {
	packer.pack_map_length(1);
	packer << string("name");
	packer << column.name;
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Key &key) {
	packer.pack_map_length(3);
	packer << string("name");
	packer << key.name;
	packer << string("unique");
	packer << key.unique;
	packer << string("columns");
	packer << key.columns;
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Table &table) {
	packer.pack_map_length(4);
	packer << string("name");
	packer << table.name;
	packer << string("columns");
	packer << table.columns;
	packer << string("primary_key_columns");
	packer << table.primary_key_columns;
	packer << string("keys");
	packer << table.keys;
}

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const Database &database) {
	packer.pack_map_length(1);
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
		} // ignore anything else, for forward compatibility
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
		} // ignore anything else, for forward compatibility
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
		} else if (attr_key == "keys") {
			unpacker >> table.keys;
		} // ignore anything else, for forward compatibility
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, Database &database) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "tables") {
			unpacker >> database.tables;
		} // ignore anything else, for forward compatibility
	}
}

#endif
