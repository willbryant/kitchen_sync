#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "message_pack/unpack.h"

void operator << (Packer<ostream> &packer, const Column &column) {
	packer.pack_map_length(1);
	packer << string("name");
	packer << column.name;
}

void operator << (Packer<ostream> &packer, const Table &table) {
	packer.pack_map_length(3);
	packer << string("name");
	packer << table.name;
	packer << string("columns");
	packer << table.columns;
	packer << string("primary_key_columns");
	packer << table.primary_key_columns;
}

void operator << (Packer<ostream> &packer, const Database &database) {
	packer.pack_map_length(1);
	packer << string("tables");
	packer << database.tables;
}

template <typename Stream>
void operator >> (Unpacker<Stream> &unpacker, Column &column) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			column.name = unpacker.template next<string>();
		} // ignore anything else, for forward compatibility
	}
}

template <typename Stream>
void operator >> (Unpacker<Stream> &unpacker, Table &table) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "name") {
			table.name = unpacker.template next<string>();
		} else if (attr_key == "columns") {
			unpacker >> table.columns;
		} else if (attr_key == "primary_key_columns") {
			unpacker >> table.primary_key_columns;
		} // ignore anything else, for forward compatibility
	}
}

template <typename Stream>
void operator >> (Unpacker<Stream> &unpacker, Database &database) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "tables") {
			unpacker >> database.tables;
		} // ignore anything else, for forward compatibility
	}
}

#endif
