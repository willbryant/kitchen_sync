#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "message_pack/unpack.h"

template <typename T>
void operator << (msgpack::packer<ostream> &packer, const vector<T> &v) {
	packer.pack_array(v.size());
	for (typename vector<T>::const_iterator it = v.begin(); it != v.end(); it++) packer << *it;
}

void operator << (msgpack::packer<ostream> &packer, const Column &column) {
	packer.pack_map(1);
	packer << string("name");
	packer << column.name;
}

void operator << (msgpack::packer<ostream> &packer, const Table &table) {
	packer.pack_map(3);
	packer << string("name");
	packer << table.name;
	packer << string("columns");
	packer << table.columns;
	packer << string("primary_key_columns");
	packer << table.primary_key_columns;
}

void operator << (msgpack::packer<ostream> &packer, const Database &database) {
	packer.pack_map(1);
	packer << string("tables");
	packer << database.tables;
}

void operator >> (Unpacker &unpacker, Column &column) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.next<string>();

		if (attr_key == "name") {
			column.name = unpacker.next<string>();
		} // ignore anything else, for forward compatibility
	}
}

void operator >> (Unpacker &unpacker, Table &table) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.next<string>();

		if (attr_key == "name") {
			table.name = unpacker.next<string>();
		} else if (attr_key == "columns") {
			unpacker >> table.columns;
		} else if (attr_key == "primary_key_columns") {
			unpacker >> table.primary_key_columns;
		} // ignore anything else, for forward compatibility
	}
}

void operator >> (Unpacker &unpacker, Database &database) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.next<string>();

		if (attr_key == "tables") {
			unpacker >> database.tables;
		} // ignore anything else, for forward compatibility
	}
}

#endif
