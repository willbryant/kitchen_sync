#ifndef SCHEMA_SERIALIZATION_H
#define SCHEMA_SERIALIZATION_H

#include "schema.h"
#include "stream.h"

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
	packer.pack_map(2);
	packer << string("name");
	packer << table.name;
	packer << string("columns");
	packer << table.columns;
}

void operator << (msgpack::packer<ostream> &packer, const Database &database) {
	packer.pack_map(1);
	packer << string("tables");
	packer << database.tables;
}

void operator >> (msgpack::object obj, Column &column) {
	if (obj.type != msgpack::type::MAP) throw runtime_error("Expected a map while reading table");
	for (msgpack::object_kv *ptr = obj.via.map.ptr; ptr != obj.via.map.ptr + obj.via.map.size; ptr++) {
		string attr_key = ptr->key.as<string>();

		if (attr_key == "name") {
			column.name = ptr->val.as<string>();
		} // ignore anything else, for forward compatibility
	}
}

void operator >> (msgpack::object obj, Columns &columns) {
	if (obj.type != msgpack::type::ARRAY) throw runtime_error("Expected an array while reading columns");
	for (msgpack::object *ptr = obj.via.array.ptr; ptr != obj.via.array.ptr + obj.via.array.size; ptr++) {
		Column column;
		*ptr >> column;
		columns.push_back(column);
	}
}

void operator >> (msgpack::object obj, Table &table) {
	if (obj.type != msgpack::type::MAP) throw runtime_error("Expected a map while reading table");
	for (msgpack::object_kv *ptr = obj.via.map.ptr; ptr != obj.via.map.ptr + obj.via.map.size; ptr++) {
		string attr_key = ptr->key.as<string>();

		if (attr_key == "name") {
			table.name = ptr->val.as<string>();
		} else if (attr_key == "columns") {
			ptr->val >> table.columns;
		} // ignore anything else, for forward compatibility
	}
}

void operator >> (msgpack::object obj, Tables &tables) {
	if (obj.type != msgpack::type::ARRAY) throw runtime_error("Expected an array while reading tables");
	for (msgpack::object *ptr = obj.via.array.ptr; ptr != obj.via.array.ptr + obj.via.array.size; ptr++) {
		Table table;
		*ptr >> table;
		tables.push_back(table);
	}
}

void operator >> (msgpack::object obj, Database &database) {
	if (obj.type != msgpack::type::MAP) throw runtime_error("Expected a map while reading schema");
	for (msgpack::object_kv *ptr = obj.via.map.ptr; ptr != obj.via.map.ptr + obj.via.map.size; ptr++) {
		string attr_key = ptr->key.as<string>();

		if (attr_key == "tables") {
			ptr->val >> database.tables;
		} // ignore anything else, for forward compatibility
	}
}

#endif
