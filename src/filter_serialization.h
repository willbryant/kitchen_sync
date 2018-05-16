#ifndef FILTER_SERIALIZATION_H
#define FILTER_SERIALIZATION_H

#include "filters.h"
#include "message_pack/unpack.h"

template <typename OutputStream>
void operator << (Packer<OutputStream> &packer, const TableFilter &table_filter) {
	int fields = 0;
	if (!table_filter.where_conditions.empty()) fields++;
	if (!table_filter.filter_expressions.empty()) fields++;

	pack_map_length(packer, fields);

	if (!table_filter.where_conditions.empty()) {
		packer << string("where_conditions");
		packer << table_filter.where_conditions;
	}
	if (!table_filter.filter_expressions.empty()) {
		packer << string("filter_expressions");
		packer << table_filter.filter_expressions;
	}
}

template <typename InputStream>
void operator >> (Unpacker<InputStream> &unpacker, TableFilter &table_filter) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		if (attr_key == "where_conditions") {
			unpacker >> table_filter.where_conditions;
		} else if (attr_key == "filter_expressions") {
			unpacker >> table_filter.filter_expressions;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

#endif