#include "subdivision.h"
#include "message_pack/copy_packed.h"

bool primary_key_subdividable(const Table &table) {
	if (table.primary_key_columns.size() != 1) return false;
	const Column &column(table.columns[table.primary_key_columns[0]]);
	return (column.column_type == ColumnTypes::SINT || column.column_type == ColumnTypes::UINT);
}

template <typename T>
inline T read_single_value(const ColumnValues &values) {
	PackedValueReadStream stream(values[0]);
	Unpacker<PackedValueReadStream> unpacker(stream);
	return unpacker.template next<T>();
}

template <typename IntegerType>
inline ColumnValues integer_midpoint(const ColumnValues &prev_key, const ColumnValues &last_key) {
	ColumnValues result(1);
	Packer<PackedValue> packer(result[0]);

	IntegerType prev = read_single_value<IntegerType>(prev_key);
	IntegerType last = read_single_value<IntegerType>(last_key);
	IntegerType midpoint = last > prev ? prev + (last - prev)/2 : prev;
	packer << midpoint;

	return result;
}

ColumnValues subdivide_primary_key_range(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	const Column &column(table.columns[table.primary_key_columns[0]]);

	if (column.column_type == ColumnTypes::SINT) {
		return integer_midpoint<int64_t>(prev_key, last_key);
	} else if (column.column_type == ColumnTypes::UINT) {
		return integer_midpoint<uint64_t>(prev_key, last_key);
	} else {
		throw runtime_error("can't subdivide columns of type " + column.column_type);
	}
}
