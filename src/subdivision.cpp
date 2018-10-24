#include "subdivision.h"
#include "message_pack/copy_packed.h"
#include <boost/multiprecision/cpp_int.hpp>

using namespace boost::multiprecision;

bool primary_key_subdividable(const Table &table) {
	if (table.primary_key_columns.size() != 1) return false;
	const Column &column(table.columns[table.primary_key_columns[0]]);
	return (column.column_type == ColumnTypes::SINT || column.column_type == ColumnTypes::UINT || column.column_type == ColumnTypes::UUID);
}

template <typename T>
inline T read_single_value(const ColumnValues &values) {
	PackedValueReadStream stream(values[0]);
	Unpacker<PackedValueReadStream> unpacker(stream);
	return unpacker.template next<T>();
}

template <typename T>
inline ColumnValues pack_single_value(const T &value) {
	ColumnValues result(1);
	Packer<PackedValue> packer(result[0]);
	packer << value;
	return result;
}

template <typename IntegerType>
inline ColumnValues subdivide_integer_range(const ColumnValues &prev_key, const ColumnValues &last_key) {
	IntegerType prev = read_single_value<IntegerType>(prev_key);
	IntegerType last = read_single_value<IntegerType>(last_key);
	IntegerType midpoint = last > prev ? prev + (last - prev)/2 : prev; // remember that overflow is undefined for signed integers in C & C++!
	return pack_single_value(midpoint);
}

inline bool parse_uuid(const string &str, uint128_t &out) {
	if (str.length() != 36 || str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-') {
		return false;
	}

	string stripped(str.substr(0, 8) + str.substr(9, 4) + str.substr(14, 4) + str.substr(19, 4) + str.substr(24, 12));

	std::istringstream converter(stripped);
	converter >> std::hex >> out;

	return (!converter.fail());
}

inline string format_uuid(uint128_t u) {
	char result[37];

	snprintf(
		result,
		sizeof(result),
		"%08jx-%04jx-%04jx-%04jx-%012jx",
		(uintmax_t)((u >> 96) & 0xffffffff),
		(uintmax_t)((u >> 80) & 0xffff),
		(uintmax_t)((u >> 64) & 0xffff),
		(uintmax_t)((u >> 48) & 0xffff),
		(uintmax_t)(u & 0xffffffffffff));

	return result;
}

inline ColumnValues subdivide_uuid_range(const ColumnValues &prev_key, const ColumnValues &last_key) {
	string prev = read_single_value<string>(prev_key);
	string last = read_single_value<string>(last_key);

	uint128_t uprev = 0;
	uint128_t ulast = 0;

	if (!parse_uuid(prev, uprev) || !parse_uuid(last, ulast)) {
		// shouldn't be possible with proper UUID types, but fail gracefully
		return prev_key;
	} else {
		uint128_t umid(uprev + (ulast - uprev)/2); // (uprev + ulast)/2 would overflow
		return pack_single_value(format_uuid(umid));
	}
}

ColumnValues subdivide_primary_key_range(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	const Column &column(table.columns[table.primary_key_columns[0]]);

	if (column.column_type == ColumnTypes::SINT) {
		return subdivide_integer_range<int64_t>(prev_key, last_key);
	} else if (column.column_type == ColumnTypes::UINT) {
		return subdivide_integer_range<uint64_t>(prev_key, last_key);
	} else if (column.column_type == ColumnTypes::UUID) {
		return subdivide_uuid_range(prev_key, last_key);
	} else {
		// don't know how to subdivide this key type
		return prev_key;
	}
}
