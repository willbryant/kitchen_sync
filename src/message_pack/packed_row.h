#ifndef PACKED_ROW_H
#define PACKED_ROW_H

#include <vector>
#include "packed_value.h"

typedef vector<PackedValue> PackedRow;

template <typename T>
inline PackedRow &operator <<(PackedRow &row, const T &obj) {
	row.resize(row.size() + 1);
	row.back() << obj;
	return row;
}

// overload, we don't actually need to pack anything for a vector, but reserve for efficiency
static void pack_array_length(PackedRow &row, size_t size) {
	row.reserve(size);
}

#endif