#ifndef PACKED_ROW_H
#define PACKED_ROW_H

#include <vector>
#include "packed_tuple.h"
#include "pack.h"

/*
Represents multiple (scalar) values encoded in MessagePack format, with indexing to provide random access.
*/
class PackedRow {
public:
	inline size_t size() const { return offsets.size(); }
	inline bool empty() const { return offsets.empty(); }

	inline size_t encoded_size() const { return values.encoded_size(); }
	inline void clear() { offsets.clear(); values.clear(); }
	inline void starting_new_object() { offsets.push_back(encoded_size()); }

	inline bool operator ==(const PackedRow &other) const { return values == other.values; }
	inline bool operator !=(const PackedRow &other) const { return values != other.values; }

	inline bool is_nil(size_t index) const { return (values.data()[offsets.at(index)] == MSGPACK_NIL); }

	vector<size_t> offsets;
	PackedTuple values;
};

template <typename T>
inline PackedRow &operator <<(PackedRow &row, const T &obj) {
	Packer<PackedTuple> packer(row.values);
	row.starting_new_object();
	packer << obj;
	return row;
};

static void pack_array_length(PackedRow &row, size_t size) {
	Packer<PackedTuple> packer(row.values);
	row.offsets.reserve(size);
	pack_array_length(packer, size);
}

#endif