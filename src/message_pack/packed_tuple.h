#ifndef PACKED_TUPLE_H
#define PACKED_TUPLE_H

#include "packed_value.h"

/*
Represents multiple (scalar) values encoded as a MessagePack array, without random access.

When clear, represents itself as an encoded zero-length MessagePack array.
*/
class PackedTuple: public PackedBuffer {
public:
	inline size_t encoded_size() const { return (used ? used : 1); }
	inline uint8_t leader()      const { return (used ? *buffer() : MSGPACK_FIXARRAY_MIN); }
	inline const uint8_t *data() const { return (used ? buffer() : &MSGPACK_FIXARRAY_MIN); }

	inline bool empty() const {
		return (leader() == MSGPACK_FIXARRAY_MIN);
	}

	inline bool operator ==(const PackedTuple &other) const {
		return (encoded_size() == other.encoded_size() && memcmp(data(), other.data(), encoded_size()) == 0);
	}

	inline bool operator !=(const PackedTuple &other) const {
		return (encoded_size() != other.encoded_size() || memcmp(data(), other.data(), encoded_size()) != 0);
	}

	inline bool operator <(const PackedTuple &other) const {
		if (encoded_size() != other.encoded_size()) return (encoded_size() < other.encoded_size());
		return (memcmp(data(), other.data(), encoded_size()) < 0);
	}
};

#endif
