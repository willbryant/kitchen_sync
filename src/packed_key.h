#ifndef PACKED_KEY_H
#define PACKED_KEY_H

#include "message_pack/unpack.h"

class ColumnValues: public PackedBuffer {
public:
	inline size_t encoded_size() const { return (used ? used : 1); }
	inline uint8_t leader()      const { return (used ? *buffer() : MSGPACK_FIXARRAY_MIN); }
	inline const uint8_t *data() const { return (used ? buffer() : &MSGPACK_FIXARRAY_MIN); }

	inline bool empty() const {
		return (leader() == MSGPACK_FIXARRAY_MIN);
	}

	inline bool operator ==(const ColumnValues &other) const {
		return (encoded_size() == other.encoded_size() && memcmp(data(), other.data(), encoded_size()) == 0);
	}

	inline bool operator !=(const ColumnValues &other) const {
		return (encoded_size() != other.encoded_size() || memcmp(data(), other.data(), encoded_size()) != 0);
	}

	inline bool operator <(const ColumnValues &other) const {
		if (encoded_size() != other.encoded_size()) return (encoded_size() < other.encoded_size());
		return (memcmp(data(), other.data(), encoded_size()) < 0);
	}
};

template <typename Stream>
Unpacker<Stream> &operator >>(Unpacker<Stream> &unpacker, ColumnValues &obj) {
	obj.clear();
	copy_object(unpacker, obj);
	return unpacker;
}

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const ColumnValues &obj) {
	packer.write_bytes(obj.data(), obj.encoded_size());
	return packer;
}

#endif
