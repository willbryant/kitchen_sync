#ifndef COPY_PACKED_H
#define COPY_PACKED_H

#include "packed_value.h"
#include "pack.h"
#include "unpack.h"

template <typename Stream>
uint8_t *copy_bytes(Unpacker<Stream> &unpacker, PackedValue &obj, size_t bytes) {
	uint8_t *start_of_data = obj.extend(bytes);
	unpacker.read_bytes(start_of_data, bytes);
	return start_of_data;
}

template <typename Stream>
uint8_t copy_and_read_uint8_t(Unpacker<Stream> &unpacker, PackedValue &obj) {
	uint8_t *p = copy_bytes(unpacker, obj, sizeof(uint8_t));
	return *p;
}

template <typename Stream>
uint16_t copy_and_read_uint16_t(Unpacker<Stream> &unpacker, PackedValue &obj) {
	uint8_t *p = copy_bytes(unpacker, obj, sizeof(uint16_t));
	return (*p << 8) + (*(p + 1)); // we can't cast to uint16_t* and use ntohs because the pointer may not be aligned, which would invoke undefined behavior
}

template <typename Stream>
uint32_t copy_and_read_uint32_t(Unpacker<Stream> &unpacker, PackedValue &obj) {
	uint8_t *p = copy_bytes(unpacker, obj, sizeof(uint32_t));
	return (*p << 24) + (*(p + 1) << 16) + (*(p + 2) << 8) + (*(p + 3)); // we can't cast to uint32_t* and use ntohl because the pointer may not be aligned, which would invoke undefined behavior
}

template <typename Stream>
void copy_object(Unpacker<Stream> &unpacker, PackedValue &obj) {
	uint8_t leader = *copy_bytes(unpacker, obj, 1);

	if ((leader == MSGPACK_NIL || leader == MSGPACK_FALSE || leader == MSGPACK_TRUE) ||
		(leader >= MSGPACK_POSITIVE_FIXNUM_MIN && leader <= MSGPACK_POSITIVE_FIXNUM_MAX) ||
		(leader >= MSGPACK_NEGATIVE_FIXNUM_MIN && leader <= MSGPACK_NEGATIVE_FIXNUM_MAX)) {
		// no payload

	} else if (leader >= MSGPACK_FIXRAW_MIN && leader <= MSGPACK_FIXRAW_MAX) {
		copy_bytes(unpacker, obj, leader & 31);

	} else if (leader >= MSGPACK_FIXARRAY_MIN && leader <= MSGPACK_FIXARRAY_MAX) {
		copy_array_members(unpacker, obj, leader & 15);

	} else if (leader >= MSGPACK_FIXMAP_MIN && leader <= MSGPACK_FIXMAP_MAX) {
		copy_map_members(unpacker, obj, leader & 15);

	} else {
		switch (leader) {
			case MSGPACK_FLOAT:
				copy_bytes(unpacker, obj, sizeof(float));
				break;

			case MSGPACK_DOUBLE:
				copy_bytes(unpacker, obj, sizeof(double));
				break;

			case MSGPACK_UINT8:
				copy_bytes(unpacker, obj, sizeof(uint8_t));
				break;

			case MSGPACK_UINT16:
				copy_bytes(unpacker, obj, sizeof(uint16_t));
				break;

			case MSGPACK_UINT32:
				copy_bytes(unpacker, obj, sizeof(uint32_t));
				break;

			case MSGPACK_UINT64:
				copy_bytes(unpacker, obj, sizeof(uint64_t));
				break;

			case MSGPACK_INT8:
				copy_bytes(unpacker, obj, sizeof(int8_t));
				break;

			case MSGPACK_INT16:
				copy_bytes(unpacker, obj, sizeof(int16_t));
				break;

			case MSGPACK_INT32:
				copy_bytes(unpacker, obj, sizeof(int32_t));
				break;

			case MSGPACK_INT64:
				copy_bytes(unpacker, obj, sizeof(int64_t));
				break;

			case MSGPACK_BIN8:
			case MSGPACK_RAW8:
				copy_bytes(unpacker, obj, copy_and_read_uint8_t(unpacker, obj));
				break;

			case MSGPACK_RAW16:
			case MSGPACK_BIN16:
				copy_bytes(unpacker, obj, copy_and_read_uint16_t(unpacker, obj));
				break;

			case MSGPACK_RAW32:
			case MSGPACK_BIN32:
				copy_bytes(unpacker, obj, copy_and_read_uint32_t(unpacker, obj));
				break;

			case MSGPACK_ARRAY16:
				copy_array_members(unpacker, obj, copy_and_read_uint16_t(unpacker, obj));
				break;

			case MSGPACK_ARRAY32:
				copy_array_members(unpacker, obj, copy_and_read_uint32_t(unpacker, obj));
				break;

			case MSGPACK_MAP16:
				copy_map_members(unpacker, obj, copy_and_read_uint16_t(unpacker, obj));
				break;

			case MSGPACK_MAP32:
				copy_map_members(unpacker, obj, copy_and_read_uint32_t(unpacker, obj));
				break;

			default:
				backtrace();
				throw unpacker_error("Don't know how to size MessagePack type " + to_string((int)leader));
		}
	}
}

template <typename Stream>
void copy_array_members(Unpacker<Stream> &unpacker, PackedValue &obj, size_t size) {
	while (size--) {
		copy_object(unpacker, obj);
	}
}

template <typename Stream>
void copy_map_members(Unpacker<Stream> &unpacker, PackedValue &obj, size_t size) {
	while (size--) {
		copy_object(unpacker, obj);
		copy_object(unpacker, obj);
	}
}

template <typename Stream>
Unpacker<Stream> &operator >>(Unpacker<Stream> &unpacker, PackedValue &obj) {
	obj.clear();
	copy_object(unpacker, obj);
	return unpacker;
}

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const PackedValue &obj) {
	packer.write_bytes(obj.data(), obj.size());
	return packer;
}

struct PackedValueReadStream {
	inline PackedValueReadStream(const PackedValue &value): data(value.data()), pos(0) {}

	inline void read(uint8_t *dest, size_t bytes) {
		memcpy(dest, data + pos, bytes);
		pos += bytes;
	}

	const uint8_t *data;
	size_t pos;
};

template <typename T>
inline PackedValue &operator <<(PackedValue &value, const T &obj) {
	Packer<PackedValue> packer(value);
	packer << obj;
	return value;
}

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
