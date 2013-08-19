#ifndef UNPACKER_H
#define UNPACKER_H

#include "unistd.h"
#include "stdint.h"
#include <cerrno>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include "to_string.h"
#include "endian.h"
#include "backtrace.h"

struct unpacker_error: public std::runtime_error {
	unpacker_error(const std::string &error): runtime_error(error) {}
};

#define MSGPACK_POSITIVE_FIXNUM_MIN 0x00
#define MSGPACK_POSITIVE_FIXNUM_MAX 0x7f
#define MSGPACK_NEGATIVE_FIXNUM_MIN 0xe0
#define MSGPACK_NEGATIVE_FIXNUM_MAX 0xff
#define MSGPACK_FIXRAW_MIN          0xa0
#define MSGPACK_FIXRAW_MAX          0xbf
#define MSGPACK_FIXMAP_MIN          0x80
#define MSGPACK_FIXMAP_MAX          0x8f
#define MSGPACK_FIXARRAY_MIN        0x90
#define MSGPACK_FIXARRAY_MAX        0x9f
#define MSGPACK_NIL                 0xc0
#define MSGPACK_FALSE               0xc2
#define MSGPACK_TRUE                0xc3
#define MSGPACK_FLOAT               0xca
#define MSGPACK_DOUBLE              0xcb
#define MSGPACK_UINT8               0xcc
#define MSGPACK_UINT16              0xcd
#define MSGPACK_UINT32              0xce
#define MSGPACK_UINT64              0xcf
#define MSGPACK_INT8                0xd0
#define MSGPACK_INT16               0xd1
#define MSGPACK_INT32               0xd2
#define MSGPACK_INT64               0xd3
#define MSGPACK_RAW16               0xda
#define MSGPACK_RAW32               0xdb
#define MSGPACK_ARRAY16             0xdc
#define MSGPACK_ARRAY32             0xdd
#define MSGPACK_MAP16               0xde
#define MSGPACK_MAP32               0xdf

class Unpacker {
public:
	Unpacker(int fd): fd(fd), have_next_byte(false), next_byte(0) {}

	// determines if the next value is nil, but doesn't read it - call \next_nil() to do that.
	bool next_is_nil() {
		return (peek() == MSGPACK_NIL);
	}

	// reads the next value of the selected type from the data stream, detecting the encoding format and converting
	// to the type, applying byte order conversion if necessary.
	template <typename T>
	T next() {
		T value;
		*this >> value;
		return value;
	}

	// reads and discards the nil that is next in the data stream - raising an exception if that is not the case.
	// necessary after a next_is_nil() call to get past the nil.
	void next_nil() {
		uint8_t leader = read_raw<uint8_t>();
		if (leader != MSGPACK_NIL) {
			backtrace();
			throw unpacker_error("Don't know how to convert MessagePack type " + to_string((int)leader) + " to nil");
		}
	}

	size_t next_array_length() {
		uint8_t leader = read_raw<uint8_t>();

		if (leader >= MSGPACK_FIXARRAY_MIN && leader <= MSGPACK_FIXARRAY_MAX) {
			return (leader & 15);
		}

		switch (leader) {
			case MSGPACK_ARRAY16:
				return ntohs(read_raw<uint16_t>());

			case MSGPACK_ARRAY32:
				return ntohl(read_raw<uint32_t>());

			default:
				backtrace();
				throw unpacker_error("Don't know how to convert MessagePack type " + to_string((int)leader) + " to array");
		}
	}

	size_t next_map_length() {
		uint8_t leader = read_raw<uint8_t>();

		if (leader >= MSGPACK_FIXMAP_MIN && leader <= MSGPACK_FIXMAP_MAX) {
			return (leader & 15);
		}

		switch (leader) {
			case MSGPACK_MAP16:
				return ntohs(read_raw<uint16_t>());

			case MSGPACK_MAP32:
				return ntohl(read_raw<uint32_t>());

			default:
				backtrace();
				throw unpacker_error("Don't know how to convert MessagePack type " + to_string((int)leader) + " to map");
		}
	}


	// reads the selected type as raw bytes from the data stream, without byte order conversion or type unmarshalling
	template <typename T>
	T read_raw() {
		T obj;
		read_raw_bytes((unsigned char*) &obj, sizeof(obj));
		return obj;
	}

	// gets but does not consume the next raw byte from the data stream
	unsigned char peek() {
		if (!have_next_byte) {
			read_raw_bytes(&next_byte, 1);
			have_next_byte = true;
		}
		return next_byte;
	}

	// reads the given number of raw bytes from the data stream, without byte order conversion or type unmarshalling
	void read_raw_bytes(unsigned char *buf, size_t bytes) {
		ssize_t bytes_read;

		if (have_next_byte && bytes > 0) {
			*buf = next_byte;
			buf++;
			bytes -= 1;
			have_next_byte = false;
		}

		while (bytes > 0) {
			bytes_read = ::read(fd, buf, bytes);

			if (bytes_read < 0) {
				if (errno == EINTR) continue;
				throw unpacker_error("Read from stream failed: " + std::string(strerror(errno)));
			}

			buf   += bytes_read;
			bytes -= bytes_read;
		}
	}

protected:
	int fd;
	bool have_next_byte;
	unsigned char next_byte;
};

template <typename T>
T &operator >>(Unpacker &unpacker, T &obj) {
	uint8_t leader = unpacker.read_raw<uint8_t>();

	if (leader >= MSGPACK_POSITIVE_FIXNUM_MIN && leader <= MSGPACK_POSITIVE_FIXNUM_MAX) {
		obj = (T) leader;

	} else if (leader >= MSGPACK_NEGATIVE_FIXNUM_MIN && leader <= MSGPACK_NEGATIVE_FIXNUM_MAX) {
		obj = (T) (int8_t)leader;

	} else {
		switch (leader) {
			case MSGPACK_FALSE:
				obj = (T) false;
				break;

			case MSGPACK_TRUE:
				obj = (T) true;
				break;

			case MSGPACK_FLOAT:
				obj = (T) unpacker.read_raw<float>();
				break;

			case MSGPACK_DOUBLE:
				obj = (T) unpacker.read_raw<double>();
				break;

			case MSGPACK_UINT8:
				obj = (T) unpacker.read_raw<uint8_t>();
				break;

			case MSGPACK_UINT16:
				obj = (T) ntohs(unpacker.read_raw<uint16_t>());
				break;

			case MSGPACK_UINT32:
				obj = (T) ntohl(unpacker.read_raw<uint32_t>());
				break;

			case MSGPACK_UINT64:
				obj = (T) ntohll(unpacker.read_raw<uint64_t>());
				break;

			case MSGPACK_INT8:
				obj = (T) unpacker.read_raw<int8_t>();
				break;

			case MSGPACK_INT16:
				obj = (T) ntohs(unpacker.read_raw<int16_t>());
				break;

			case MSGPACK_INT32:
				obj = (T) ntohl(unpacker.read_raw<int32_t>());
				break;

			case MSGPACK_INT64:
				obj = (T) ntohll(unpacker.read_raw<int64_t>());
				break;

			default:
				backtrace();
				throw unpacker_error("Don't know how to convert MessagePack type " + to_string((int)leader) + " to type " + typeid(T).name());
		}
	}
	return obj;
}

template <>
std::string &operator >>(Unpacker &unpacker, std::string &obj) {
	uint8_t leader = unpacker.read_raw<uint8_t>();

	if (leader >= MSGPACK_FIXRAW_MIN && leader <= MSGPACK_FIXRAW_MAX) {
		obj.resize(leader & 31);
	} else {
		switch(leader) {
			case MSGPACK_RAW16:
				obj.resize(ntohs(unpacker.read_raw<uint16_t>()));
				break;

			case MSGPACK_RAW32:
				obj.resize(ntohl(unpacker.read_raw<uint32_t>()));
				break;

			default:
				backtrace();
				throw unpacker_error("Don't know how to convert MessagePack type " + to_string((int)leader) + " to string");
		}
	}

	unpacker.read_raw_bytes((unsigned char *)obj.data(), obj.size());
	return obj;
}

template <typename T>
std::vector<T> &operator >>(Unpacker &unpacker, std::vector<T> &obj) {
	size_t array_length = unpacker.next_array_length();
	obj.clear();
	while (array_length--) {
		obj.push_back(unpacker.next<T>());
	}
}

template <typename K, typename V>
std::map<K, V> &operator >>(Unpacker &unpacker, std::map<K, V> &obj) {
	size_t map_length = unpacker.next_map_length();
	obj.clear();
	while (map_length--) {
		K key = unpacker.next<K>();
		V val = unpacker.next<V>();
		obj[key] = val;
	}
}

#endif
