#ifndef PACK_H
#define PACK_H

#include "unistd.h"
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include "endian.h"
#include "type_codes.h"
#include "../to_string.h"
#include "../backtrace.h"

using namespace std;

struct packer_error: public std::runtime_error {
	packer_error(const std::string &error): runtime_error(error) {}
};

template <typename Stream>
class Packer {
public:
	inline Packer(Stream &stream): stream(stream) {}

	// writes the given value as bytes to the data stream, without byte order conversion or type marshalling
	template <typename T>
	inline void write_bytes(const T &obj) {
		write_bytes((const uint8_t*)&obj, sizeof(obj));
	}

	// writes the given number of bytes to the data stream, without byte order conversion or type unmarshalling
	inline void write_bytes(const uint8_t *buf, size_t bytes) {
		stream.write(buf, bytes);
	}

	inline void flush() {
		stream.flush();
	}

protected:
	Stream &stream;
};

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const nullptr_t &obj) {
	packer.write_bytes(MSGPACK_NIL);
	return packer;
}

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const long long &obj) {
	if (obj > 0xffffffff) {
		packer.write_bytes(MSGPACK_UINT64);
		packer.write_bytes((uint64_t) htonll(obj));

	} else if (obj > 0xffff) {
		packer.write_bytes(MSGPACK_UINT32);
		packer.write_bytes((uint32_t) htonl(obj));

	} else if (obj > 0xff) {
		packer.write_bytes(MSGPACK_UINT16);
		packer.write_bytes((uint16_t) htons(obj));

	} else if (obj > MSGPACK_POSITIVE_FIXNUM_MAX) {
		packer.write_bytes(MSGPACK_UINT8);
		packer.write_bytes((uint8_t) obj);

	} else if (obj >= (int8_t) MSGPACK_NEGATIVE_FIXNUM_MIN) {
		packer.write_bytes((int8_t) obj);

	} else if (obj >= (int8_t) 0xff) {
		packer.write_bytes(MSGPACK_INT8);
		packer.write_bytes((int8_t) obj);

	} else if (obj >= (int16_t) 0xffff) {
		packer.write_bytes(MSGPACK_INT16);
		packer.write_bytes((int16_t) htons(obj));

	} else if (obj >= (int32_t) 0xffffffff) {
		packer.write_bytes(MSGPACK_INT32);
		packer.write_bytes((int32_t) htonl(obj));

	} else {
		packer.write_bytes(MSGPACK_INT64);
		packer.write_bytes((int64_t) htonll(obj));
	}
	return packer;
}

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const unsigned long long &obj) {
	if (obj > 0xffffffff) {
		packer.write_bytes(MSGPACK_UINT64);
		packer.write_bytes((uint64_t) htonll(obj));

	} else if (obj > 0xffff) {
		packer.write_bytes(MSGPACK_UINT32);
		packer.write_bytes((uint32_t) htonl(obj));

	} else if (obj > 0xff) {
		packer.write_bytes(MSGPACK_UINT16);
		packer.write_bytes((uint16_t) htons(obj));

	} else if (obj > MSGPACK_POSITIVE_FIXNUM_MAX) {
		packer.write_bytes(MSGPACK_UINT8);
		packer.write_bytes((uint8_t) obj);

	} else {
		packer.write_bytes((uint8_t) obj);
	}
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const long &obj) {
	packer << (long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const int &obj) {
	packer << (long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const short &obj) {
	packer << (long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const unsigned long &obj) {
	packer << (unsigned long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const unsigned int &obj) {
	packer << (unsigned long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const unsigned short &obj) {
	packer << (unsigned long long) obj;
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const bool &obj) {
	packer.write_bytes(obj ? MSGPACK_TRUE : MSGPACK_FALSE);
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const float &obj) {
	if (sizeof(float) != sizeof(uint32_t)) throw runtime_error("Can't convert float to/from network byte order on this platform");
	uint32_t copy;
	memcpy(&copy, &obj, sizeof(copy));
	packer.write_bytes(MSGPACK_FLOAT);
	packer.write_bytes((uint32_t) htonl(copy));
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const double &obj) {
	if (sizeof(double) != sizeof(uint64_t)) throw runtime_error("Can't convert double to/from network byte order on this platform");
	uint64_t copy;
	memcpy(&copy, &obj, sizeof(copy));
	packer.write_bytes(MSGPACK_DOUBLE);
	packer.write_bytes((uint64_t) htonll(copy));
	return packer;
}

template <typename Stream>
void pack_raw_length(Packer<Stream> &packer, size_t size) {
	if (size <= MSGPACK_FIXRAW_MAX - MSGPACK_FIXRAW_MIN) {
		packer.write_bytes((uint8_t) (MSGPACK_FIXRAW_MIN + size));

	/* we could/should use MSGPACK_RAW8 here when size <= 0xff, but that's a breaking change for hash calculation;
	   the STR8 and RAW8 types didn't exist when we implemented this originally, so anyone wanting to reproduce our
	   hashes should set 'compatibility mode' in their msgpack packer to get the same behavior we have. */

	} else if (size <= 0xffff) {
		packer.write_bytes(MSGPACK_RAW16);
		packer.write_bytes((uint16_t) htons(size));

	} else if (size <= 0xffffffff) {
		packer.write_bytes(MSGPACK_RAW32);
		packer.write_bytes((uint32_t) htonl(size));

	} else {
		throw runtime_error("String too large to serialize");
	}
}

template <typename Stream>
inline void pack_raw(Packer<Stream> &packer, const uint8_t *buf, size_t bytes) {
	pack_raw_length(packer, bytes);
	packer.write_bytes(buf, bytes);
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const std::string &obj) {
	pack_raw(packer, (const uint8_t *)obj.data(), obj.size());
	return packer;
}

// tiny wrapper around a pointer with length so we can avoid copying bytes into
// std::string objects when we only need them to call << to get pack_raw anyway.
struct uncopied_byte_string {
	inline uncopied_byte_string(const void *buf, size_t size): buf(buf), size(size) {}

	const void *buf;
	size_t size;

private:
	// forbid copying to reduce bugs - you shouldn't use this class like this.
	// could be tolerated if required in future, as long as you can guarantee
	// the backing pointers are still valid.
	uncopied_byte_string(const uncopied_byte_string& copy_from) { throw logic_error("copying forbidden"); }
};

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const uncopied_byte_string &obj) {
	pack_raw(packer, (const uint8_t *)obj.buf, obj.size);
	return packer;
}

template <typename Stream>
void pack_array_length(Packer<Stream> &packer, size_t size) {
	if (size <= MSGPACK_FIXARRAY_MAX - MSGPACK_FIXARRAY_MIN) {
		packer.write_bytes((uint8_t) (MSGPACK_FIXARRAY_MIN + size));

	} else if (size <= 0xffff) {
		packer.write_bytes(MSGPACK_ARRAY16);
		packer.write_bytes((uint16_t) htons(size));

	} else if (size <= 0xffffffff) {
		packer.write_bytes(MSGPACK_ARRAY32);
		packer.write_bytes((uint32_t) htonl(size));

	} else {
		throw runtime_error("Array too large to serialize");
	}
}

template <typename Stream, typename T>
Packer<Stream> &operator <<(Packer<Stream> &packer, const std::vector<T> &obj) {
	pack_array_length(packer, obj.size());
	for (const T &t : obj) {
		packer << t;
	}
	return packer;
}

template <typename Stream>
void pack_map_length(Packer<Stream> &packer, size_t size) {
	if (size <= MSGPACK_FIXMAP_MAX - MSGPACK_FIXMAP_MIN) {
		packer.write_bytes((uint8_t) (MSGPACK_FIXMAP_MIN + size));

	} else if (size <= 0xffff) {
		packer.write_bytes(MSGPACK_MAP16);
		packer.write_bytes((uint16_t) htons(size));

	} else if (size <= 0xffffffff) {
		packer.write_bytes(MSGPACK_MAP32);
		packer.write_bytes((uint32_t) htonl(size));

	} else {
		throw runtime_error("Map too large to serialize");
	}
}

template <typename Stream, typename K, typename V>
Packer<Stream> &operator <<(Packer<Stream> &packer, const std::map<K, V> &obj) {
	pack_map_length(packer, obj.size());
	for (const std::pair<const K, V> &pair : obj) {
		packer << pair.first;
		packer << pair.second;
	}
	return packer;
}

#endif
