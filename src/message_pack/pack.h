#ifndef PACK_H
#define PACK_H

#include "unistd.h"
#include "stdint.h"
#include <cerrno>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include "endian.h"
#include "type_codes.h"
#include "../to_string.h"
#include "../backtrace.h"

struct packer_error: public std::runtime_error {
	packer_error(const std::string &error): runtime_error(error) {}
};

template <typename Stream>
class Packer {
public:
	Packer(Stream &stream): stream(stream) {}

	template <typename T>
	inline void pack(const T &value) {
		*this << value;
	}

	inline void pack_nil() {
		write_raw(MSGPACK_NIL);
	}

	inline void pack_raw(const uint8_t *buf, size_t bytes) {
		pack_raw_length(bytes);
		write_raw_bytes(buf, bytes);
	}

	void pack_raw_length(size_t size) {
		if (size <= MSGPACK_FIXRAW_MAX - MSGPACK_FIXRAW_MIN) {
			write_raw((uint8_t) (MSGPACK_FIXRAW_MIN + size));

		} else if (size <= 0xffff) {
			write_raw(MSGPACK_RAW16);
			write_raw((uint16_t) htons(size));

		} else if (size <= 0xffffffff) {
			write_raw(MSGPACK_RAW32);
			write_raw((uint32_t) htonl(size));

		} else {
			throw runtime_error("String too large to serialize");
		}
	}

	void pack_array_length(size_t size) {
		if (size <= MSGPACK_FIXARRAY_MAX - MSGPACK_FIXARRAY_MIN) {
			write_raw((uint8_t) (MSGPACK_FIXARRAY_MIN + size));

		} else if (size <= 0xffff) {
			write_raw(MSGPACK_ARRAY16);
			write_raw((uint16_t) htons(size));

		} else if (size <= 0xffffffff) {
			write_raw(MSGPACK_ARRAY32);
			write_raw((uint32_t) htonl(size));

		} else {
			throw runtime_error("Array too large to serialize");
		}
	}

	void pack_map_length(size_t size) {
		if (size <= MSGPACK_FIXMAP_MAX - MSGPACK_FIXMAP_MIN) {
			write_raw((uint8_t) (MSGPACK_FIXMAP_MIN + size));

		} else if (size <= 0xffff) {
			write_raw(MSGPACK_MAP16);
			write_raw((uint16_t) htons(size));

		} else if (size <= 0xffffffff) {
			write_raw(MSGPACK_MAP32);
			write_raw((uint32_t) htonl(size));

		} else {
			throw runtime_error("Map too large to serialize");
		}
	}

	// writes the selected type as raw bytes to the data stream, without byte order conversion or type marshalling
	template <typename T>
	inline void write_raw(const T &obj) {
		write_raw_bytes((const uint8_t*)&obj, sizeof(obj));
	}

	// writes the given number of raw bytes to the data stream, without byte order conversion or type unmarshalling
	inline void write_raw_bytes(const uint8_t *buf, size_t bytes) {
		stream.write(buf, bytes);
	}

	inline void flush() {
		stream.flush();
	}

protected:
	Stream &stream;
};

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const long long &obj) {
	if (obj > 0xffffffff) {
		packer.write_raw(MSGPACK_UINT64);
		packer.write_raw((uint64_t) htonll(obj));

	} else if (obj > 0xffff) {
		packer.write_raw(MSGPACK_UINT32);
		packer.write_raw((uint32_t) htonl(obj));

	} else if (obj > 0xff) {
		packer.write_raw(MSGPACK_UINT16);
		packer.write_raw((uint16_t) htons(obj));

	} else if (obj > MSGPACK_POSITIVE_FIXNUM_MAX) {
		packer.write_raw(MSGPACK_UINT8);
		packer.write_raw((uint8_t) obj);

	} else if (obj >= (int8_t) MSGPACK_NEGATIVE_FIXNUM_MIN) {
		packer.write_raw((int8_t) obj);

	} else if (obj >= (int8_t) 0xff) {
		packer.write_raw(MSGPACK_INT8);
		packer.write_raw((int8_t) obj);

	} else if (obj >= (int16_t) 0xffff) {
		packer.write_raw(MSGPACK_INT16);
		packer.write_raw((int16_t) htons(obj));

	} else if (obj >= (int32_t) 0xffffffff) {
		packer.write_raw(MSGPACK_INT32);
		packer.write_raw((int32_t) htonl(obj));

	} else {
		packer.write_raw(MSGPACK_INT64);
		packer.write_raw((int64_t) htonll(obj));
	}
	return packer;
}

template <typename Stream>
Packer<Stream> &operator <<(Packer<Stream> &packer, const unsigned long long &obj) {
	if (obj > 0xffffffff) {
		packer.write_raw(MSGPACK_UINT64);
		packer.write_raw((uint64_t) htonll(obj));

	} else if (obj > 0xffff) {
		packer.write_raw(MSGPACK_UINT32);
		packer.write_raw((uint32_t) htonl(obj));

	} else if (obj > 0xff) {
		packer.write_raw(MSGPACK_UINT16);
		packer.write_raw((uint16_t) htons(obj));

	} else if (obj > MSGPACK_POSITIVE_FIXNUM_MAX) {
		packer.write_raw(MSGPACK_UINT8);
		packer.write_raw((uint8_t) obj);

	} else {
		packer.write_raw((uint8_t) obj);
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
	packer.write_raw(obj ? MSGPACK_TRUE : MSGPACK_FALSE);
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const float &obj) {
	packer.write_raw(MSGPACK_FLOAT);
	packer.write_raw(obj);
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const double &obj) {
	packer.write_raw(MSGPACK_FLOAT);
	packer.write_raw(obj);
	return packer;
}

template <typename Stream>
inline Packer<Stream> &operator <<(Packer<Stream> &packer, const std::string &obj) {
	packer.pack_raw((const uint8_t *)obj.data(), obj.size());
	return packer;
}

template <typename Stream, typename T>
Packer<Stream> &operator <<(Packer<Stream> &packer, const std::vector<T> &obj) {
	packer.pack_array_length(obj.size());
	for (typename std::vector<T>::const_iterator it = obj.begin(); it != obj.end(); ++it) {
		packer << *it;
	}
	return packer;
}

template <typename Stream, typename K, typename V>
Packer<Stream> &operator <<(Packer<Stream> &packer, const std::map<K, V> &obj) {
	packer.pack_map_length(obj.size());
	for (typename std::map<K, V>::const_iterator it = obj.begin(); it != obj.end(); ++it) {
		packer << it->first;
		packer << it->second;
	}
	return packer;
}

#endif
