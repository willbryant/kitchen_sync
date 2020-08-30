#ifndef PACKED_VALUE
#define PACKED_VALUE

#include "packed_buffer.h"
#include "type_codes.h"

struct PackedValue: public PackedBuffer {
	inline size_t encoded_size() const { return used; }
	inline uint8_t leader() const { return (used ? *encoded_bytes : 0); }
	inline const uint8_t *data() const { return encoded_bytes; }

	inline bool is_nil() const { return (leader() == MSGPACK_NIL); }

	inline bool operator ==(const PackedValue &other) const {
		return (encoded_size() == other.encoded_size() && memcmp(data(), other.data(), encoded_size()) == 0);
	}

	inline bool operator !=(const PackedValue &other) const {
		return (encoded_size() != other.encoded_size() || memcmp(data(), other.data(), encoded_size()) != 0);
	}

	inline bool operator <(const PackedValue &other) const {
		if (encoded_size() != other.encoded_size()) return (encoded_size() < other.encoded_size());
		return (memcmp(data(), other.data(), encoded_size()) < 0);
	}
};

#endif
