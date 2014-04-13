#ifndef PACKED_VALUE
#define PACKED_VALUE

#include "type_codes.h"

struct PackedValue {
	std::vector<uint8_t> encoded_bytes;

	inline uint8_t *extend(size_t bytes) {
		size_t size_before = encoded_bytes.size();
		encoded_bytes.resize(size_before + bytes);
		return encoded_bytes.data() + size_before;
	}

	inline void clear() { encoded_bytes.clear(); }

	inline bool empty() const { return encoded_bytes.empty(); }
	inline size_t size() const { return encoded_bytes.size(); }
	inline uint8_t leader() const { return (encoded_bytes.empty() ? 0 : encoded_bytes.front()); }
	inline const uint8_t *data() const { return encoded_bytes.data(); }

	inline bool is_nil() const { return (leader() == MSGPACK_NIL); }

	inline bool operator == (const PackedValue &other) const {
		return (encoded_bytes == other.encoded_bytes);
	}

	inline bool operator < (const PackedValue &other) const {
		return (encoded_bytes < other.encoded_bytes);
	}
};

#endif
