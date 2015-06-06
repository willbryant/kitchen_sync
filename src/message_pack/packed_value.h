#ifndef PACKED_VALUE
#define PACKED_VALUE

#include <string.h>
#include <stdlib.h>
#include <stdexcept>
#include "type_codes.h"

struct PackedValue {
	PackedValue(): encoded_bytes(NULL), used(0) {}

	~PackedValue() {
		free(encoded_bytes);
	}

	PackedValue(const PackedValue &from): encoded_bytes(NULL) {
		*this = from;
	}

	PackedValue(PackedValue &&from): encoded_bytes(NULL) {
		*this = std::move(from);
	}

	PackedValue &operator=(const PackedValue &from) {
		if (&from != this) {
			free(encoded_bytes);
			encoded_bytes = NULL;
			used = from.used;
			if (used) {
				encoded_bytes = (uint8_t *)malloc(used);
				if (!encoded_bytes) throw std::bad_alloc();
				memcpy(encoded_bytes, from.encoded_bytes, used);
			}
		}
		return *this;
	}

	PackedValue &operator=(PackedValue &&from) {
		if (this != &from) {
			free(encoded_bytes);
			used = from.used;
			encoded_bytes = from.encoded_bytes;
			from.encoded_bytes = NULL;
			from.used = 0;
		}
		return *this;
	}

	inline uint8_t *extend(size_t bytes) {
		uint8_t *new_encoded_bytes = (uint8_t *)realloc(encoded_bytes, used + bytes);
		if (!new_encoded_bytes) throw std::bad_alloc();
		size_t size_before = used;
		used += bytes;
		encoded_bytes = new_encoded_bytes;
		return encoded_bytes + size_before;
	}

	inline void clear() {
		free(encoded_bytes);
		encoded_bytes = NULL;
		used = 0;
	}

	inline void write(const uint8_t *src, size_t bytes) {
		memcpy(extend(bytes), src, bytes);
	}

	inline bool empty() const { return !used; }
	inline size_t size() const { return used; }
	inline uint8_t leader() const { return (used ? *encoded_bytes : 0); }
	inline const uint8_t *data() const { return encoded_bytes; }

	inline bool is_nil()   const { return (leader() == MSGPACK_NIL); }
	inline bool is_false() const { return (leader() == MSGPACK_FALSE); }
	inline bool is_true()  const { return (leader() == MSGPACK_TRUE); }

	inline bool operator == (const PackedValue &other) const {
		return (used == other.used && memcmp(encoded_bytes, other.encoded_bytes, used) == 0);
	}

	inline bool operator < (const PackedValue &other) const {
		if (used != other.used) return (used < other.used);
		return (memcmp(encoded_bytes, other.encoded_bytes, used) < 0);
	}

protected:
	uint8_t *encoded_bytes;
	size_t used;
};

#endif
