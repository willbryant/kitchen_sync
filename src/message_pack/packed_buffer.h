#ifndef PACKED_BUFFER_H
#define PACKED_BUFFER_H

#include <string.h>
#include <stdlib.h>
#include <stdexcept>

struct PackedBuffer {
	PackedBuffer(): encoded_bytes(nullptr), used(0) {}

	~PackedBuffer() {
		free(encoded_bytes);
	}

	PackedBuffer(const PackedBuffer &from): encoded_bytes(nullptr) {
		*this = from;
	}

	PackedBuffer(PackedBuffer &&from): encoded_bytes(nullptr) {
		*this = std::move(from);
	}

	PackedBuffer &operator=(const PackedBuffer &from) {
		if (&from != this) {
			free(encoded_bytes);
			encoded_bytes = nullptr;
			used = from.used;
			if (used) {
				encoded_bytes = (uint8_t *)malloc(used);
				if (!encoded_bytes) throw std::bad_alloc();
				memcpy(encoded_bytes, from.encoded_bytes, used);
			}
		}
		return *this;
	}

	PackedBuffer &operator=(PackedBuffer &&from) {
		if (this != &from) {
			free(encoded_bytes);
			used = from.used;
			encoded_bytes = from.encoded_bytes;
			from.encoded_bytes = nullptr;
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
		encoded_bytes = nullptr;
		used = 0;
	}

	inline void write(const uint8_t *src, size_t bytes) {
		memcpy(extend(bytes), src, bytes);
	}

protected:
	uint8_t *encoded_bytes;
	size_t used;
};

#endif
