#ifndef PACKED_BUFFER_H
#define PACKED_BUFFER_H

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdexcept>

#define FIXED_PACKED_BUFFER_SIZE 16

struct PackedBuffer {
	PackedBuffer(): used(0) {}

	~PackedBuffer() {
		if (heap_allocated()) free(_allocd);
	}

	PackedBuffer(const PackedBuffer &from): used(0) {
		*this = from;
	}

	PackedBuffer(PackedBuffer &&from): used(0) {
		*this = std::move(from);
	}

	PackedBuffer &operator=(const PackedBuffer &from) {
		if (&from != this) {
			clear();
			if (from.heap_allocated()) {
				_allocd = (uint8_t *)malloc(from.used);
				if (!_allocd) throw std::bad_alloc();
				memcpy(_allocd, from._allocd, from.used);
			} else if (from.used) {
				memcpy(_inline, from._inline, from.used);
			}
			used = from.used;
		}
		return *this;
	}

	PackedBuffer &operator=(PackedBuffer &&from) {
		if (this != &from) {
			if (heap_allocated()) free(_allocd);
			if (from.heap_allocated()) {
				_allocd = from._allocd;
			} else {
				memcpy(_inline, from._inline, from.used);
			}
			used = from.used;
			from.used = 0;
		}
		return *this;
	}

	inline uint8_t *extend(size_t bytes) {
		size_t used_before = used;

		if (heap_allocated()) {
			uint8_t *new_malloc = (uint8_t *)realloc(_allocd, used + bytes);
			if (!new_malloc) throw std::bad_alloc();
			_allocd = new_malloc;

		} else if (used + bytes > FIXED_PACKED_BUFFER_SIZE) {
			uint8_t *new_malloc = (uint8_t *)malloc(used + bytes);
			if (!new_malloc) throw std::bad_alloc();
			memcpy(new_malloc, _inline, used_before);
			_allocd = new_malloc;
		}

		used += bytes;
		return buffer() + used_before;
	}

	inline void clear() {
		if (heap_allocated()) free(_allocd);
		used = 0;
	}

	inline void write(const uint8_t *src, size_t bytes) {
		memcpy(extend(bytes), src, bytes);
	}

protected:
	inline   bool heap_allocated() const { return (used > FIXED_PACKED_BUFFER_SIZE); }
	inline const uint8_t *buffer() const { return (heap_allocated() ? _allocd : _inline); }
	inline       uint8_t *buffer()       { return (heap_allocated() ? _allocd : _inline); }

	size_t used;
	union {
		uint8_t  _inline[FIXED_PACKED_BUFFER_SIZE];
		uint8_t *_allocd;
	};
};

#endif
