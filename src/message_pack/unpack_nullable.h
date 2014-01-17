#ifndef UNPACK_NULLABLE_H
#define UNPACK_NULLABLE_H

#include "unpack.h"

template <typename ValueType>
struct Nullable {
	inline Nullable(): null(true) {}
	inline Nullable(ValueType value): null(false), value(value) {}

	inline bool operator == (const Nullable &other) const {
		return (other.null == null && other.value == value);
	}

	bool null;
	ValueType value;
};

template <typename Stream, typename ValueType>
Unpacker<Stream> &operator >>(Unpacker<Stream> &unpacker, Nullable<ValueType> &obj) {
	if (unpacker.next_is_nil()) {
		unpacker.next_nil();
		obj.null = true;
		obj.value = ValueType();
	} else {
		obj.null = false;
		unpacker >> obj.value;
	}
	return unpacker;
}

#endif
