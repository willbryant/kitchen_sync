#ifndef UNPACK_ANY_H
#define UNPACK_ANY_H

// use boost::any to handle multiple types
#include <boost/any.hpp>

// use boost::optional to indicate nils
#include <boost/optional.hpp>

// use unordered_map to provide maps for boost::any without needing a cross-type < function; boost::unordered_map uses the system's std::unordered_map or tr1::unordered_map if available, so no downside to using this given we need boost for the other includes in this file
#include <boost/unordered_map.hpp>

#include "unpack.h"

template <typename Stream, typename K, typename V>
Unpacker<Stream> &operator >>(Unpacker<Stream> &unpacker, boost::unordered_map<K, V> &obj) {
	size_t map_length = unpacker.next_map_length();
	obj.clear();
	obj.reserve(map_length);
	while (map_length--) {
		K key = unpacker.template next<K>();
		V val = unpacker.template next<V>();
		obj[key] = val;
	}
	return unpacker;
}

// attempt to provide useful comparisons of boost::any key values; supports empty (nil),
// strings, and integers.
struct hash_any {
	size_t operator ()(const boost::any &obj) const {
		if (obj.empty()) return 0; // arbitrary
		if (boost::any_cast<string*>(obj)) return boost::hash_value(*boost::any_cast<string*>(obj));
		if (boost::any_cast<int64_t*>(obj)) return boost::hash_value(*boost::any_cast<int64_t*>(obj));
		throw std::bad_cast();
	}
};

typedef std::vector<boost::any> any_vector;
typedef boost::unordered_map<boost::any, boost::any, hash_any> any_map;

template <typename Stream>
Unpacker<Stream> &operator >>(Unpacker<Stream> &unpacker, boost::any &obj) {
	uint8_t leader = unpacker.peek();

	// raw => string
	if ((leader >= MSGPACK_FIXRAW_MIN && leader <= MSGPACK_FIXRAW_MAX) ||
		leader == MSGPACK_RAW16 || leader == MSGPACK_RAW32) {
		obj = unpacker.template next<std::string>();

	// arrays
	} else if ((leader >= MSGPACK_FIXARRAY_MIN && leader <= MSGPACK_FIXARRAY_MAX) ||
		leader == MSGPACK_ARRAY16 || leader == MSGPACK_ARRAY32) {
		obj = unpacker.template next<any_vector>();

	// maps
	} else if ((leader >= MSGPACK_FIXMAP_MIN && leader <= MSGPACK_FIXMAP_MAX) ||
		leader == MSGPACK_MAP16 || leader == MSGPACK_MAP32) {
		obj = unpacker.template next<any_map>();

	// boolean
	} else if (leader == MSGPACK_FALSE || leader == MSGPACK_TRUE) {
		obj = unpacker.template next<bool>();

	// nil - bit debateable how to handle these, could hack in nullptr_t
	} else if (leader == MSGPACK_NIL) {
		unpacker.next_nil();
		boost::optional<boost::any> value; // don't set to anything
		obj = value;

	// treat anything else as numeric, discarding all type information (including signedness, problematically)
	} else {
		int64_t value;
		unpacker >> value;
		obj = value;
	}
	return unpacker;
}

template <typename T>
const boost::any &operator >>(const boost::any &obj, T &value) {
	const T* typed = boost::any_cast<T>(&obj);
	if (typed) {
		value = *typed;
		return obj;
	}
	throw runtime_error("Don't know how to convert " + string(obj.type().name()) + " to " + string(typeid(T).name()));
}

template <typename T>
const boost::any &operator >>(const boost::any &obj, std::vector<T> &value) {
	const std::vector<T>* typed = boost::any_cast<std::vector<T> >(&obj);
	if (typed) {
		value = *typed;
		return obj;
	}
	const any_vector* typed_vector = boost::any_cast<any_vector>(&obj);
	if (typed_vector) {
		value.clear();
		value.resize(typed_vector->size());
		for (size_t n = 0; n < typed_vector->size(); n++) (*typed_vector)[n] >> value[n];
		return obj;
	}
	throw runtime_error("Don't know how to convert " + string(obj.type().name()) + " to " + string(typeid(T).name()));
}

template <typename K, typename V>
const boost::any &operator >>(const boost::any &obj, std::map<K, V> &value) {
	const std::map<K, V>* typed = boost::any_cast<std::map<K, V> >(&obj);
	if (typed) {
		value = *typed;
		return obj;
	}
	const any_map* typed_map = boost::any_cast<any_map>(&obj);
	if (typed_map) {
		value.clear();
		value.resize(typed_map->size());
		for (typename std::map<K, V>::const_iterator it = typed_map->begin(); it != typed_map->end(); ++it) {
			K k;
			V v;
			it->first >> k;
			it->second >> v;
			value[k] = v;
		}
		return obj;
	}
	throw runtime_error("Don't know how to convert " + string(obj.type().name()) + " to map<" + string(typeid(K).name()) + "," + string(typeid(K).name()) + ">");
}

#endif
