#ifndef ENCODE_PACKED
#define ENCODE_PACKED

#include "message_pack/copy_packed.h"

template <typename DatabaseClient>
string encode(DatabaseClient &client, const PackedValue &value) {
	if (value.is_nil())		return "NULL";
	if (value.is_false())	return "false";
	if (value.is_true())	return "true";

	VectorReadStream stream(value);
	Unpacker<VectorReadStream> unpacker(stream);
	string unpacked_value;
	unpacker >> unpacked_value;
	return "'" + client.escape_value(unpacked_value) + "'";
}

#endif
