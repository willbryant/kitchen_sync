#ifndef EWKB_H
#define EWKB_H

inline int from_hex_nibble(char ch) {
	if (ch >= '0' && ch <= '9') {
		return (ch - '0');
	} else if (ch >= 'a' && ch <= 'f') {
		return (ch - 'a' + 10);
	} else if (ch >= 'A' && ch <= 'F') {
		return (ch - 'A' + 10);
	} else {
		return -1;
	}
}

inline string from_hex_string(const char *input, int length) {
	string result;
	result.resize(length/2);
	for (int pos = 0; pos < length; pos += 2) {
		unsigned char hi = (unsigned char)from_hex_nibble(*input++);
		unsigned char lo = (unsigned char)from_hex_nibble(*input++);
		result[pos/2] = (char)(hi << 4 | lo);
	}
	return result;
}

// converts the PostgreSQL-specific EWKB hex strings to our standard format, which is effectively MySQL's - a 4-byte SRID at
// the start followed by normal WKB data.  this isn't actually the only difference between WKB and EWKB, but we can't do
// anything about the rest of the differences, which mostly relate to supporting extra dimensions that MySQL can't deal with.
// there's no strong reason to prefer MySQL's format over PostgreSQL's; we just have to pick one and it just seems safer to
// prefer a format that doesn't involve bit-bashing, and we need to hex-decode PostgreSQL's data anyway.
// nb. we've chosen to implement this here rather than putting an expression based on say ST_AsBinary in the SELECT statement
// because if the user has set a replace: option for a geometry column it's not clear how to combine the two expressions.
inline string ewkb_hex_to_standard_geom_bin(const char *input, int length) {
	// EKWB should always start with a byte-order indicator byte (with value 0 for BE and 1 for LE) and a 4-byte geometry type
	// code, and postgresql gives it back to us hex encoded, so geometry values should always be at least 10 characters long
	if (length < 10 || length % 2 != 0 || input[0] != '0' || (input[1] != '0' && input[1] != '1')) {
		return string(input, length);
	}

	bool little_endian = input[1] == '1';

	// if an SRID is present, EWKB XORs the value 0x20000000 into the geometry type code.  in LE format though, this will be
	// represented as 0x00000020.
	if (from_hex_nibble(input[little_endian ? 8 : 2]) & 0x2) {
		// SRID present, move it to the front (we're effectively emulating mysql style here)
		string result(from_hex_string(input + 10, 8));
		result.append(from_hex_string(input, 10));
		result.append(from_hex_string(input + 18, length - 18));
		result[little_endian ? 8 : 5] ^= 0x20; // hex-decoding has halved all the offsets, and then we've moved the SRID to the start, making the flag position 4 bytes later
		return result;
	} else {
		// if there's no SRID, we simply need to hex-decode and prefix with a zero SRID we're done
		return string("\0\0\0\0", 4) + from_hex_string(input, length);
	}
}

#endif
