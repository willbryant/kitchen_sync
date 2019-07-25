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

inline string hex_to_bin_string(const char *input, int length) {
	string result;
	result.resize(length/2);
	for (int pos = 0; pos < length; pos += 2) {
		unsigned char hi = (unsigned char)from_hex_nibble(*input++);
		unsigned char lo = (unsigned char)from_hex_nibble(*input++);
		result[pos/2] = (char)(hi << 4 | lo);
	}
	return result;
}

// converts the MySQL-specific geometry values - a 4-byte SRID at the start followed by normal WKB data - to PostgreSQL-style
// EWKB values.  there's no particular reason to prefer PostgreSQL's format over MySQL's; we just have to pick one and PostGIS
// is much more widely used than MySQL's spatial type support (most of which is comparatively recent) and supports formats
// that MySQL doesn't (eg. a third dimension), so their type is a superset of MySQL's.
// nb. we've chosen to implement this here rather than putting an expression based on say ST_AsBinary in the SELECT statement
// because if the user has set a replace: option for a geometry column it's not clear how to combine the two expressions.
inline string mysql_bin_to_ewkb_bin(const char *input, int length) {
	// mysql geometry strings should always start with a 4-byte SRID, a 1 byte BOM, and a 4-byte geometry type code;
	// anything else is clearly invalid.
	if (length < 9 || (input[4] != 0 && input[4] != 1)) {
		return string(input, length);
	}

	if (input[0] == 0 && input[1] == 0 && input[2] == 0 && input[3] == 0) {
		// zero SRID (ie. SRID not set), skip it
		return string(input + 4, length - 4);
	} else {
		// SRID set, move it to after the type code
		string result(input + 4, 5);          // BOM + geometry type code
		result.append(input, 4);              // SRID
		result.append(input + 9, length - 9); // rest of the data

		// XOR 0x20000000 into the geometry type code - but in LE format, this will be represented as 0x00000020
		bool little_endian = input[4] == 1;
		result[little_endian ? 4 : 1] |= 0x20;
		return result;
	}
}

// converts the PostgreSQL-style EWKB values (which we assume have already been hex-decoded) to MySQL's format - a 4-byte
// SRID at the start followed by normal WKB data.  this isn't actually the only difference between WKB and EWKB, but we can't do
// anything about the rest of the differences, which mostly relate to supporting extra dimensions that MySQL can't deal with.
// MySQL will raise an SQL error if they user attempts to sync unsupported geometry data.
// nb. we've chosen to implement this here rather than putting an expression based on say ST_AsBinary in the SELECT statement
// because if the user has set a replace: option for a geometry column it's not clear how to combine the two expressions.
inline string ewkb_bin_to_mysql_bin(const string &input) {
	// WKB should always start with a byte-order indicator byte (with value 0 for BE and 1 for LE) and a 4-byte geometry type
	// code (note we've hex-decoded postgresql results before calling this function, so we're dealing with actual byte counts);
	// anything else is clearly invalid.
	if (input.length() < 5 || (input[0] != 0 && input[0] != 1)) {
		return input;
	}

	bool little_endian = input[0] == 1;

	// if an SRID is present, EWKB XORs the value 0x20000000 into the geometry type code - but byte-swapped for LE as above
	if (input[little_endian ? 4 : 1] & 0x20) {
		// SRID present, move it to the front
		string result(input.substr(5, 4)); // SRID
		result.append(input.substr(0, 5)); // BOM + geometry type code
		result.append(input.substr(9));    // rest of the data

		// XOR the SRID flag back out
		result[little_endian ? 8 : 5] ^= 0x20; // we've moved the SRID to the start, making the flag position 4 bytes later
		return result;
	} else {
		// if there's no SRID, we simply need to prefix with a zero SRID we're done
		string result("\0\0\0\0", 4);
		result.append(input);
		return result;
	}
}

#endif
