#ifndef LEGACY_SCHEMA_SERIALIZATION
#define LEGACY_SCHEMA_SERIALIZATION

// generally we don't fork the code to extend the protocol for a new version, we just add fields
// and keep the existing ones with the existing semantics.  in protocol version 8 we've done a
// cleanup of legacy quirks, so the separate code below is frozen in to preserve the output for
// protocol version 7 (last used by KS version 1.17) and earlier.

namespace LegacyColumnType {
	const string BLOB = "BLOB";
	const string TEXT = "TEXT";
	const string VCHR = "VARCHAR";
	const string FCHR = "CHAR";
	const string UUID = "UUID";
	const string BOOL = "BOOL";
	const string SINT = "INT";
	const string UINT = "INT UNSIGNED";
	const string REAL = "REAL";
	const string DECI = "DECIMAL";
	const string DATE = "DATE";
	const string TIME = "TIME";
	const string DTTM = "DATETIME";
	const string UNKN = "UNKNOWN";
}

const ColumnTypeList LegacySupportedColumnTypes{
	ColumnType::binary,
	ColumnType::text,
	ColumnType::text_varchar,
	ColumnType::text_fixed,
	ColumnType::uuid,
	ColumnType::boolean,
	ColumnType::sint_8bit,
	ColumnType::sint_16bit,
	ColumnType::sint_24bit,
	ColumnType::sint_32bit,
	ColumnType::sint_64bit,
	ColumnType::uint_8bit,
	ColumnType::uint_16bit,
	ColumnType::uint_24bit,
	ColumnType::uint_32bit,
	ColumnType::uint_64bit,
	ColumnType::float_64bit,
	ColumnType::float_32bit,
	ColumnType::decimal,
	ColumnType::date,
	ColumnType::time,
	ColumnType::time_tz,
	ColumnType::datetime,
	ColumnType::datetime_tz,
	ColumnType::datetime_mysqltimestamp, // even though mysql-specific and represented using a non-specific type, we serialized the flag in the legacy protocol, so it's effectively always a distinct case for our code
};

template <typename OutputStream>
void legacy_serialize(Packer<OutputStream> &packer, const Column &column) {
	string legacy_type = LegacyColumnType::UNKN;
	size_t legacy_size = column.size;
	string legacy_flag;

	switch (column.column_type) {
		case ColumnType::binary:
			legacy_type = LegacyColumnType::BLOB;
			// we used to use the size in bytes, which worked for the types like tinyblob/mediumblob etc, but not for varbinary(n); now we just store the maximum length
			if (column.size < 256) {
				legacy_size = 1;
			} else if (column.size < 65536) {
				legacy_size = 2;
			} else if (column.size < 16777216) {
				legacy_size = 3;
			} else {
				legacy_size = 0; // note this was used to mean max
			}
			break;

		case ColumnType::text:
			legacy_type = LegacyColumnType::TEXT;
			// as for BLOB
			if (column.size < 256) {
				legacy_size = 1;
			} else if (column.size < 65536) {
				legacy_size = 2;
			} else if (column.size < 16777216) {
				legacy_size = 3;
			} else {
				legacy_size = 0; // note this was used to mean max
			}
			break;

		case ColumnType::text_varchar:
			legacy_type = LegacyColumnType::VCHR;
			break;

		case ColumnType::text_fixed:
			legacy_type = LegacyColumnType::FCHR;
			break;

		case ColumnType::uuid:
			legacy_type = LegacyColumnType::UUID;
			break;

		case ColumnType::boolean:
			legacy_type = LegacyColumnType::BOOL;
			break;

		case ColumnType::sint_8bit:
			legacy_type = LegacyColumnType::SINT;
			legacy_size = 1;
			break;

		case ColumnType::sint_16bit:
			legacy_type = LegacyColumnType::SINT;
			legacy_size = 2;
			break;

		case ColumnType::sint_24bit:
			legacy_type = LegacyColumnType::SINT;
			legacy_size = 3;
			break;

		case ColumnType::sint_32bit:
			legacy_type = LegacyColumnType::SINT;
			legacy_size = 4;
			break;

		case ColumnType::sint_64bit:
			legacy_type = LegacyColumnType::SINT;
			legacy_size = 8;
			break;

		case ColumnType::uint_8bit:
			legacy_type = LegacyColumnType::UINT;
			legacy_size = 1;
			break;

		case ColumnType::uint_16bit:
			legacy_type = LegacyColumnType::UINT;
			legacy_size = 2;
			break;

		case ColumnType::uint_24bit:
			legacy_type = LegacyColumnType::UINT;
			legacy_size = 3;
			break;

		case ColumnType::uint_32bit:
			legacy_type = LegacyColumnType::UINT;
			legacy_size = 4;
			break;

		case ColumnType::uint_64bit:
			legacy_type = LegacyColumnType::UINT;
			legacy_size = 8;
			break;

		case ColumnType::float_64bit:
			legacy_type = LegacyColumnType::REAL;
			legacy_size = 8;
			break;

		case ColumnType::float_32bit:
			legacy_type = LegacyColumnType::REAL;
			legacy_size = 4;
			break;

		case ColumnType::decimal:
			legacy_type = LegacyColumnType::DECI;
			break;

		case ColumnType::date:
			legacy_type = LegacyColumnType::DATE;
			break;

		case ColumnType::time:
			legacy_type = LegacyColumnType::TIME;
			break;

		case ColumnType::time_tz:
			legacy_type = LegacyColumnType::TIME;
			legacy_flag = "time_zone";
			break;

		case ColumnType::datetime:
			legacy_type = LegacyColumnType::DTTM;
			break;

		case ColumnType::datetime_tz:
			legacy_type = LegacyColumnType::DTTM;
			legacy_flag = "time_zone";
			break;

		case ColumnType::datetime_mysqltimestamp: // even though mysql-specific, we would always serialize the flag above, so it's effectively always a distinct type
			legacy_type = LegacyColumnType::DTTM;
			legacy_flag = "mysql_timestamp";
			break;

		case ColumnType::unknown:
			legacy_type = LegacyColumnType::UNKN;
			break;

		default:
			// shouldn't happen since we use the LegacyColumnTypes above as the "negotiated" type list when using the legacy protocol
			throw runtime_error("Can't serialize type " + to_string(static_cast<int>(column.column_type)) + " using legacy protocol");
	}

	int fields = 2;
	if (legacy_size) fields++;
	if (column.scale) fields++;
	if (!column.nullable) fields++;
	if (!column.subtype.empty()) fields++;
	if (column.default_type != DefaultType::no_default) fields++;
	if (column.flags.auto_update_timestamp) fields++;
	if (!legacy_flag.empty()) fields++;
	pack_map_length(packer, fields);
	packer << string("name");
	packer << column.name;
	packer << string("column_type");
	packer << legacy_type;
	if (legacy_size) {
		packer << string("size");
		packer << legacy_size;
	}
	if (column.scale) {
		packer << string("scale");
		packer << column.scale;
	}
	if (!column.nullable) {
		packer << string("nullable");
		packer << column.nullable;
	}
	if (!column.subtype.empty()) {
		packer << string("db_type_def");
		packer << column.subtype;
	}
	switch (column.default_type) {
		case DefaultType::no_default:
			break;

		case DefaultType::default_value:
			packer << string("default_value");
			packer << column.default_value;
			break;

		case DefaultType::generated_by_sequence:
		case DefaultType::generated_by_default_as_identity:
		case DefaultType::generated_always_as_identity:
			packer << string("sequence");
			packer << column.default_value; // never populated or used, but had been provided for forward compatibility
			break;

		case DefaultType::default_expression:
		case DefaultType::generated_always_virtual:
		case DefaultType::generated_always_stored:
			packer << string("default_function");
			packer << column.default_value;
			break;

		default:
			throw runtime_error("Default type " + to_string(static_cast<int>(column.default_type)) + " not supported by legacy protocol");
	}
	if (!legacy_flag.empty()) {
		packer << legacy_flag;
		packer << true;
	}
	if (column.flags.auto_update_timestamp) {
		packer << string("mysql_on_update_timestamp");
		packer << true;
	}
}

template <typename VersionedFDWriteStream>
void legacy_serialize(Packer<VersionedFDWriteStream> &packer, const Key &key) {
	pack_map_length(packer, 3);
	packer << string("name");
	packer << key.name;
	packer << string("unique");
	packer << key.unique();
	packer << string("columns");
	packer << key.columns;
}

template <typename InputStream>
void legacy_deserialize(Unpacker<InputStream> &unpacker, Column &column) {
	size_t map_length = unpacker.next_map_length(); // checks type

	while (map_length--) {
		string attr_key = unpacker.template next<string>();

		// note we don't need to support fields or flags not present in v1.17, which was the last version to use protocol version 7
		if (attr_key == "name") {
			unpacker >> column.name;
		} else if (attr_key == "column_type") {
			string legacy_type;
			unpacker >> legacy_type;
			if (legacy_type == LegacyColumnType::BLOB) {
				column.column_type = ColumnType::binary;
			} else if (legacy_type == LegacyColumnType::TEXT) {
				column.column_type = ColumnType::text;
			} else if (legacy_type == LegacyColumnType::VCHR) {
				column.column_type = ColumnType::text_varchar;
			} else if (legacy_type == LegacyColumnType::FCHR) {
				column.column_type = ColumnType::text_fixed;
			} else if (legacy_type == LegacyColumnType::UUID) {
				column.column_type = ColumnType::uuid;
			} else if (legacy_type == LegacyColumnType::BOOL) {
				column.column_type = ColumnType::boolean;
			} else if (legacy_type == LegacyColumnType::SINT) {
				column.column_type = ColumnType::sint_32bit; // modified by size below
			} else if (legacy_type == LegacyColumnType::UINT) {
				column.column_type = ColumnType::uint_32bit; // modified by size below
			} else if (legacy_type == LegacyColumnType::REAL) {
				column.column_type = ColumnType::float_64bit;
			} else if (legacy_type == LegacyColumnType::DECI) {
				column.column_type = ColumnType::decimal;
			} else if (legacy_type == LegacyColumnType::DATE) {
				column.column_type = ColumnType::date;
			} else if (legacy_type == LegacyColumnType::TIME) {
				column.column_type = ColumnType::time;
			} else if (legacy_type == LegacyColumnType::DTTM) {
				column.column_type = ColumnType::datetime;
			} else if (legacy_type == LegacyColumnType::UNKN) {
				column.column_type = ColumnType::unknown;
			} else {
				throw runtime_error("Unexpected legacy column type: " + legacy_type);
			}
		} else if (attr_key == "size") {
			unpacker >> column.size;
			switch (column.column_type) {
				case ColumnType::binary:
				case ColumnType::text:
					// as above, we used to use the size in bytes, which worked for the types like tinyblob/mediumblob etc, but not for varbinary(n); now we just store the maximum length
					switch (column.size) {
						case 0:
							// leave as 0 (meaning max)
							break;

						case 1:
							column.size = 255;
							break;

						case 2:
							column.size = 65535;
							break;

						case 3:
							column.size = 16777215;
							break;

						default:
							throw runtime_error("Unexpected legacy TEXT/BLOB size: " + to_string(column.size));
					}
					break;

				case ColumnType::sint_32bit:
					switch (column.size) {
						case 1:
							column.column_type = ColumnType::sint_8bit;
							break;

						case 2:
							column.column_type = ColumnType::sint_16bit;
							break;

						case 3:
							column.column_type = ColumnType::sint_24bit;
							break;

						case 4:
							// leave as SINT_32BIT
							break;

						case 8:
							column.column_type = ColumnType::sint_64bit;
							break;

						default:
							throw runtime_error("Unexpected legacy SINT size: " + to_string(column.size));
					}
					column.size = 0;
					break;

				case ColumnType::uint_32bit:
					switch (column.size) {
						case 1:
							column.column_type = ColumnType::uint_8bit;
							break;

						case 2:
							column.column_type = ColumnType::uint_16bit;
							break;

						case 3:
							column.column_type = ColumnType::uint_24bit;
							break;

						case 4:
							// leave as UINT_32BIT
							break;

						case 8:
							column.column_type = ColumnType::uint_64bit;
							break;

						default:
							throw runtime_error("Unexpected legacy UINT size: " + to_string(column.size));
					}
					column.size = 0;
					break;

				default:
					// otherwise leave as is
					break;
			}
		} else if (attr_key == "scale") {
			unpacker >> column.scale;
		} else if (attr_key == "nullable") {
			unpacker >> column.nullable;
		} else if (attr_key == "db_type_def") {
			unpacker >> column.subtype;
		} else if (attr_key == "sequence") {
			column.default_type = DefaultType::generated_by_sequence; // debateable which we should use here, but that's the only PostgreSQL identity type that was supported by versions of KS that used the legacy schema serialization format
			unpacker.skip(); // value was never used
		} else if (attr_key == "default_value") {
			column.default_type = DefaultType::default_value;
			unpacker >> column.default_value;
		} else if (attr_key == "default_function") {
			column.default_type = DefaultType::default_expression;
			unpacker >> column.default_value;
		} else if (attr_key == "mysql_timestamp") {
			if (unpacker.template next<bool>()) {
				if (column.column_type == ColumnType::datetime) column.column_type = ColumnType::datetime_mysqltimestamp;
			}
		} else if (attr_key == "mysql_on_update_timestamp") {
			unpacker >> column.flags.auto_update_timestamp;
		} else if (attr_key == "time_zone") {
			if (unpacker.template next<bool>()) {
				if (column.column_type == ColumnType::time) column.column_type = ColumnType::time_tz;
				if (column.column_type == ColumnType::datetime) column.column_type = ColumnType::datetime_tz;
			}
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

#endif
