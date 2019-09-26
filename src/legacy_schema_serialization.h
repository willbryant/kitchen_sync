#ifndef LEGACY_SCHEMA_SERIALIZATION
#define LEGACY_SCHEMA_SERIALIZATION

// generally we don't fork the code to extend the protocol for a new version, we just add fields
// and keep the existing ones with the existing semantics.  in protocol version 8 we've done a
// cleanup of legacy quirks, so the separate code below is frozen in to preserve the output for
// protocol version 7 (last used by KS version 1.17) and earlier.

template <typename OutputStream>
void legacy_serialize(Packer<OutputStream> &packer, const Column &column) {
	int fields = 2;
	if (column.size) fields++;
	if (column.scale) fields++;
	if (!column.nullable) fields++;
	if (!column.db_type_def.empty()) fields++;
	if (column.default_type != DefaultType::no_default) fields++;
	if (column.flags.mysql_timestamp) fields++;
	if (column.flags.mysql_on_update_timestamp) fields++;
	if (column.flags.time_zone) fields++;
	pack_map_length(packer, fields);
	packer << string("name");
	packer << column.name;
	packer << string("column_type");
	packer << column.column_type;
	if (column.size) {
		packer << string("size");
		if (column.column_type == ColumnTypes::BLOB || column.column_type == ColumnTypes::TEXT) {
			// we used to use the size in bytes, which worked for the types like tinyblob/mediumblob etc, but not for varbinary(n); now we just store the maximum length
			if (column.size < 256) {
				packer << 1;
			} else if (column.size < 65536) {
				packer << 2;
			} else if (column.size < 16777216) {
				packer << 3;
			} else {
				packer << 0; // note this was used to mean max
			}
		} else {
			packer << column.size;
		}
	}
	if (column.scale) {
		packer << string("scale");
		packer << column.scale;
	}
	if (!column.nullable) {
		packer << string("nullable");
		packer << column.nullable;
	}
	if (!column.db_type_def.empty()) {
		packer << string("db_type_def");
		packer << column.db_type_def;
	}
	switch (column.default_type) {
		case DefaultType::no_default:
			break;

		case DefaultType::sequence:
			packer << string("sequence");
			packer << column.default_value; // currently unused, but allowed for forward compatibility
			break;

		case DefaultType::default_value:
			packer << string("default_value");
			packer << column.default_value;
			break;

		case DefaultType::default_expression:
			packer << string("default_function");
			packer << column.default_value;
			break;
	}
	if (column.flags.mysql_timestamp) {
		packer << string("mysql_timestamp");
		packer << true;
	}
	if (column.flags.mysql_on_update_timestamp) {
		packer << string("mysql_on_update_timestamp");
		packer << true;
	}
	if (column.flags.time_zone) {
		packer << string("time_zone");
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
			unpacker >> column.column_type;
		} else if (attr_key == "size") {
			unpacker >> column.size;
			if (column.column_type == ColumnTypes::BLOB || column.column_type == ColumnTypes::TEXT) {
				// as above, we used to use the size in bytes, which worked for the types like tinyblob/mediumblob etc, but not for varbinary(n); now we just store the maximum length
				switch (column.size) {
					case 1:
						column.size = 255;
						break;

					case 2:
						column.size = 65535;
						break;

					case 3:
						column.size = 16777215;
						break;
				}
			}
		} else if (attr_key == "scale") {
			unpacker >> column.scale;
		} else if (attr_key == "nullable") {
			unpacker >> column.nullable;
		} else if (attr_key == "db_type_def") {
			unpacker >> column.db_type_def;
		} else if (attr_key == "sequence") {
			column.default_type = DefaultType::sequence;
			unpacker >> column.default_value; // currently unused, but allowed for forward compatibility
		} else if (attr_key == "default_value") {
			column.default_type = DefaultType::default_value;
			unpacker >> column.default_value;
		} else if (attr_key == "default_function") {
			column.default_type = DefaultType::default_expression;
			unpacker >> column.default_value;
		} else if (attr_key == "mysql_timestamp") {
			unpacker >> column.flags.mysql_timestamp;
		} else if (attr_key == "mysql_on_update_timestamp") {
			unpacker >> column.flags.mysql_on_update_timestamp;
		} else if (attr_key == "time_zone") {
			unpacker >> column.flags.time_zone;
		} else {
			// ignore anything else, for forward compatibility
			unpacker.skip();
		}
	}
}

inline ColumnTypeList legacy_supported_types() {
	return ColumnTypeList{
		ColumnTypes::BLOB,
		ColumnTypes::TEXT,
		ColumnTypes::VCHR,
		ColumnTypes::FCHR,
		ColumnTypes::UUID,
		ColumnTypes::BOOL,
		ColumnTypes::SINT,
		ColumnTypes::UINT,
		ColumnTypes::REAL,
		ColumnTypes::DECI,
		ColumnTypes::DATE,
		ColumnTypes::TIME,
		ColumnTypes::DTTM,
	};
}

#endif
