#ifndef LEGACY_SCHEMA_SERIALIZATION
#define LEGACY_SCHEMA_SERIALIZATION

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

#endif
