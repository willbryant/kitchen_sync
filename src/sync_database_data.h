template <typename DatabaseClient>
void sync_database_data(
	DatabaseClient &client, Unpacker &input, Packer<ostream> &output, const Database &database) {

	client.disable_referential_integrity();

	// single-threaded for now
	for (Tables::const_iterator from_table = database.tables.begin(); from_table != database.tables.end(); ++from_table) {
		sync_table_data(client, input, output, *from_table);
	}

	client.enable_referential_integrity();
}
