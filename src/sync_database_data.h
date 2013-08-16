template <typename DatabaseClient>
void sync_database_data(
	DatabaseClient &client, Stream &input, const Database &database) {

	// single-threaded for now
	for (Tables::const_iterator from_table = database.tables.begin(); from_table != database.tables.end(); ++from_table) {
		sync_table_data(client, input, *from_table);
	}
}
