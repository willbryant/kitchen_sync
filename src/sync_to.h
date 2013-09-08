#include <iostream>
#include <unistd.h>

#include "schema.h"
#include "schema_functions.h"
#include "sync_database_data.h"

template<typename DatabaseClient>
void sync_to(const char *database_host, const char *database_port, const char *database_name, const char *database_username, const char *database_password) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;

	DatabaseClient client(database_host, database_port, database_name, database_username, database_password, false /* not readonly */);
	DatabaseClient read_client(database_host, database_port, database_name, database_username, database_password, true /* readonly */);
	Unpacker<istream> input(cin);
	Packer<ostream> output(cout);

	// tell the other end what protocol we speak, and have them tell us which version we're able to converse in
	send_command(output, "protocol", PROTOCOL_VERSION_SUPPORTED);
	int protocol;
	input >> protocol;

	// get its schema
	send_command(output, "schema");
	Database from_database;
	input >> from_database;

	// get our end's schema
	Database to_database(client.database_schema());

	// check they match
	check_schema_match(from_database, to_database);

	// start syncing table data
	sync_database_data(client, read_client, input, output, from_database);

	client.commit_transaction();
	send_command(output, "quit");
}
