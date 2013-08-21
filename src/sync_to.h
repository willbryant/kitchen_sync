#include <iostream>
#include <unistd.h>

#include "schema.h"
#include "schema_functions.h"
#include "sync_database_data.h"
#include "sync_table_data.h"
#include "sync_table_rows.h"

template<typename DatabaseClient>
void sync_to(DatabaseClient &client) {
	const int PROTOCOL_VERSION_SUPPORTED = 1;
	
	Unpacker input(STDIN_FILENO); // uses file descriptors rather than cin so it can get proper read-available-bytes behavior, which is hidden by istream
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
	sync_database_data(client, input, output, from_database);

	client.commit_transaction();
	send_command(output, "quit");
}
