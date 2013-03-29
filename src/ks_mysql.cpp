#include "endpoint.h"

#include <stdexcept>
#include <mysql.h>

class MySQLClient {
public:
	MySQLClient(
		const char *database_host,
		const char *database_port,
		const char *database_name,
		const char *database_username,
		const char *database_password);
	~MySQLClient();

private:
	MYSQL mysql;
};

MySQLClient::MySQLClient(
	const char *database_host,
	const char *database_port,
	const char *database_name,
	const char *database_username,
	const char *database_password) {

	// mysql_real_connect takes separate params for numeric ports and unix domain sockets
	int port = 0;
	const char *socket = NULL;
	if (database_port) {
		if (*database_port >= '0' && *database_port <= '9') {
			port = atoi(database_port);
		} else {
			socket = database_port;
		}
	}

	mysql_init(&mysql);
	mysql_options(&mysql, MYSQL_READ_DEFAULT_GROUP, "ks_mysql");
	if (!mysql_real_connect(&mysql, database_host, database_username, database_password, database_name, port, socket, 0)) {
		throw runtime_error(mysql_error(&mysql));
	}
}

MySQLClient::~MySQLClient() {
	mysql_close(&mysql);
}

int main(int argc, char *argv[]) {
	return endpoint_main<MySQLClient>(argc, argv);
}
