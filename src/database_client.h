#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include <map>
#include "schema.h"

class DatabaseClient {
public:
	inline const Database &database_schema() { return database; }

protected:
	Database database;
};

#endif
