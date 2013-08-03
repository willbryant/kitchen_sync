#ifndef SCHEMA_FUNCTIONS_H
#define SCHEMA_FUNCTIONS_H

#include <stdexcept>
#include "schema.h"

struct schema_mismatch: public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

void check_schema_match(Database &from_database, Database &to_database);

#endif
