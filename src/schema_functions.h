#ifndef SCHEMA_FUNCTIONS_H
#define SCHEMA_FUNCTIONS_H

#include <stdexcept>
#include <set>

#include "schema.h"

struct schema_mismatch: public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

void match_schemas(const Database &from_database, const Database &to_database);

#endif
