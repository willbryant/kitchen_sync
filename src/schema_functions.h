#ifndef SCHEMA_FUNCTIONS_H
#define SCHEMA_FUNCTIONS_H

#include <stdexcept>
#include <set>

#include "schema.h"

struct schema_mismatch: public runtime_error {
	schema_mismatch(const string &error): runtime_error(error) { }
};

inline int extract_column_length(const string &db_type) {
       size_t pos = db_type.find('(');
       if (pos >= db_type.length() - 1) throw runtime_error("Couldn't find length in type specification " + db_type);
       return atoi(db_type.c_str() + pos + 1);
}

inline int extract_column_scale(const string &db_type) {
       size_t pos = db_type.find(',');
       if (pos >= db_type.length() - 1) throw runtime_error("Couldn't find scale in type specification " + db_type);
       return atoi(db_type.c_str() + pos + 1);
}

void match_schemas(const Database &from_database, const Database &to_database);

#endif
