#ifndef SCHEMA_FUNCTIONS_H
#define SCHEMA_FUNCTIONS_H

#include <stdexcept>

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

#endif
