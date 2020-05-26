#ifndef VERSION_H
#define VERSION_H

#include <sstream>

#define KS_MAJOR_VERSION 2
#define KS_MINOR_VERSION 3
#define KS_MAINT_VERSION 0

inline string ks_version() {
	std::ostringstream string_stream;

	string_stream << KS_MAJOR_VERSION << '.' << KS_MINOR_VERSION << '.' << KS_MAINT_VERSION;

	return string_stream.str();
}

#endif
