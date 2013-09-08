#ifndef TO_STRING_H
#define TO_STRING_H

#include <string>
#include <sstream>

template <typename T>
std::string to_string(T n) {
	std::ostringstream stream;
	stream << n;
	return stream.str();
}

#endif
