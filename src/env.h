#ifndef ENV_H
#define ENV_H

#include <cstdlib>
#include <string>

const char *getenv_default(const char *name, const char *default_value) {
	return getenv(name) ? getenv(name) : default_value;
}

int getenv_default(const char *name, int default_value) {
	return getenv(name) ? atoi(getenv(name)) : default_value;
}

void setenv(const char *name, const std::string &value) {
	setenv(name, value.c_str(), 1);
}

#endif
