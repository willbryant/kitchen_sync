#include "schema.h"

#include <stdexcept>

size_t Table::index_of_column(const string &name) const {
	for (size_t pos = 0; pos < columns.size(); pos++) {
		if (columns[pos].name == name) return pos;
	}
	throw out_of_range("Unknown column " + name);
}
