#include <string>
#include <boost/regex.hpp>
#include <boost/program_options.hpp>

using namespace std;
using namespace boost;
using namespace boost::program_options;

struct DbUrl {
	DbUrl(const string &protocol, const string &username, const string &password, const string &host, const string &port, const string &database):
		protocol(protocol), username(username), password(password), host(host), port(port), database(database) {}

	string protocol, username, password, host, port, database;

	inline static int from_hex(char ch);
	static string urldecode(const string &str);
};

inline int DbUrl::from_hex(char ch) { 
	if (ch >= '0' && ch <= '9') {
		return (ch - '0');
	} else if (ch >= 'a' && ch <= 'f') {
		return (ch - 'a' + 10);
	} else if (ch >= 'A' && ch <= 'F') {
		return (ch - 'A' + 10);
	} else {
		return -1;
	}
}

string DbUrl::urldecode(const string &str) {
	string result;
	result.reserve(str.length()); // optimization

	string::const_iterator it = str.begin(); 
	string::const_iterator end = str.end(); 

	while (it != end) {
		if (*it == '%') {
			// escape sequence
			int digit1 = from_hex(*(++it));

			if (digit1 >= 0) {
				int digit2 = from_hex(*(++it));

				if (digit2 >= 0) {
					// valid hex escape, store the decoded character
					result += digit1 * 16 + digit2;

					// loop around and carry on with the next digit
					++it;
					continue;
				} else {
					// invalid escape sequence, copy the % through as a literal, and go back a place to copy the first hex digit
					result += '%';
					--it;
					// fall through to copy the first hex digit as a literal
				}
			} else {
				// invalid escape sequence, copy the % through as a literal
				result += '%';
				// fall through to copy the first hex digit as a literal
			}
		}

		// normal character
		result += *(it++);
	}

	return result;
}

void validate(
	boost::any& v, 
	const vector<string>& values,
	DbUrl* target_type,
	int) {
	// vendor://[user[:pass]@]hostname[:port]/database
	static regex r("^(\\w+)://(?:([^:]+)(?::([^@]+))?@)?([^:/]+)(?::(.+))?/([^/]+)$");
	validators::check_first_occurrence(v);

	smatch match;
	if (regex_match(validators::get_single_string(values), match, r)) {
		// FUTURE: implement percent-decoding
		v = any(DbUrl(
				DbUrl::urldecode(match[1]),
				DbUrl::urldecode(match[2]),
				DbUrl::urldecode(match[3]),
				DbUrl::urldecode(match[4]),
				DbUrl::urldecode(match[5]),
				DbUrl::urldecode(match[6])));
	} else {
		throw validation_error(validation_error::invalid_option_value);
	}
}
