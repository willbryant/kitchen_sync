#include "db_url.h"

#include <stdexcept>

using namespace std;

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

pair<string, string> split_pair(const string &str, const string &separator, int pick_which_side) {
	size_t pos = str.find(separator);
	if (pos != string::npos) {
		return pair<string, string>(str.substr(0, pos), str.substr(pos + separator.length()));
	} else if (pick_which_side > 0) {
		return pair<string, string>("", str);
	} else if (pick_which_side < 0) {
		return pair<string, string>(str, "");
	} else {
		throw invalid_argument("Invalid URL part: expected to find '" + separator + "' in '" + str + "'");
	}
}

pair<string, string> separate_bracketed_host_and_port(const string &str) {
	size_t pos = str.find(']');
	if (pos == str.length() - 1) {
		return pair<string, string>(str.substr(1, pos - 1), "");
	} else if (pos != string::npos && str[pos + 1] == ':') {
		return pair<string, string>(str.substr(1, pos - 1), str.substr(pos + 2));
	} else {
		throw invalid_argument("Invalid URL part: expected host address to end with ']' in '" + str + "'");
	}
}

DbUrl::DbUrl(const string &url) {
	// vendor://[user[:pass]@]hostname[:port]/database
	pair<string, string> protocol_and_rest = split_pair(url, "://", 0);
	pair<string, string> username_password_host_port_and_database = split_pair(protocol_and_rest.second, "/", 0);
	pair<string, string> username_password_and_host_port = split_pair(username_password_host_port_and_database.first, "@", 1);
	pair<string, string> username_password = split_pair(username_password_and_host_port.first, ":", -1);
	pair<string, string> host_port = !username_password_and_host_port.second.empty() && username_password_and_host_port.second[0] == '[' ?
		separate_bracketed_host_and_port(username_password_and_host_port.second) : split_pair(username_password_and_host_port.second, ":", -1);
	pair<string, string> database_and_schema = split_pair(username_password_host_port_and_database.second, "/", -1);

	protocol = DbUrl::urldecode(protocol_and_rest.first);
	username = DbUrl::urldecode(username_password.first);
	password = DbUrl::urldecode(username_password.second);
	host = DbUrl::urldecode(host_port.first);
	port = DbUrl::urldecode(host_port.second);
	database = DbUrl::urldecode(database_and_schema.first);
	schema = DbUrl::urldecode(database_and_schema.second);
}
