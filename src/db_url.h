#include <string>

using namespace std;

struct DbUrl {
	DbUrl() {}
	DbUrl(const string &url);

	string protocol, username, password, host, port, database, schema;

	inline static int from_hex(char ch);
	static string urldecode(const string &str);
};
