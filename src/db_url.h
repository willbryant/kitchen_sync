#include <string>

using namespace std;

struct DbUrl {
	DbUrl() {}
	DbUrl(const string &protocol, const string &username, const string &password, const string &host, const string &port, const string &database):
		protocol(protocol), username(username), password(password), host(host), port(port), database(database) {}
	DbUrl(const string &url);

	string protocol, username, password, host, port, database;

	inline static int from_hex(char ch);
	static string urldecode(const string &str);
};
