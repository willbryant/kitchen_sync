#ifndef COMMANDS_H
#define COMMANDS_H

#include "msgpack.hpp"

using namespace std;

struct Command {
	string name;
	vector<msgpack::object> arguments;
};

void operator >> (msgpack::object obj, Command &command) {
	if (obj.type != msgpack::type::ARRAY) throw logic_error("Expected an array while reading command");
	if (obj.via.array.size < 1) throw logic_error("Expected at least one element when reading command");
	command.name = obj.via.array.ptr->as<string>();
	command.arguments = vector<msgpack::object>(obj.via.array.ptr + 1, obj.via.array.ptr + obj.via.array.size);
}

inline void send_values(msgpack::packer<ostream> &packer) {
	/* do nothing, this specialization is just to terminate the variadic template expansion */
}

template<typename T, typename... Values>
inline void send_values(msgpack::packer<ostream> &packer, const T &arg0, const Values &...args) {
	packer << arg0;
	send_values(packer, args...);
}

template<typename... Values>
void send_command(ostream &os, const string &name, const Values &...args) {
	msgpack::packer<ostream> packer(os);
	packer.pack_array(1 + sizeof...(Values)); /* name + number of args */
	packer << name;
	send_values(packer, args...);
	os.flush();
}

#endif
