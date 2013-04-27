#ifndef COMMANDS_H
#define COMMANDS_H

#include "msgpack.hpp"

using namespace std;

struct Command {
	string name;
	vector<msgpack::object> arguments;

	inline Command() { }
	inline Command(const string &name): name(name) { }
	inline Command(const string &name, int intarg): name(name) { arguments.push_back(msgpack::object(intarg)); }
};

void operator << (msgpack::packer<ostream> &packer, const Command &command) {
	packer.pack_array(1 + command.arguments.size());
	packer << command.name;
	for (vector<msgpack::object>::const_iterator it = command.arguments.begin(); it != command.arguments.end(); it++) packer << *it;
}

void operator << (ostream &os, const Command &command) {
	msgpack::packer<ostream> packer(os);
	packer << command;
	os.flush();
};

void operator >> (msgpack::object obj, Command &command) {
	if (obj.type != msgpack::type::ARRAY) throw logic_error("Expected an array while reading command");
	if (obj.via.array.size < 1) throw logic_error("Expected at least one element when reading command");
	command.name = obj.via.array.ptr->as<string>();
	command.arguments = vector<msgpack::object>(obj.via.array.ptr + 1, obj.via.array.ptr + obj.via.array.size);
}

#endif
