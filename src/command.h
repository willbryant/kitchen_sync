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

void send_command(ostream &os, const string &name) {
	msgpack::packer<ostream> packer(os);
	packer.pack_array(1 + 0); /* name + number of args */
	packer << name;
	os.flush();
}

template<class T1>
void send_command(ostream &os, const string &name, const T1 &arg1) {
	msgpack::packer<ostream> packer(os);
	packer.pack_array(1 + 1); /* name + number of args */
	packer << name;
	packer << arg1;
	os.flush();
}

template<class T1, class T2>
void send_command(ostream &os, const string &name, const T1 &arg1, const T2 &arg2) {
	msgpack::packer<ostream> packer(os);
	packer.pack_array(1 + 2); /* name + number of args */
	packer << name;
	packer << arg1;
	packer << arg2;
	os.flush();
}

#endif
