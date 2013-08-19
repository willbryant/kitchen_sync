#ifndef COMMANDS_H
#define COMMANDS_H

#include "msgpack.hpp"
#include "unpacker.h"

using namespace std;

struct Command {
	string name;
	// TODO: implement arbitrary object deserialization; happens we only need strings in this position right now
	string arg0;
	// TODO: implement arbitrary object deserialization; happens we only need arrays of strings in this position right now
	vector< vector<string> > args;

	template<class T>
	T argument(int index) {
		return args[index - 1];
	}
};

Command &operator >> (Unpacker &unpacker, Command &command) {
	size_t array_length = unpacker.next_array_length(); // checks type
	if (array_length < 1) throw logic_error("Expected at least one element when reading command");

	command.name = unpacker.next<string>();
	array_length--;

	// TODO: implement arbitrary object deserialization; happens we only need strings in this position right now
	if (array_length >= 2) {
		unpacker >> command.arg0;
		array_length--;
	} else {
		command.arg0.clear();
	}

	command.args.clear();
	while (array_length--) {
		// TODO: implement arbitrary object deserialization; happens we only need arrays of strings in this position right now
		command.args.push_back(unpacker.next< vector<string> >());
	}

	return command;
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
