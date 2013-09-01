#ifndef COMMANDS_H
#define COMMANDS_H

#include "message_pack/pack.h"
#include "message_pack/unpack_any.h"

using namespace std;

struct Command {
	string name;
	vector<boost::any> arguments;

	template<class T>
	T argument(int index) const {
		T value;
		arguments[index] >> value;
		return value;
	}
};

Command &operator >> (Unpacker &unpacker, Command &command) {
	size_t array_length = unpacker.next_array_length(); // checks type
	if (array_length < 1) throw logic_error("Expected at least one element when reading command");

	command.name = unpacker.next<string>();
	command.arguments.clear();
	while (--array_length) {
		command.arguments.push_back(unpacker.next<boost::any>());
	}

	return command;
}

template<typename Stream>
inline void send_values(Packer<Stream> &packer) {
	/* do nothing, this specialization is just to terminate the variadic template expansion */
}

template<typename Stream, typename T, typename... Values>
inline void send_values(Packer<Stream> &packer, const T &arg0, const Values &...args) {
	packer << arg0;
	send_values(packer, args...);
}

template<typename... Values>
void send_command(Packer<ostream> &packer, const string &name, const Values &...args) {
	packer.pack_array_length(1 + sizeof...(Values)); /* name + number of args */
	packer << name;
	send_values(packer, args...);
	packer.flush();
}

#endif
