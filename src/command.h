#ifndef COMMANDS_H
#define COMMANDS_H

#include <stdexcept>
#include "message_pack/pack.h"
#include "message_pack/unpack_any.h"

using namespace std;

typedef uint32_t verb_t;

struct Command {
	verb_t verb;
	vector<boost::any> arguments;

	template<class T>
	T argument(int index) const {
		T value;
		arguments[index] >> value;
		return value;
	}
};

template <typename InputStream>
Command &operator >> (Unpacker<InputStream> &unpacker, Command &command) {
	size_t array_length = unpacker.next_array_length(); // checks type
	if (array_length < 1) throw logic_error("Expected at least one element when reading command");

	command.verb = unpacker.template next<verb_t>();
	command.arguments.clear();
	while (--array_length) {
		command.arguments.push_back(unpacker.template next<boost::any>());
	}

	return command;
}

template <typename OutputStream>
inline void send_values(Packer<OutputStream> &packer) {
	/* do nothing, this specialization is just to terminate the variadic template expansion */
}

template <typename OutputStream, typename T, typename... Values>
inline void send_values(Packer<OutputStream> &packer, const T &arg0, const Values &...args) {
	packer << arg0;
	send_values(packer, args...);
}

template <typename OutputStream, typename... Values>
void send_command(Packer<OutputStream> &packer, verb_t verb, const Values &...args) {
	packer.pack_array_length(1 + sizeof...(Values)); /* verb + number of args */
	packer << verb;
	send_values(packer, args...);
	packer.flush();
}

struct command_error: public runtime_error {
	command_error(const string &error): runtime_error(error) { }
};

namespace Commands {
	const verb_t OPEN = 1;
	const verb_t ROWS = 2;
	const verb_t HASH = 3;
	const verb_t ROWS_AND_HASH = 4;

	const verb_t PROTOCOL = 32;
	const verb_t EXPORT_SNAPSHOT  = 33;
	const verb_t IMPORT_SNAPSHOT  = 34;
	const verb_t UNHOLD_SNAPSHOT  = 35;
	const verb_t WITHOUT_SNAPSHOT = 36;
	const verb_t SCHEMA = 37;
	const verb_t QUIT = 0;
};

#endif
