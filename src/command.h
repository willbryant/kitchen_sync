#ifndef COMMANDS_H
#define COMMANDS_H

#include <stdexcept>
#include "message_pack/pack.h"
#include "message_pack/unpack.h"

using namespace std;

struct command_error: public runtime_error {
	command_error(const string &error): runtime_error(error) { }
};

typedef uint32_t verb_t;

template <typename InputStream>
inline void read_values(Unpacker<InputStream> &unpacker) {
	/* do nothing, this specialization is just to terminate the variadic template expansion */
}

template <typename InputStream, typename T, typename... Values>
inline void read_values(Unpacker<InputStream> &unpacker, T &arg0, Values &...args) {
	unpacker >> arg0;
	read_values(unpacker, args...);
}

template <typename InputStream, typename... Values>
inline void read_array(Unpacker<InputStream> &unpacker, Values &...args) {
	size_t array_length = unpacker.next_array_length(); // checks type
	if (array_length != sizeof...(args)) throw command_error("Expected " + to_string(sizeof...(args)) + " arguments, got " + to_string(array_length));
	read_values(unpacker, args...);
}

template <typename InputStream, typename... Values>
inline void read_all_arguments(Unpacker<InputStream> &unpacker, Values &...args) {
	read_array(unpacker, args...);
	if (sizeof...(Values) > 0) { // in which case read_array has already checked we received the empty array to indicate no more arguments
		size_t array_length = unpacker.next_array_length(); // checks type
		if (array_length != 0) throw command_error("Expected only one set of arguments");
	}
}


template <typename InputStream, typename... Values>
inline void read_expected_command(Unpacker<InputStream> &unpacker, verb_t expected_verb, Values &...args) {
	verb_t verb;
	unpacker >> verb;
	if (verb != expected_verb) throw command_error("Expected verb " + to_string(expected_verb) + " but received " + to_string(verb));
	read_all_arguments(unpacker, args...);
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
inline void send_array(Packer<OutputStream> &packer, const Values &...args) {
	pack_array_length(packer, sizeof...(Values)); /* number of args */
	send_values(packer, args...);
}

template <typename OutputStream, typename... Values>
inline void send_command_begin(Packer<OutputStream> &packer, verb_t verb, const Values &...args) {
	packer << verb;
	send_array(packer, args...);
}

template <typename OutputStream>
inline void send_command_end(Packer<OutputStream> &packer) {
	pack_array_length(packer, 0); // each command is terminated by an array of length 0
	packer.flush();
}

template <typename OutputStream, typename... Values>
inline void send_command(Packer<OutputStream> &packer, verb_t verb, const Values &...args) {
	send_command_begin(packer, verb, args...);
	if (sizeof...(Values) > 0) { // in which case send_command_begin has already sent the empty array to indicate no more arguments
		send_command_end(packer);
	} else {
		packer.flush();
	}
}

namespace Commands {
	const verb_t OPEN = 1;
	const verb_t ROWS = 2;
	const verb_t HASH_NEXT = 3;
	const verb_t HASH_FAIL = 4;
	const verb_t ROWS_AND_HASH_NEXT = 5;
	const verb_t ROWS_AND_HASH_FAIL = 6;

	const verb_t PROTOCOL = 32;
	const verb_t EXPORT_SNAPSHOT  = 33;
	const verb_t IMPORT_SNAPSHOT  = 34;
	const verb_t UNHOLD_SNAPSHOT  = 35;
	const verb_t WITHOUT_SNAPSHOT = 36;
	const verb_t SCHEMA = 37;
	const verb_t TARGET_BLOCK_SIZE = 38;
	const verb_t QUIT = 0;
};

#endif
