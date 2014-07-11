#include "process.h"

#include <stdexcept>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/wait.h>

#include "unidirectional_pipe.h"

string Process::related_binary_path(const string &argv0, const string &this_program_name, const string &desired_binary_name) {
	if (argv0.length() > this_program_name.length() &&
		argv0[argv0.length() - this_program_name.length() - 1] == '/' &&
		argv0.substr(argv0.length() - this_program_name.length(), this_program_name.length()) == this_program_name) {
		return argv0.substr(0, argv0.length() - this_program_name.length()) + desired_binary_name;
	} else {
		return desired_binary_name;
	}
}

pid_t Process::fork_and_exec(const string &binary, const char *args[]) {
	pid_t child = fork();

	if (child < 0) {
		// hit the process limit
		throw runtime_error("Couldn't fork to start binary: " + string(strerror(errno)));

	} else if (child == 0) {
		// we are the child; run the binary
		if (execvp(binary.c_str(), (char * const *)args) < 0) {
			throw runtime_error("Couldn't exec " + binary + ": " + string(strerror(errno)));
		}
		throw logic_error("execv returned");

	} else {
		return child;
	}
}

pid_t Process::fork_and_exec(const string &binary, const char *args[], UnidirectionalPipe &stdin_pipe, UnidirectionalPipe &stdout_pipe) {
	pid_t child = fork();

	if (child < 0) {
		// hit the process limit
		throw runtime_error("Couldn't fork to start binary: " + string(strerror(errno)));

	} else if (child == 0) {
		// we are the child; close the ends of the pipe that we won't use (as otherwise we'd not see the pipes close when the other end is done)
		stdin_pipe.close_write();
		stdout_pipe.close_read();

		// attach our stdin
		stdin_pipe.dup_read_to(STDIN_FILENO);
		stdin_pipe.close_read();

		// attach our stdout
		stdout_pipe.dup_write_to(STDOUT_FILENO);
		stdout_pipe.close_write();

		// run the binary
		if (execv(binary.c_str(), (char * const *)args) < 0) {
			throw runtime_error("Couldn't exec " + binary + ": " + string(strerror(errno)));
		}
		throw logic_error("execv returned");

	} else {
		return child;
	}
}

bool Process::wait_for_and_check(pid_t child) {
	int status;
	while (true) {
		if (waitpid(child, &status, 0) < 0) {
			throw runtime_error("Couldn't wait for child: " + string(strerror(errno)));
		}
		return WIFEXITED(status) && WEXITSTATUS(status) == 0;
	}
}
