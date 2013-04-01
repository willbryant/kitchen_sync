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

pid_t Process::fork_and_exec(const string &binary, const char *args[], UnidirectionalPipe &stdin_pipe, UnidirectionalPipe &stdout_pipe) {
	pid_t child = fork();

	if (child < 0) {
		// hit the process limit
		throw runtime_error("Couldn't fork to start first binary: " + string(strerror(errno)));

	} else if (child == 0) {
		// we are the child; close the ends of the pipe that we won't use (as otherwise we'd not see the pipes close when the other end is done)
		int result;
		stdin_pipe.close_write();
		stdout_pipe.close_read();

		// attach our stdin
		do { result = dup2(stdin_pipe.read_fileno(), STDIN_FILENO); } while (result < 0 && errno == EINTR); // closes STDIN_FILENO
		if (result < 0) {
			throw runtime_error("Couldn't reattach STDIN: " + string(strerror(errno)));
		}
		stdin_pipe.close_read();

		// attach our stdout
		do { result = dup2(stdout_pipe.write_fileno(), STDOUT_FILENO); } while (result < 0 && errno == EINTR); // closes STDOUT_FILENO
		if (result < 0) {
			throw runtime_error("Couldn't reattach STDOUT: " + string(strerror(errno)));
		}
		stdout_pipe.close_write();

		// run the binary
		if (execv(binary.c_str(), (char * const *)args) < 0) {
			throw runtime_error("Couldn't exec " + binary + string(strerror(errno)));
		}
		throw logic_error("execv returned");

	} else {
		return child;
	}
}

void Process::fork_and_exec_pair(const string &binary1, const string &binary2, const char *args1[], const char *args2[], pid_t *child1, pid_t *child2) {
	UnidirectionalPipe pipe1;
	UnidirectionalPipe pipe2;
	*child1 = fork_and_exec(binary1, args1, pipe1, pipe2);
	*child2 = fork_and_exec(binary2, args2, pipe2, pipe1);
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
