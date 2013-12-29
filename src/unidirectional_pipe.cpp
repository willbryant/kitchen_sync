#include "unidirectional_pipe.h"

#include <stdexcept>
#include <string>
#include <cerrno>
#include <cstring>
#include <unistd.h>

using namespace std;

UnidirectionalPipe::UnidirectionalPipe() {
	if (pipe(pipe_handles) < 0) {
		throw runtime_error("Couldn't create a pipe: " + string(strerror(errno)));
	}
}

UnidirectionalPipe::~UnidirectionalPipe() {
	close_read();
	close_write();
}

/* we arbitrarily make the first the read end and the second the write end; in fact they're identical to the OS */
int UnidirectionalPipe:: read_fileno() { return pipe_handles[0]; }
int UnidirectionalPipe::write_fileno() { return pipe_handles[1]; }

void UnidirectionalPipe::dup_read_to(int fd) {
	int result;
	do { result = dup2(read_fileno(), fd); } while (result < 0 && errno == EINTR); // closes fd if it is currently open
	if (result < 0) {
		throw runtime_error("Couldn't reattach read descriptor: " + string(strerror(errno)));
	}
}

void UnidirectionalPipe::dup_write_to(int fd) {
	int result;
	do { result = dup2(write_fileno(), fd); } while (result < 0 && errno == EINTR); // closes fd if it is currently open
	if (result < 0) {
		throw runtime_error("Couldn't reattach write descriptor: " + string(strerror(errno)));
	}
}

void UnidirectionalPipe::close_read() {
	if (pipe_handles[0]) {
		close(pipe_handles[0]);
		pipe_handles[0] = 0;
	}
}

void UnidirectionalPipe::close_write() {
	if (pipe_handles[1]) {
		close(pipe_handles[1]);
		pipe_handles[1] = 0;
	}
}
