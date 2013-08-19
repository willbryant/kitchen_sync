#ifndef STREAM_H
#define STREAM_H

#include "msgpack.hpp"
#include <cerrno>

class Stream {
public:
	Stream(int fd): _fd(fd) { }

	template <typename T>
	T &operator >>(T &obj) {
		while (true) {
			msgpack::unpacked result;
			if (unpacker.next(&result)) {
				result.get() >> obj;
				return obj;
			}
			if (!fill_buffer()) {
				throw runtime_error("Reached end of stream unexpectedly");
			}
		}
	};

protected:
	bool fill_buffer();

private:
	int _fd;
	msgpack::unpacker unpacker;
};

bool Stream::fill_buffer() {
	unpacker.reserve_buffer(1024);
	if (unpacker.buffer_capacity() == 0) return true;

	streamsize bytes_read = read(_fd, unpacker.buffer(), unpacker.buffer_capacity());
	if (bytes_read < 0) throw runtime_error("Read from stream failed: " + string(strerror(errno)));
	unpacker.buffer_consumed(bytes_read);
	return (bytes_read > 0);
}

#endif
