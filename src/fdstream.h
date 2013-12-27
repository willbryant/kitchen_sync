#ifndef FDSTREAM_H
#define FDSTREAM_H

#include <unistd.h>
#include <stdexcept>

struct stream_error: public std::runtime_error {
	stream_error(const std::string &error): runtime_error(error) {}
};

struct FDReadStream {
	FDReadStream(int fd): fd(fd), buf_pos(0), buf_avail(0) {}
	~FDReadStream() {}

	// gets but does not consume the next raw byte from the data stream
	inline uint8_t peek() {
		if (!buf_avail) fill_buf();
		return buf[buf_pos];
	}

	// reads the given number of raw bytes from the data stream
	inline void read(uint8_t *dest, size_t bytes) {
		while (bytes > buf_avail) {
			memcpy(dest, buf + buf_pos, buf_avail);
			dest   += buf_avail;
			bytes -= buf_avail;
			fill_buf();
		}
		memcpy(dest, buf + buf_pos, bytes);
		buf_pos   += bytes;
		buf_avail -= bytes;
	}

protected:
	void fill_buf() {
		ssize_t bytes_read;
		while (true) {
			bytes_read = ::read(fd, buf, sizeof(buf));
			if (bytes_read <= 0) {
				if (errno == EINTR) continue;
				throw stream_error("Couldn't read from descriptor: " + string(strerror(errno)));
			}
			buf_avail = bytes_read;
			buf_pos = 0;
			break;
		}
	}

	int fd;
	size_t buf_pos, buf_avail;
	uint8_t buf[16384];
};

struct FDWriteStream {
	FDWriteStream(int fd): fd(fd), buf_used(0) {}
	~FDWriteStream() {}

	// writes the given number of raw bytes to the data stream, possibly using a buffer; call flush() to force that to the underlying descriptor
	inline void write(const uint8_t *src, size_t bytes) {
		if (bytes > sizeof(buf)) { // this both protects against integer overflows and avoids unnecessary copying into our buffer for large objects
			flush();
			write_buf(src, bytes);

		} else if (buf_used + bytes > sizeof(buf)) {
			flush();
			memcpy(buf, src, bytes);
			buf_used = bytes;

		} else {
			memcpy(buf + buf_used, src, bytes);
			buf_used += bytes;
		}
	}

	// forces any bytes currently in the buffer to the underlying descriptor
	inline void flush() {
		write_buf(buf, buf_used);
		buf_used = 0;
	}

protected:
	void write_buf(const uint8_t* ptr, size_t bytes) {
		ssize_t bytes_written;
		while (bytes > 0) {
			bytes_written = ::write(fd, ptr, bytes);
			if (bytes_written <= 0) {
				if (errno == EINTR) continue;
				throw stream_error("Couldn't write to descriptor: " + string(strerror(errno)));
			}
			ptr   += bytes_written;
			bytes -= bytes_written;
		}
	}

	int fd;
	size_t buf_used;
	uint8_t buf[16384];
};

#endif
