#include <stdexcept>

class UnidirectionalPipe {
public:
	UnidirectionalPipe();
	~UnidirectionalPipe();

	int  read_fileno();
	int write_fileno();

	void dup_read_to(int fd);
	void dup_write_to(int fd);

	void close_read();
	void close_write();

private:
	int pipe_handles[2];

	// forbid copying
	UnidirectionalPipe(const UnidirectionalPipe& copy_from) { throw std::logic_error("copying forbidden"); }
};
