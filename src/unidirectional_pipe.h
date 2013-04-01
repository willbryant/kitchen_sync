class UnidirectionalPipe {
public:
	UnidirectionalPipe();
	~UnidirectionalPipe();

	int  read_fileno();
	int write_fileno();

	void close_read();
	void close_write();

private:
	int pipe_handles[2];
};
