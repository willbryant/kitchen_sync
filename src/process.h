#include <string>
#include <unistd.h>

using namespace std;

class UnidirectionalPipe;

class Process {
public:
	static string related_binary_path(const string &argv0, const string &this_program_name, const string &desired_binary_name);
	static pid_t fork_and_exec(const string &binary, const char *args[]);
	static pid_t fork_and_exec(const string &binary, const char *args[], UnidirectionalPipe &stdin_pipe, UnidirectionalPipe &stdout_pipe);
	static bool wait_for_and_check(pid_t child);
};
