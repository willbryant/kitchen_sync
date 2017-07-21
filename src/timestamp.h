#include <iomanip>
#include <sys/time.h>

inline double timestamp() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec + tv.tv_usec/1000000.0;
}
