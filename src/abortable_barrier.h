#ifndef ABORTABLE_BARRIER_H
#define ABORTABLE_BARRIER_H

#include <stdexcept>
#include <thread>
#include <mutex>
#include <condition_variable>

struct aborted_error: public std::runtime_error {
	aborted_error(): runtime_error("Aborted") { }
};

struct AbortableBarrier {
	AbortableBarrier(size_t workers): workers(workers), waiting_for_workers(workers), generation(0), aborted(false) {}

	bool wait_at_barrier();
	void check_aborted();
	bool abort();

	std::mutex mutex;
	std::condition_variable cond;
	size_t workers;
	size_t waiting_for_workers;
	size_t generation;
	bool aborted;
};

#endif
