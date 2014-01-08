#include "abortable_barrier.h"

bool AbortableBarrier::wait_at_barrier() {
	boost::unique_lock<boost::mutex> lock(mutex);
	if (aborted) throw aborted_error();

	if (--waiting_for_workers == 0) {
		generation++; // overflows are OK
		waiting_for_workers = workers;
		cond.notify_all();
		return true;
	}

	size_t current_generation = generation;
	while (true) {
		cond.wait(lock);
		if (aborted) throw aborted_error();
		if (generation != current_generation) return false;
	}
}

void AbortableBarrier::check_aborted() {
	boost::unique_lock<boost::mutex> lock(mutex);
	if (aborted) throw aborted_error();
}

bool AbortableBarrier::abort() {
	boost::unique_lock<boost::mutex> lock(mutex);
	if (aborted) return false;
	aborted = true;
	cond.notify_all();
	return true;
}
