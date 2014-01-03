#include <queue>
#include <boost/thread.hpp>

struct SyncTableQueue {
	SyncTableQueue(): tables_not_finished(-1), aborted(false) {}

	void enqueue(const Tables &tables) {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_not_finished = tables.size();
		for (Tables::const_iterator from_table = tables.begin(); from_table != tables.end(); ++from_table) {
			queue.push_back(&*from_table);
		}
		enqueued.notify_all();
	}

	const Table* pop() {
		const Table* table(NULL);
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		while (tables_not_finished && queue.empty()) enqueued.wait(lock);
		if (tables_not_finished) {
			table = queue.front();
			queue.pop_front();
		}
		return table;
	}

	void wait_until_started() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		while (tables_not_finished == -1) enqueued.wait(lock);
	}

	void finished_table() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_not_finished--;
		if (!tables_not_finished) enqueued.notify_all();
	}

	void abort() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_not_finished = 0; // note that this value makes both wait_until_started() and pop() return, so both input & output threads will terminate once they get the notification
		aborted = true;
		enqueued.notify_all();
	}

	bool check_if_finished_all_tables() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		return (tables_not_finished == 0);
	}
	
	list<const Table*> queue;
	boost::mutex queue_mutex;
	boost::condition_variable enqueued;
	size_t tables_not_finished;
	bool aborted;
};
