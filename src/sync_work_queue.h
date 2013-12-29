#include <queue>
#include <boost/thread.hpp>

struct WorkTask {
	WorkTask(): table(NULL) {}
	WorkTask(const Table &table): table(&table) {}
	WorkTask(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash): table(&table), prev_key(prev_key), last_key(last_key), hash(hash) {}

	const Table *table;
	ColumnValues prev_key;
	ColumnValues last_key;
	string hash;

	bool operator <(const WorkTask &other) const {
		if (table == other.table) return false;
		if (!table) return true;
		return (table->name > other.table->name); // earlier-named tables have higher priority
	}
};

struct SyncWorkQueue {
	SyncWorkQueue(): tables_left(-1), aborted(false) {}

	void enqueue(const Tables &tables) {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_left = tables.size();
		for (Tables::const_iterator from_table = tables.begin(); from_table != tables.end(); ++from_table) {
			queue.push(WorkTask(*from_table));
		}
		enqueued.notify_all();
	}

	void enqueue(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const string &hash) {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		bool was_empty = queue.empty();
		queue.push(WorkTask(table, prev_key, last_key, hash));
		if (was_empty) enqueued.notify_one();
	}

	WorkTask pop() {
		WorkTask task;
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		while (tables_left && queue.empty()) enqueued.wait(lock);
		if (tables_left) {
			task = queue.top();
			queue.pop();
		}
		return task;
	}

	void wait_until_started() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		while (tables_left == -1) enqueued.wait(lock);
	}

	void finished_table() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_left--;
		if (!tables_left) enqueued.notify_all();
	}

	void abort() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_left = 0; // note that this value makes both wait_until_started() and pop() return, so both input & output threads will terminate once they get the notification
		aborted = true;
		enqueued.notify_all();
	}

	bool check_if_finished_all_tables() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		return (tables_left == 0);
	}
	
	priority_queue<WorkTask> queue;
	boost::mutex queue_mutex;
	boost::condition_variable enqueued;
	size_t tables_left;
	bool aborted;
};
