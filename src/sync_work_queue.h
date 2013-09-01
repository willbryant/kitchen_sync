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
	SyncWorkQueue(const Tables &tables) {
		tables_left = tables.size();
		for (Tables::const_iterator from_table = tables.begin(); from_table != tables.end(); ++from_table) {
			queue.push(WorkTask(*from_table));
		}
	}

	priority_queue<WorkTask> queue;
	boost::mutex queue_mutex;
	boost::condition_variable enqueued;
	size_t tables_left;

	void finished_table() {
		boost::unique_lock<boost::mutex> lock(queue_mutex);
		tables_left--;
		if (!tables_left) enqueued.notify_all();
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
};
