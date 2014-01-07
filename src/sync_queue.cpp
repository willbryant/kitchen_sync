#include "sync_queue.h"

void SyncQueue::enqueue(const Tables &tables, const set<string> &ignore_tables) {
	boost::unique_lock<boost::mutex> lock(mutex);
	for (Tables::const_iterator from_table = tables.begin(); from_table != tables.end(); ++from_table) {
		if (!ignore_tables.count(from_table->name)) {
			queue.push_back(&*from_table);
		}
	}
}

const Table* SyncQueue::pop() {
	boost::unique_lock<boost::mutex> lock(mutex);
	if (aborted) throw aborted_error();
	if (queue.empty()) return NULL;
	const Table *table = queue.front();
	queue.pop_front();
	return table;
}
