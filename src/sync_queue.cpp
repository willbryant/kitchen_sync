#include "sync_queue.h"

void SyncQueue::enqueue(const Tables &tables) {
	unique_lock<std::mutex> lock(mutex);
	for (const Table &from_table : tables) {
		queue.push_back(&from_table);
	}
}

const Table* SyncQueue::pop() {
	unique_lock<std::mutex> lock(mutex);
	if (aborted) throw aborted_error();
	if (queue.empty()) return nullptr;
	const Table *table = queue.front();
	queue.pop_front();
	return table;
}
