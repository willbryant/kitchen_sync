#ifndef SYNC_QUEUE_H
#define SYNC_QUEUE_H

#include <queue>
#include <list>
#include <set>

#include "abortable_barrier.h"
#include "schema.h"

using namespace std;

template <typename DatabaseClient>
struct SyncQueue: public AbortableBarrier {
	SyncQueue(size_t workers): AbortableBarrier(workers) {}

	void enqueue_tables_to_process(const Tables &tables) {
		unique_lock<std::mutex> lock(mutex);
		for (const Table &from_table : tables) {
			tables_to_process.push_back(&from_table);
		}
	}

	const Table* pop_table_to_process() {
		unique_lock<std::mutex> lock(mutex);
		if (aborted) throw aborted_error();
		if (tables_to_process.empty()) return nullptr;
		const Table *table = tables_to_process.front();
		tables_to_process.pop_front();
		return table;
	}
	
	list<const Table*> tables_to_process;
	string snapshot;
};

#endif
