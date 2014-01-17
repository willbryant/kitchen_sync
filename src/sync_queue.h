#ifndef SYNC_QUEUE_H
#define SYNC_QUEUE_H

#include <queue>
#include <set>

#include "abortable_barrier.h"
#include "schema.h"

struct SyncQueue: public AbortableBarrier {
	SyncQueue(size_t workers): AbortableBarrier(workers) {}

	void enqueue(const Tables &tables, const set<string> &ignore_tables, const set<string> &only_tables);
	const Table* pop();
	
	list<const Table*> queue;
	string snapshot;
};

#endif
