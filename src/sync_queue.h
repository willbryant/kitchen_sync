#ifndef SYNC_QUEUE_H
#define SYNC_QUEUE_H

#include <queue>
#include <list>
#include <set>
#include <memory>

#include "abortable_barrier.h"
#include "schema.h"

using namespace std;

typedef tuple<ColumnValues, ColumnValues> KeyRange;
typedef tuple<ColumnValues, ColumnValues, size_t, size_t> KeyRangeWithRowCount;
const size_t UNKNOWN_ROW_COUNT = numeric_limits<size_t>::max();

struct TableJob {
	TableJob(const Table &table): table(table), hash_commands(0), hash_commands_completed(0), rows_commands(0) {}

	const Table &table;
	std::mutex mutex;
	std::condition_variable borrowed_task_completed;

	list<KeyRange> ranges_to_retrieve;
	list<KeyRangeWithRowCount> ranges_to_check;

	size_t hash_commands;
	size_t hash_commands_completed;
	size_t rows_commands;
};

template <typename DatabaseClient>
struct SyncQueue: public AbortableBarrier {
	SyncQueue(size_t workers): AbortableBarrier(workers) {}

	void enqueue_tables_to_process(const Tables &tables) {
		unique_lock<std::mutex> lock(mutex);
		for (const Table &from_table : tables) {
			tables_to_process.push_back(make_shared<TableJob>(from_table));
		}
	}

	shared_ptr<TableJob> pop_table_to_process() {
		unique_lock<std::mutex> lock(mutex);
		if (aborted) throw aborted_error();
		if (tables_to_process.empty()) return nullptr;
		shared_ptr<TableJob> table_job = tables_to_process.front();
		tables_to_process.pop_front();
		tables_being_processed.insert(table_job);
		return table_job;
	}

	void completed_table(shared_ptr<TableJob> table_job) {
		unique_lock<std::mutex> lock(mutex);
		tables_being_processed.erase(table_job);
	}
	
	list<shared_ptr<TableJob>> tables_to_process;
	set<shared_ptr<TableJob>> tables_being_processed;
	string snapshot;
};

#endif
