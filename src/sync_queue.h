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
	TableJob(const Table &table): table(table), notify_when_work_could_be_shared(false), hash_commands(0), hash_commands_completed(0), rows_commands(0) {}

	inline bool have_work_to_share() { return (!ranges_to_check.empty()); }

	const Table &table;
	std::mutex mutex;
	std::condition_variable borrowed_task_completed;

	list<KeyRange> ranges_to_retrieve;
	list<KeyRangeWithRowCount> ranges_to_check;
	bool notify_when_work_could_be_shared;

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

		if (tables_to_process.empty()) {
			for (shared_ptr<TableJob> table_job : tables_to_process)      start_sharing_work_in(table_job);
			for (shared_ptr<TableJob> table_job : tables_being_processed) start_sharing_work_in(table_job);
			return nullptr;
		}

		shared_ptr<TableJob> table_job = tables_to_process.front();
		tables_to_process.pop_front();
		tables_being_processed.insert(table_job);

		return table_job;
	}

	void completed_table(const shared_ptr<TableJob> &table_job) {
		unique_lock<std::mutex> lock(mutex);

		tables_with_work_to_share.erase(table_job);
		tables_being_processed.erase(table_job);

		if (finished()) {
			// unblock workers waiting in borrow_work()
			cond.notify_all();
		}
	}

	void have_work_to_share(const shared_ptr<TableJob> &table_job) {
		unique_lock<std::mutex> lock(mutex);

		tables_with_work_to_share.insert(table_job);

		cond.notify_all();
	}

	shared_ptr<TableJob> borrow_work() {
		unique_lock<std::mutex> lock(mutex);

		while (true) {
			if (finished()) {
				return nullptr;
			}

			for (auto it = tables_with_work_to_share.begin(); it != tables_with_work_to_share.end(); it = tables_with_work_to_share.erase(it)) {
				const shared_ptr<TableJob> &table_job(*it);
				unique_lock<std::mutex> table_job_lock(table_job->mutex);

				if (table_job->have_work_to_share()) {
					return table_job;
				}
			}

			cond.wait(lock);
		}
	}

	bool abort() {
		bool result = AbortableBarrier::abort();

		for (shared_ptr<TableJob> table_job : tables_being_processed) {
			unique_lock<std::mutex> table_job_lock(table_job->mutex);
			table_job->borrowed_task_completed.notify_all();
		}

		return result;
	}

	string snapshot;

private:
	inline bool finished() {
		return (tables_to_process.empty() && tables_being_processed.empty());
	}

	void start_sharing_work_in(shared_ptr<TableJob> table_job) {
		unique_lock<std::mutex> table_job_lock(table_job->mutex);

		table_job->notify_when_work_could_be_shared = true;

		if (table_job->have_work_to_share()) {
			tables_with_work_to_share.insert(table_job);
		}
	}

	list<shared_ptr<TableJob>> tables_to_process;
	set<shared_ptr<TableJob>> tables_being_processed;
	set<shared_ptr<TableJob>> tables_with_work_to_share;
};

#endif
