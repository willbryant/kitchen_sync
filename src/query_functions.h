#ifndef QUERY_FUNCTIONS_H
#define QUERY_FUNCTIONS_H

#include "sql_functions.h"
#include "row_serialization.h" /* for ValueCollector */

template <typename DatabaseClient>
size_t count_rows(DatabaseClient &client, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
	return atoi(client.select_one(count_rows_sql(client, table, prev_key, last_key)).c_str());
}

template <typename DatabaseClient>
ColumnValues first_key(DatabaseClient &client, const Table &table) {
	ValueCollector receiver;
	client.query(select_first_key_sql(client, table), receiver);
	return receiver.values;
}

template <typename DatabaseClient>
ColumnValues last_key(DatabaseClient &client, const Table &table) {
	ValueCollector receiver;
	client.query(select_last_key_sql(client, table), receiver);
	return receiver.values;
}

template <typename DatabaseClient>
ColumnValues not_earlier_key(DatabaseClient &client, const Table &table, const ColumnValues &key, const ColumnValues &prev_key, const ColumnValues &last_key) {
	ValueCollector receiver;
	client.query(select_not_earlier_key_sql(client, table, key, prev_key, last_key), receiver);
	return receiver.values;
}

template <typename DatabaseClient, typename RowReceiver>
size_t retrieve_rows(DatabaseClient &client, RowReceiver &row_receiver, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, ssize_t row_count = NO_ROW_COUNT_LIMIT) {
	return client.query(retrieve_rows_sql(client, table, prev_key, last_key, row_count), row_receiver);
}

template <typename RowReceiver>
struct ForwardSameKeyOnly: public RowLastKey {
	ForwardSameKeyOnly(RowReceiver &row_receiver, const ColumnIndices &primary_key_columns, const ColumnValues &curr_key): RowLastKey(primary_key_columns), row_receiver(row_receiver), curr_key(curr_key), rows_forwarded(0) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		RowLastKey::operator()(row);

		if (last_key == curr_key) {
			row_receiver(row);
			rows_forwarded++;
		}
	}

	RowReceiver &row_receiver;
	const ColumnValues &curr_key;
	size_t rows_forwarded;
};

template <typename DatabaseClient, typename RowReceiver>
size_t retrieve_extra_rows_with_same_key(DatabaseClient &client, RowReceiver &row_receiver, const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key, const ColumnValues &curr_key, ssize_t rows_already_retrieved) {
	ForwardSameKeyOnly<RowReceiver> forwarder(row_receiver, table.primary_key_columns, curr_key);
	size_t rows_to_request = 1;
	while (true) {
		size_t row_count = client.query(retrieve_rows_sql(client, table, prev_key, last_key, rows_to_request, rows_already_retrieved + forwarder.rows_forwarded /* skip however many rows as we've already processed */), forwarder);
		if (row_count < rows_to_request || forwarder.last_key != curr_key) return forwarder.rows_forwarded;
		if (rows_to_request < 1024*1024) rows_to_request *= 2; // avoid making O(n) queries if the partial key turns out to have poor selectivity
	}
}

#endif
