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

#endif
