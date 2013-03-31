/* dumps out the results of a query for debugging - but only supports string values currently */
template<class Row>
struct RowPrinter {
	void operator()(Row &row) {
		for (int column_number = 0; column_number < row.n_columns(); column_number++) {
			cout << row.string_at(column_number);
			if (column_number != row.n_columns() - 1) cout << "\t";
		}
		cout << endl;
	}
};
