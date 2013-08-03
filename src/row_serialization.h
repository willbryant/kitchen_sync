template<class DatabaseRow>
struct RowPacker {
	RowPacker(msgpack::packer<ostream> &packer): packer(packer) {}

	~RowPacker() {
		// we use nil to indicate the end of the rowset
		packer.pack_nil();
	}

	void operator()(const DatabaseRow &row) {
		packer.pack_array(row.n_columns());

		for (int i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				packer.pack_nil();
			} else {
				packer << row.string_at(i);
			}
		}
	}

	msgpack::packer<ostream> &packer;
};
