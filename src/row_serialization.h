template<class DatabaseRow>
struct RowPacker {
	RowPacker(msgpack::packer<ostream> &packer): packer(packer), packed_length(false) {}

	~RowPacker() {
		if (!packed_length) {
			packer.pack_array(0);
		}
	}

	void operator()(const DatabaseRow &row) {
		if (!packed_length) {
			packer.pack_array(row.results().n_tuples());
			packed_length = true;
		}

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
	bool packed_length;
};
