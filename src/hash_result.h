struct HashResult {
	string hash;
	ColumnValues last_key;
};

void operator >> (msgpack::object obj, HashResult &hash_result) {
	if (obj.type != msgpack::type::ARRAY) throw runtime_error("Expected an array while reading hash results");
	if (obj.via.array.size != 2) throw runtime_error("Expected array to have two elements while reading hash results");
	*obj.via.array.ptr >> hash_result.hash;
	*(obj.via.array.ptr + 1) >> hash_result.last_key;
}
