struct HashResult {
	string hash;
	int row_count;
};

void operator >> (Unpacker &unpacker, HashResult &hash_result) {
	size_t array_length = unpacker.next_array_length(); // checks type
	if (array_length != 2) throw runtime_error("Expected array to have two elements while reading hash results");
	unpacker >> hash_result.hash;
	unpacker >> hash_result.row_count;
}
