#include <iostream>

#include "../src/row_serialization.h"
#include "../src/hash_algorithm.h"
#include "../src/timestamp.h"

template <typename T>
double benchmark_one(const T &value, size_t columns, size_t rows, size_t reps, HashAlgorithm hash_algorithm) {
	size_t total_bytes_hashed(0);
	double start_time = timestamp();
	for (size_t rep = 0; rep < reps; rep++) {
		RowHasher hasher(hash_algorithm);
		for (size_t row = 0; row < rows; row++) {
			pack_array_length(hasher.row_packer, columns);
			for (size_t column = 0; column < columns; column++) {
				hasher.row_packer << value;
			}
		}
		Hash hash(hasher.finish());
		total_bytes_hashed += hasher.size;
	}
	double end_time = timestamp();
	// cout << "hashed " << total_bytes_hashed/1000 << " bytes per hash" << endl;
	return total_bytes_hashed/(end_time - start_time)/1024.0/1024.0;
}

template <typename T>
void benchmark(T value, size_t columns, size_t rows, size_t reps = 1000) {
	cout << "MD5:      " << benchmark_one(value, columns, rows, reps, HashAlgorithm::md5)    << "MB/s" << endl;
	cout << "XXHASH64: " << benchmark_one(value, columns, rows, reps, HashAlgorithm::xxh64)  << "MB/s" << endl;
	cout << endl;
}

int main(int argc, char *argv[]) {
	try {
		cout << "individual tiny rows (~10 B):" << endl;
		benchmark<int32_t>(2147483647, 2, 1);

		cout << "multiple tiny rows (~100 B):" << endl;
		benchmark<int32_t>(2147483647, 2, 10);

		cout << "many tiny rows (~1 KB):" << endl;
		benchmark<int32_t>(2147483647, 2, 100);

		cout << "very many tiny rows (~100 KB):" << endl;
		benchmark<int32_t>(2147483647, 2, 10000);

		cout << endl;

		cout << "individual medium rows (~240 B):" << endl;
		benchmark<string>("b104829e-3f9f-11e9-b6f7-f2189827a7e0", 6, 1);

		cout << "multiple medium rows (~24 KB):" << endl;
		benchmark<string>("b104829e-3f9f-11e9-b6f7-f2189827a7e0", 6, 100);

		cout << endl;

		cout << "individual long rows (~1 MB):" << endl;
		string s("0123456789abcdef");
		s.reserve(16384);
		for (int i = 0; i < 16; i++) s += s;
		benchmark<string>(s, 1, 1);

		cout << "multiple long rows (~1 GB):" << endl;
		benchmark<string>(s, 1, 1024, 1);
	} catch (const exception &e) {
		cerr << e.what() << endl;
	}
}
