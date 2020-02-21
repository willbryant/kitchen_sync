#ifndef HASH_ALGORITHM_H
#define HASH_ALGORITHM_H

enum class HashAlgorithm {
	auto_select = -1,

	md5 = 0,
	xxh64 = 1,
	blake3 = 2,
};

#endif
