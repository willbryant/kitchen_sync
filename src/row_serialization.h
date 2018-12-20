#ifndef ROW_SERIALIZATION_H
#define ROW_SERIALIZATION_H

#ifdef __APPLE__
	#define COMMON_DIGEST_FOR_OPENSSL
	#include <CommonCrypto/CommonDigest.h>
#else
	#include <openssl/md5.h>
#endif

#define XXH_STATIC_LINKING_ONLY
#include "xxHash/xxhash.h"

#include "hash_algorithm.h"
#include "message_pack/pack.h"
#include "message_pack/packed_value.h"

struct ValueCollector {
	ValueCollector() {}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		values.resize(row.n_columns());
		for (size_t i = 0; i < row.n_columns(); i++) {
			values[i].clear();
			row.pack_column_into(values[i], i);
		}
	}

	vector<PackedValue> values;
};

template <typename OutputStream>
struct RowPacker {
	RowPacker(Packer<OutputStream> &packer): packer(packer) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		row.pack_row_into(packer);
	}

	Packer<OutputStream> &packer;
};

#define MAX_DIGEST_LENGTH MD5_DIGEST_LENGTH

struct Hash {
	inline Hash(): md_len(0) {}
	inline std::string to_string() const { return string(md_value, md_value + md_len); }

	unsigned int md_len;
	unsigned char md_value[MAX_DIGEST_LENGTH];
};

template <typename OutputStream>
inline void operator << (Packer<OutputStream> &packer, const Hash &hash) {
	pack_raw(packer, (const uint8_t *)hash.md_value, hash.md_len);
}

inline bool operator == (const Hash &hash, const string &str) {
	return (hash.md_len == str.length() && memcmp(str.c_str(), hash.md_value, hash.md_len) == 0);
}

inline bool operator != (const Hash &hash, const string &str) {
	return !(hash == str);
}

inline bool operator == (const string &str, const Hash &hash) {
	return hash == str;
}

inline bool operator != (const string &str, const Hash &hash) {
	return !(hash == str);
}

struct RowHasher {
	RowHasher(HashAlgorithm hash_algorithm): hash_algorithm(hash_algorithm), size(0), finished(false), row_packer(*this) {
		switch (hash_algorithm) {
			case HashAlgorithm::md5:
				MD5_Init(&mdctx);
				break;

			case HashAlgorithm::xxh64:
				XXH64_reset(&xxh64_state, 0);
				break;
		}
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		row.pack_row_into(row_packer);
	}

	inline void write(const uint8_t *buf, size_t bytes) {
		size += bytes;

		switch (hash_algorithm) {
			case HashAlgorithm::md5:
				MD5_Update(&mdctx, buf, bytes);
				break;

			case HashAlgorithm::xxh64:
				XXH64_update(&xxh64_state, buf, bytes);
				break;
		}
	}

	const Hash &finish() {
		if (finished) {
			return hash;
		}
		finished = true;
		switch (hash_algorithm) {
			case HashAlgorithm::md5:
				hash.md_len = MD5_DIGEST_LENGTH;
				MD5_Final(hash.md_value, &mdctx);
				return hash;

			case HashAlgorithm::xxh64:
				hash.md_len = sizeof(uint64_t);
				*((uint64_t*)&hash.md_value) = htonll(XXH64_digest(&xxh64_state));
				return hash;

			default:
				// never hit, but silence compiler warning
				return hash;
		}
	}

	HashAlgorithm hash_algorithm;
	union {
		MD5_CTX mdctx;
		XXH64_state_t xxh64_state;
	};
	size_t size;
	Packer<RowHasher> row_packer;
	Hash hash;
	bool finished;
};

struct RowLastKey {
	RowLastKey(const vector<size_t> &primary_key_columns): primary_key_columns(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		// keep its primary key, in case this turns out to be the last row, in which case we'll need to send it to the other end
		last_key.resize(primary_key_columns.size());
		for (size_t i = 0; i < primary_key_columns.size(); i++) {
			last_key[i].clear();
			row.pack_column_into(last_key[i], primary_key_columns[i]);
		}
	}

	const vector<size_t> &primary_key_columns;
	vector<PackedValue> last_key;
};

struct RowHasherAndLastKey: RowHasher, RowLastKey {
	RowHasherAndLastKey(HashAlgorithm hash_algorithm, const vector<size_t> &primary_key_columns): RowHasher(hash_algorithm), RowLastKey(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		RowHasher::operator()(row);
		RowLastKey::operator()(row);
	}
};

template <typename OutputStream>
struct RowPackerAndLastKey: RowPacker<OutputStream>, RowLastKey {
	RowPackerAndLastKey(Packer<OutputStream> &packer, const vector<size_t> &primary_key_columns): RowPacker<OutputStream>(packer), RowLastKey(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		RowPacker<OutputStream>::operator()(row);
		RowLastKey::operator()(row);
	}
};

#endif
