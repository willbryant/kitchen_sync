#ifndef ROW_SERIALIZATION_H
#define ROW_SERIALIZATION_H

#include "md5/md5.h"

#define XXH_STATIC_LINKING_ONLY
#include "xxHash/xxhash.h"

#include "blake3/blake3.h"

#include "hash_algorithm.h"
#include "message_pack/pack.h"
#include "message_pack/packed_value.h"
#include "column_values.h"

template <typename Packer, typename DatabaseRow>
void pack_row_into(Packer &packer, DatabaseRow &row) {
	pack_array_length(packer, row.n_columns());

	for (size_t column_number = 0; column_number < row.n_columns(); column_number++) {
		row.pack_column_into(packer, column_number);
	}
}

struct ValueCollector {
	ValueCollector() {}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		values.clear();
		Packer<ColumnValues> packer(values);
		pack_row_into(packer, row);
	}

	ColumnValues values;
};

template <typename OutputStream>
struct RowPacker {
	RowPacker(Packer<OutputStream> &packer): packer(packer) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		pack_row_into(packer, row);
	}

	Packer<OutputStream> &packer;
};

#define MAX_DIGEST_LENGTH BLAKE3_OUT_LEN

struct Hash {
	inline Hash(): md_len(0) {}
	inline std::string to_string() const { return string(md_value, md_value + md_len); }

	unsigned int md_len;
	union {
		unsigned char md_value[MAX_DIGEST_LENGTH];
		uint64_t md_value_64;
	};
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
	RowHasher(HashAlgorithm hash_algorithm): hash_algorithm(hash_algorithm), size(0), finished(false), packer(*this) {
		switch (hash_algorithm) {
			case HashAlgorithm::md5:
				MD5_Init(&mdctx);
				break;

			case HashAlgorithm::xxh64:
				XXH64_reset(&xxh64_state, 0);
				break;

			case HashAlgorithm::blake3:
				blake3_hasher_init(&blake3_state);
				break;

			default:
				// never hit, but silence compiler warning
				throw runtime_error("invalid hash algorithm");
		}
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		pack_row_into(packer, row);
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

			case HashAlgorithm::blake3:
				blake3_hasher_update(&blake3_state, buf, bytes);
				break;

			default:
				// never hit, but silence compiler warning
				throw runtime_error("invalid hash algorithm");
		}
	}

	const Hash &finish() {
		if (finished) {
			return hash;
		}
		finished = true;
		switch (hash_algorithm) {
			case HashAlgorithm::md5:
				hash.md_len = 16;
				MD5_Final(hash.md_value, &mdctx);
				return hash;

			case HashAlgorithm::xxh64:
				hash.md_len = sizeof(uint64_t);
				hash.md_value_64 = htonll(XXH64_digest(&xxh64_state));
				return hash;

			case HashAlgorithm::blake3:
				hash.md_len = BLAKE3_OUT_LEN;
				blake3_hasher_finalize(&blake3_state, hash.md_value, BLAKE3_OUT_LEN);
				return hash;

			default:
				// never hit, but silence compiler warning
				throw runtime_error("invalid hash algorithm");
		}
	}

	HashAlgorithm hash_algorithm;
	union {
		MD5_CTX mdctx;
		XXH64_state_t xxh64_state;
		blake3_hasher blake3_state;
	};
	size_t size;
	Packer<RowHasher> packer;
	Hash hash;
	bool finished;
};

struct RowLastKey {
	RowLastKey(const vector<size_t> &primary_key_columns): primary_key_columns(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		// keep its primary key, in case this turns out to be the last row, in which case we'll need to send it to the other end
		last_key.clear();
		Packer<ColumnValues> packer(last_key);
		pack_array_length(packer, primary_key_columns.size());
		for (size_t column_number : primary_key_columns) {
			row.pack_column_into(packer, column_number);
		}
	}

	const vector<size_t> &primary_key_columns;
	ColumnValues last_key;
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
