#include <openssl/md5.h>

template <typename OutputStream>
struct RowPacker {
	RowPacker(Packer<OutputStream> &packer): packer(packer) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		packer.pack_array_length(row.n_columns());

		for (size_t i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				packer.pack_nil();
			} else {
				packer << row.string_at(i);
			}
		}
	}

	Packer<OutputStream> &packer;
};

#define MAX_DIGEST_LENGTH MD5_DIGEST_LENGTH

struct Hash {
	inline std::string to_string() const { return string(md_value, md_value + md_len); }

	unsigned int md_len;
	unsigned char md_value[MAX_DIGEST_LENGTH];
};

template <typename OutputStream>
inline void operator << (Packer<OutputStream> &packer, const Hash &hash) {
	packer.pack_raw((const uint8_t *)hash.md_value, hash.md_len);
}

inline bool operator == (const Hash &hash, const string &str) {
	return (hash.md_len == str.length() && memcmp(str.c_str(), hash.md_value, hash.md_len) == 0);
}

struct RowHasher {
	RowHasher(): row_count(0), size(0), row_packer(*this) {
		MD5_Init(&mdctx);
	}

	const Hash &finish() {
		hash.md_len = MD5_DIGEST_LENGTH;
		MD5_Final(hash.md_value, &mdctx);
		return hash;
	}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		row_count++;
		
		// pack the row to get a byte stream, and hash it as it is written
		row_packer.pack_array_length(row.n_columns());

		for (size_t i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				row_packer.pack_nil();
			} else {
				row_packer << row.string_at(i);
			}
		}
	}

	inline void write(const uint8_t *buf, size_t bytes) {
		MD5_Update(&mdctx, buf, bytes);
		size += bytes;
	}

	MD5_CTX mdctx;
	size_t row_count;
	size_t size;
	Packer<RowHasher> row_packer;
	Hash hash;
};

struct RowLastKey {
	RowLastKey(const vector<size_t> &primary_key_columns): primary_key_columns(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		// keep its primary key, in case this turns out to be the last row, in which case we'll need to send it to the other end
		last_key.resize(primary_key_columns.size());
		for (size_t i = 0; i < primary_key_columns.size(); i++) {
			last_key[i] = row.string_at(primary_key_columns[i]);
		}
	}

	const vector<size_t> &primary_key_columns;
	vector<string> last_key;
};

struct RowHasherAndLastKey: RowHasher, RowLastKey {
	RowHasherAndLastKey(const vector<size_t> &primary_key_columns): RowLastKey(primary_key_columns) {
	}

	template <typename DatabaseRow>
	inline void operator()(const DatabaseRow &row) {
		RowHasher::operator()(row);
		RowLastKey::operator()(row);
	}
};
