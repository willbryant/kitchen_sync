#ifdef __APPLE__
	#include <CommonCrypto/CommonDigest.h>
	#define MD5_CTX CC_MD5_CTX
	#define MD5_Init CC_MD5_Init
	#define MD5_Update CC_MD5_Update
	#define MD5_Final CC_MD5_Final
	#define MD5_DIGEST_LENGTH CC_MD5_DIGEST_LENGTH
#else
	#include <openssl/md5.h>
#endif

struct RowCounter {
	RowCounter(): row_count(0) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		row_count++;
	}

	size_t row_count;
};

template <typename OutputStream>
struct RowPacker: RowCounter {
	RowPacker(Packer<OutputStream> &packer): packer(packer) {}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		RowCounter::operator()(row);
		row.pack_row_into(packer);
	}

	void reset_row_count() {
		row_count = 0;
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
	packer.pack_raw((const uint8_t *)hash.md_value, hash.md_len);
}

inline bool operator == (const Hash &hash, const string &str) {
	return (hash.md_len == str.length() && memcmp(str.c_str(), hash.md_value, hash.md_len) == 0);
}

struct RowHasher: RowCounter {
	RowHasher(): size(0), row_packer(*this) {
		MD5_Init(&mdctx);
	}

	const Hash &finish() {
		hash.md_len = MD5_DIGEST_LENGTH;
		MD5_Final(hash.md_value, &mdctx);
		return hash;
	}

	template <typename DatabaseRow>
	void operator()(const DatabaseRow &row) {
		RowCounter::operator()(row);
		row.pack_row_into(row_packer);
	}

	inline void write(const uint8_t *buf, size_t bytes) {
		MD5_Update(&mdctx, buf, bytes);
		size += bytes;
	}

	MD5_CTX mdctx;
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
			last_key[i].clear();
			row.pack_column_into(last_key[i], primary_key_columns[i]);
		}
	}

	const vector<size_t> &primary_key_columns;
	vector<PackedValue> last_key;
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
