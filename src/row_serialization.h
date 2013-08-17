#include <openssl/evp.h>

#define DIGEST_NAME "md5"

template<class DatabaseRow>
struct RowPacker {
	RowPacker(msgpack::packer<ostream> &packer): packer(packer) {}

	~RowPacker() {
		// we use an empty array to indicate the end of the rowset
		packer.pack_array(0);
	}

	void operator()(const DatabaseRow &row) {
		packer.pack_array(row.n_columns());

		for (size_t i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				packer.pack_nil();
			} else {
				packer << row.string_at(i);
			}
		}
	}

	msgpack::packer<ostream> &packer;
};

struct Hash {
	unsigned int md_len;
	unsigned char md_value[EVP_MAX_MD_SIZE];
};

void operator << (msgpack::packer<ostream> &packer, const Hash &hash) {
	packer.pack_raw(hash.md_len);
	packer.pack_raw_body((const char*)hash.md_value, hash.md_len);
}

bool operator == (const Hash &hash, const string &str) {
	return (hash.md_len == str.length() && string(hash.md_value, hash.md_value + hash.md_len) == str);
}

struct InitOpenSSL {
	InitOpenSSL() {
		OpenSSL_add_all_digests();
	}
};

static InitOpenSSL init_open_ssl;

template<class DatabaseRow>
struct RowHasher {
	RowHasher(): seen_rows(false) {
		const EVP_MD *md = EVP_get_digestbyname(DIGEST_NAME);
		if (!md) throw runtime_error("Unknown message digest " DIGEST_NAME);
		mdctx = EVP_MD_CTX_create();
		EVP_DigestInit_ex(mdctx, md, NULL);
	}

	~RowHasher() {
		EVP_MD_CTX_destroy(mdctx);
	}

	const Hash &finish() {
		EVP_DigestFinal_ex(mdctx, hash.md_value, &hash.md_len);
		return hash;
	}

	void operator()(const DatabaseRow &row) {
		seen_rows = true;
		
		// pack the row to get a byte stream
		msgpack::sbuffer packed_row;
		msgpack::packer<msgpack::sbuffer> row_packer(packed_row);
		row_packer.pack_array(row.n_columns());

		for (size_t i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				row_packer.pack_nil();
			} else {
				row_packer << row.string_at(i);
			}
		}

		// hash the byte stream
		EVP_DigestUpdate(mdctx, packed_row.data(), packed_row.size());
	}

	Hash hash;
	EVP_MD_CTX *mdctx;
	bool seen_rows;
};

template<class DatabaseRow>
struct RowHasherAndPacker {
	RowHasherAndPacker(msgpack::packer<ostream> &packer, const vector<size_t> &primary_key_columns): packer(packer), primary_key_columns(primary_key_columns) {
	}

	~RowHasherAndPacker() {
		// send [hash, primary key of last row] to the other end.  ideally we'd use a named map
		// here to make it easier to understand and extend, but this is the very core of the
		// high-rate communications, so we need to keep it as minimal as possible and rely on the
		// protocol version for future extensibility.
		packer.pack_array(2);
		packer << row_hasher.finish();
		packer << key_of_last_row;
	}

	void operator()(const DatabaseRow &row) {
		// hash the row
		row_hasher(row);

		// and keep its primary key, in case this turns out to be the last row, in which case we'll need to send it to the other end
		key_of_last_row.resize(primary_key_columns.size());
		for (size_t i = 0; i < primary_key_columns.size(); i++) {
			key_of_last_row[i] = row.string_at(primary_key_columns[i]);
		}
	}

	msgpack::packer<ostream> &packer;
	const vector<size_t> &primary_key_columns;
	vector<string> key_of_last_row;
	RowHasher<DatabaseRow> row_hasher;
};
