#include <openssl/evp.h>

#define DIGEST_NAME "md5"

template<class DatabaseRow>
struct RowPacker {
	RowPacker(msgpack::packer<ostream> &packer): packer(packer) {}

	~RowPacker() {
		// we use nil to indicate the end of the rowset
		packer.pack_nil();
	}

	void operator()(const DatabaseRow &row) {
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
};

struct Hash {
	unsigned int md_len;
	unsigned char md_value[EVP_MAX_MD_SIZE];
};

void operator << (msgpack::packer<ostream> &packer, const Hash &hash) {
	packer.pack_raw(hash.md_len);
	packer.pack_raw_body((const char*)hash.md_value, hash.md_len);
}

struct InitOpenSSL {
	InitOpenSSL() {
		OpenSSL_add_all_digests();
	}
};

static InitOpenSSL init_open_ssl;

template<class DatabaseRow>
struct RowHasher {
	RowHasher() {
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
		msgpack::sbuffer packed_row;
		msgpack::packer<msgpack::sbuffer> row_packer(packed_row);
		row_packer.pack_array(row.n_columns());

		for (int i = 0; i < row.n_columns(); i++) {
			if (row.null_at(i)) {
				row_packer.pack_nil();
			} else {
				row_packer << row.string_at(i);
			}
		}
		EVP_DigestUpdate(mdctx, packed_row.data(), packed_row.size());
	}

	Hash hash;
	EVP_MD_CTX *mdctx;
};

template<class DatabaseRow>
struct RowHasherAndPacker {
	RowHasherAndPacker(msgpack::packer<ostream> &packer): packer(packer) {}

	~RowHasherAndPacker() {
		packer << row_hasher.finish();
	}

	void operator()(const DatabaseRow &row) {
		row_hasher(row);
	}

	msgpack::packer<ostream> &packer;
	RowHasher<DatabaseRow> row_hasher;
};
