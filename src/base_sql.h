#ifndef BASE_SQL_H
#define BASE_SQL_H

struct BaseSQL {
	inline BaseSQL(const string &prefix, const string &suffix): prefix(prefix), suffix(suffix) {
		reset();
	}

	inline void reset() {
		curr = prefix;
		curr.reserve(64*1024); // arbitrary
	}

	inline void operator += (const string &sql) {
		curr += sql;
	}

	inline void operator += (char sql) {
		curr += sql;
	}

	inline bool have_content() {
		return curr.size() > prefix.size();
	}

	template <typename DatabaseClient>
	inline void apply(DatabaseClient &client) {
		if (have_content()) {
			curr += suffix;
			client.execute(curr);
			reset();
		}
	}

	string prefix;
	string suffix;
	string curr;
};

#endif
