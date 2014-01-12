#ifndef BASE_SQL_H
#define BASE_SQL_H

struct BaseSQL {
	static const size_t MAX_SENSIBLE_INSERT_COMMAND_SIZE = 8*1024*1024;
	static const size_t MAX_SENSIBLE_DELETE_COMMAND_SIZE =     16*1024;

	inline BaseSQL(const string &prefix, const string &suffix): prefix(prefix), suffix(suffix) {
		reset();
	}

	inline void reset() {
		curr = prefix;
		curr.reserve(2*MAX_SENSIBLE_INSERT_COMMAND_SIZE);
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
			client.execute(curr + suffix);
			reset();
		}
	}

	string prefix;
	string suffix;
	string curr;
};

#endif
