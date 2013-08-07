template <class T>
string to_string(T n) {
	std::ostringstream stream;
	stream << n;
	return stream.str();
}
