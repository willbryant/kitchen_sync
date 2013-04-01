#include <iostream>

#include "kitchen_sync.pb.h"

using namespace std;

template<class T>
void sync_to(T &client) {
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
}
