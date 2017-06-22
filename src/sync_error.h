#ifndef SYNC_ERROR_H
#define SYNC_ERROR_H

struct sync_error: public runtime_error {
	sync_error(): runtime_error("Sync error") { }
};

#endif
